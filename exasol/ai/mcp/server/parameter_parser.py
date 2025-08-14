import re
from abc import (
    ABC,
    abstractmethod,
)
from textwrap import dedent
from typing import Any

from pyexasol import ExaConnection

from exasol.ai.mcp.server.parameter_pattern import (
    exa_type_pattern,
    identifier_pattern,
    parameter_list_pattern,
    quoted_identifier_pattern,
    regex_flags,
)
from exasol.ai.mcp.server.server_settings import MetaParameterSettings
from exasol.ai.mcp.server.utils import sql_text_value

VARIADIC_MARKER = "..."

FUNCTION_USAGE = (
    "In an SQL query, the names of database objects, such as schemas, tables, "
    "functions, and columns should be enclosed in double quotes. "
    "A reference to a function should include a reference to its schema."
)


class ParameterParser(ABC):
    def __init__(self, connection: ExaConnection, conf: MetaParameterSettings) -> None:
        self.connection = connection
        self.conf = conf
        self._parameter_extract_pattern: re.Pattern | None = None

    def _execute_query(self, query: str) -> list[dict[str, Any]]:
        return self.connection.meta.execute_snapshot(query=query).fetchall()

    def describe(
        self,
        schema_name: str,
        func_name: str,
    ) -> dict[str, Any]:
        """
        Requests and parses metadata for the specified function or script.
        """
        if not self.conf.enable:
            raise RuntimeError("Parameter listing is disabled.")
        schema_name = schema_name or self.connection.current_schema()
        if not schema_name:
            raise ValueError("Schema name is not provided.")
        if not func_name:
            raise ValueError("Function or script name is not provided.")

        query = self.get_func_query(schema_name, func_name)
        result = self._execute_query(query=query)
        if not result:
            raise ValueError(
                f"The function or script {schema_name}.{func_name} not found."
            )
        script_info = result[0]
        return self.extract_parameters(script_info)

    @property
    def parameter_extract_pattern(self) -> re.Pattern:
        r"""
        Lazily compiles a pattern for extracting parameter names and their SQL types
        from a parameter list.

        The pattern is looking for a sequence of parameters, each starting either
        from the beginning or from comma: (?:^|,). The parameter should be followed
        by either the end of the input or comma: (?=\Z|,).
        """
        if self._parameter_extract_pattern is not None:
            return self._parameter_extract_pattern

        pattern = (
            rf"(?:^|,)\s*(?P<{self.conf.name_field}>{quoted_identifier_pattern})"
            rf"\s+(?P<{self.conf.type_field}>{exa_type_pattern})\s*(?=\Z|,)"
        )
        self._parameter_extract_pattern = re.compile(pattern, flags=regex_flags)
        return self._parameter_extract_pattern

    def parse_parameter_list(self, params: str) -> str | list[dict[str, str]]:
        """
        Breaks the input string into parameter definitions. The input string should be
        extracted from a text of a function or a script and contain a list of parameters,
        where each parameter consists of a name and an SQL type. The list can be either
        an input or, in case of an EMIT UDF, the emit list.

        The parameters may be dynamic (variadic). This is designated with "...". In
        such case, the function simply returns "...".

        Otherwise, the function returns a list of dictionaries, where each dictionary
        describes a parameter and includes its name and type. The dictionary keys are
        defined by the provided configuration. The double quotes in the parameter name
        gets removed.
        """

        def remove_double_quotes(di: dict[str, str]) -> dict[str, str]:
            return {key: val.strip('"') for key, val in di.items()}

        params = params.lstrip()
        if params == VARIADIC_MARKER:
            return params

        return [
            remove_double_quotes(m.groupdict())
            for m in self.parameter_extract_pattern.finditer(params)
        ]

    def format_return_type(self, param: str) -> str | dict[str, str]:
        return {self.conf.type_field: param}

    @abstractmethod
    def get_func_query(self, schema_name: str, func_name: str) -> str:
        """
        Builds a query requesting metadata for a given function or script.
        """

    @abstractmethod
    def extract_parameters(self, info: dict[str, Any]) -> dict[str, Any]:
        """
        Parses the text of a function or a UDF script, extracting its input and output
        parameters and/or return type. Returns the result in a json form.

        Should raise a ValueError if the parsing fails.

        Note: This function does not validate the entire function or script text. It is
        only looking at its header.

        Args:
            info:
                The function or script information obtained from reading the relevant
                row in respectively SYS.EXA_ALL_FUNCTIONS or SYS.EXA_ALL_SCRIPTS table.
        """


class FuncParameterParser(ParameterParser):

    def __init__(self, connection: ExaConnection, conf: MetaParameterSettings) -> None:
        super().__init__(connection, conf)
        self._func_pattern: re.Pattern | None = None

    def get_func_query(self, schema_name: str, func_name: str) -> str:
        return dedent(
            f"""
            SELECT * FROM SYS.EXA_ALL_FUNCTIONS
            WHERE FUNCTION_SCHEMA = {sql_text_value(schema_name)} AND
                FUNCTION_NAME = {sql_text_value(func_name)}
        """
        )

    @property
    def func_pattern(self) -> re.Pattern:
        """
        Lazily compiles the function parsing pattern.
        """
        if self._func_pattern is not None:
            return self._func_pattern

        # The schema is optional
        func_schema_pattern = rf"(?:{quoted_identifier_pattern}\s*\.\s*)?"
        func_name_pattern = quoted_identifier_pattern
        pattern = (
            r"\A\s*FUNCTION\s+"
            rf"{func_schema_pattern}{func_name_pattern}\s*"
            rf"\((?P<{self.conf.input_field}>{parameter_list_pattern})\)\s*"
            rf"RETURN\s+(?P<{self.conf.return_field}>{exa_type_pattern})\s+"
        )
        self._func_pattern = re.compile(pattern, flags=regex_flags)
        return self._func_pattern

    def extract_parameters(self, info: dict[str, Any]) -> dict[str, Any]:
        m = self.func_pattern.match(info["FUNCTION_TEXT"])
        if m is None:
            raise ValueError(
                "Failed to parse the text of the function "
                f'{info["FUNCTION_SCHEMA"]}.{info["FUNCTION_NAME"]}.'
            )
        return {
            self.conf.input_field: self.parse_parameter_list(
                m.group(self.conf.input_field)
            ),
            self.conf.return_field: self.format_return_type(
                m.group(self.conf.return_field)
            ),
            self.conf.comment_field: info["FUNCTION_COMMENT"],
            self.conf.usage_field: FUNCTION_USAGE,
        }


class ScriptParameterParser(ParameterParser):

    def __init__(self, connection: ExaConnection, conf: MetaParameterSettings) -> None:
        super().__init__(connection, conf)
        self._emit_pattern: re.Pattern | None = None
        self._return_pattern: re.Pattern | None = None

    def get_func_query(self, schema_name: str, func_name: str) -> str:
        return dedent(
            f"""
            SELECT * FROM SYS.EXA_ALL_SCRIPTS
            WHERE SCRIPT_SCHEMA = {sql_text_value(schema_name)} AND
                SCRIPT_NAME = {sql_text_value(func_name)}
        """
        )

    def _udf_pattern(self, emits: bool) -> re.Pattern:
        """Compiles a pattern for parsing a UDF script."""

        # The parameter matching pattern should account for the possibility of the
        # variadic syntax: ...
        dynamic_list_pattern = rf"(?:\s*...\s*|{parameter_list_pattern})"

        output_pattern = (
            rf"EMITS\s*\((?P<{self.conf.emit_field}>{dynamic_list_pattern})\)\s*"
            if emits
            else rf"RETURNS\s+(?P<{self.conf.return_field}>{exa_type_pattern})\s+"
        )
        language_pattern = identifier_pattern
        # The schema is optional.
        udf_schema_pattern = rf"(?:{quoted_identifier_pattern}\s*\.\s*)?"
        udf_name_pattern = quoted_identifier_pattern

        pattern = (
            rf"\A\s*CREATE\s+{language_pattern}\s+(?:SCALAR|SET)\s+SCRIPT\s+"
            rf"{udf_schema_pattern}{udf_name_pattern}\s*"
            rf"\((?P<{self.conf.input_field}>{dynamic_list_pattern})\)\s*"
            rf"{output_pattern}AS\s+"
        )
        return re.compile(pattern, flags=regex_flags)

    @property
    def emit_udf_pattern(self) -> re.Pattern:
        """
        Lazily compiles the Emit-UDF parsing pattern.
        """
        if self._emit_pattern is None:
            self._emit_pattern = self._udf_pattern(emits=True)
        return self._emit_pattern

    @property
    def return_udf_pattern(self) -> re.Pattern:
        """
        Lazily compiles the Return-UDF parsing pattern.
        """
        if self._return_pattern is None:
            self._return_pattern = self._udf_pattern(emits=False)
        return self._return_pattern

    def extract_udf_parameters(self, info: dict[str, Any]) -> dict[str, Any]:
        pattern = (
            self.emit_udf_pattern
            if info["SCRIPT_RESULT_TYPE"] == "EMITS"
            else self.return_udf_pattern
        )
        m = pattern.match(info["SCRIPT_TEXT"])
        if m is None:
            raise ValueError(
                "Failed to parse the text of the UDF script "
                f'{info["SCRIPT_SCHEMA"]}.{info["SCRIPT_NAME"]}.'
            )
        param_func = {
            self.conf.input_field: self.parse_parameter_list,
            self.conf.emit_field: self.parse_parameter_list,
            self.conf.return_field: self.format_return_type,
        }
        return {
            param_field: param_func[param_field](params)
            for param_field, params in m.groupdict().items()
            if params or (param_field == self.conf.input_field)
        }

    def _get_variadic_note(self, variadic_input: bool, variadic_emit: bool) -> str:
        """
        A helper function for generating code example. Writes an explanation of the
        variadic syntax.
        """
        if (not variadic_input) and (not variadic_emit):
            return ""
        if not variadic_emit:
            variadic_param = "input"
            variadic_action = "provided"
        elif not variadic_input:
            variadic_param = "output"
            variadic_action = "emitted"
        else:
            variadic_param = "input and output"
            variadic_action = "provided and emitted"
        variadic_emit_note = (
            (
                " When calling a UDF with dynamic output parameters, the EMITS clause "
                "should be provided in the call, as demonstrated in the example below."
            )
            if variadic_emit
            else ""
        )
        return (
            f" This particular UDF has dynamic {variadic_param} parameters. "
            f"The {self.conf.comment_field} may give a hint on what parameters are "
            f"expected to be {variadic_action} in a specific use case."
            f"{variadic_emit_note} Note that in the following example the "
            f"{variadic_param} parameters are given only for illustration. They "
            f"shall not be used as a guide on how to call this UDF."
        )

    @staticmethod
    def _get_emit_note(emit: bool, emit_size: int, func_type: str) -> str:
        """
        A helper function for generating code example. Writes an explanation of the
        difference between a normal function and an EMIT UDF.
        """
        if not emit:
            return ""
        input_unit = "row" if func_type == "scalar" else "group"
        if emit_size == 0:
            output_desc = ""
        elif emit_size == 1:
            output_desc = ", one column each"
        else:
            output_desc = f", each with {emit_size} columns"
        return (
            f" Unlike normal {func_type} functions that return a single value for "
            f"every input {input_unit}, this UDF can emit multiple output rows per "
            f"input {input_unit}{output_desc}. An SQL SELECT statement calling a UDF "
            f"that emits output columns, such as this one, cannot include any "
            f"additional columns."
        )

    @staticmethod
    def _get_general_note(emit: bool) -> str:
        emit_note = ", including columns returned by the UDF," if emit else ""
        return (
            "Note that in an SQL query, the names of database objects, such as "
            f"schemas, tables, UDFs, and columns{emit_note} should be "
            "enclosed in double quotes. "
            "A reference to a UDF should include a reference to its schema."
        )

    def get_udf_call_example(
        self, result: dict[str, Any], input_type: str, func_name: str
    ) -> str:
        """
        Generates call example for a given UDF. For the examples of the
        generated texts see `test_get_udf_call_example` unit test.
        """
        emit = self.conf.emit_field in result
        variadic_emit = emit and result[self.conf.emit_field] == VARIADIC_MARKER
        variadic_input = result[self.conf.input_field] == VARIADIC_MARKER
        emit_size = (
            len(result[self.conf.emit_field]) if emit and (not variadic_emit) else 0
        )
        func_type = "scalar" if input_type.upper() == "SCALAR" else "aggregate"
        if variadic_input:
            input_params = '"INPUT_1", "INPUT_2"'
        else:
            input_params = ", ".join(
                f'"{param[self.conf.name_field]}"'
                for param in result[self.conf.input_field]
            )
        if variadic_emit:
            output_params = '"OUTPUT_1", "OUTPUT_2"'
        elif emit:
            output_params = ", ".join(
                f'"{param[self.conf.name_field]}"'
                for param in result[self.conf.emit_field]
            )
        else:
            output_params = ""

        introduction = (
            f"In most cases, an Exasol {input_type} User Defined Function (UDF) can "
            f"be called just like a normal {func_type} function."
            f"{self._get_emit_note(emit, emit_size, func_type)}"
            f"{self._get_variadic_note(variadic_input, variadic_emit)}"
        )

        example_header = "Here is a usage example for this particular UDF:"

        return_alias = "" if emit else ' AS "RETURN_VALUE"'
        emit_clause = (
            ' EMITS ("OUTPUT_1" VARCHAR(100), "OUTPUT_2" DOUBLE)'
            if variadic_emit
            else ""
        )
        from_clause = ' FROM "MY_SOURCE_TABLE"' if input_params else ""
        example = (
            f'```\nSELECT "{func_name}"({input_params}){return_alias}{emit_clause}'
            f"{from_clause}\n```"
        )

        example_footer = (
            (
                "This example assumes that the currently opened schema has the table "
                f'"MY_SOURCE_TABLE" with the following columns: {input_params}.'
            )
            if input_params
            else ""
        )
        if emit:
            emit_note = (
                f"The query produces a result set with the columns ({output_params}), "
                "similar to what is returned by a SELECT query."
            )
            example_footer = f"{example_footer}\n{emit_note}"

        return "\n".join(
            [
                introduction,
                example_header,
                example,
                example_footer,
                self._get_general_note(emit),
            ]
        )

    def extract_parameters(self, info: dict[str, Any]) -> dict[str, Any]:
        result = self.extract_udf_parameters(info)
        result[self.conf.comment_field] = info["SCRIPT_COMMENT"]
        result[self.conf.usage_field] = self.get_udf_call_example(
            result, input_type=info["SCRIPT_INPUT_TYPE"], func_name=info["SCRIPT_NAME"]
        )
        return result
