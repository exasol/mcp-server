import re
from abc import (
    ABC,
    abstractmethod,
)
from typing import Any

from exasol.ai.mcp.server.connection.db_connection import DbConnection
from exasol.ai.mcp.server.setup.server_settings import McpServerSettings
from exasol.ai.mcp.server.tools.meta_query import (
    ExasolMetaQuery,
    MetaType,
)
from exasol.ai.mcp.server.tools.parameter_pattern import (
    exa_type_pattern,
    identifier_pattern,
    parameter_list_pattern,
    quoted_identifier_pattern,
    regex_flags,
)
from exasol.ai.mcp.server.tools.schema.db_output_schema import (
    DBColumn,
    DBEmitFunction,
    DBFunction,
    DBReturnFunction,
)

VARIADIC_MARKER = "..."
PARAMETER_NAME = "PARAMETER_NAME"
PARAMETER_TYPE = "PARAMETER_TYPE"
FUNCTION_INPUT = "FUNCTION_INPUT"
FUNCTION_RETURNS = "FUNCTION_RETURNS"
FUNCTION_EMITS = "FUNCTION_EMIT"


class ParameterParser(ABC):
    def __init__(self, connection: DbConnection) -> None:
        self.connection = connection
        self._parameter_extract_pattern: re.Pattern | None = None

    def _execute_query(self, query: str) -> list[dict[str, Any]]:
        return self.connection.execute_query(query=query).fetchall()

    def describe(
        self,
        schema_name: str,
        func_name: str,
    ) -> DBFunction:
        """
        Requests and parses metadata for the specified function or script.
        """
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
            rf"(?:^|,)\s*(?P<{PARAMETER_NAME}>{quoted_identifier_pattern})"
            rf"\s+(?P<{PARAMETER_TYPE}>{exa_type_pattern})\s*(?=\Z|,)"
        )
        self._parameter_extract_pattern = re.compile(pattern, flags=regex_flags)
        return self._parameter_extract_pattern

    @staticmethod
    def is_variadic(params: str) -> bool:
        """
        Checks if the parameter string indicates that the parameters are dynamic
        (variadic). This is designated with "...".
        """
        params = params.lstrip()
        return params == VARIADIC_MARKER

    def parse_parameter_list(self, params: str) -> list[DBColumn]:
        """
        Breaks the input string into parameter definitions. The input string should be
        extracted from a text of a function or a script and contain a list of parameters,
        where each parameter consists of a name and an SQL type. The list can be either
        an input or, in case of an EMIT UDF, the emit list.
        The double quotes in the parameter names get removed.
        """

        def format_parameter(di: dict[str, str]) -> DBColumn:
            # Need to remove double quotes from the extracted values.
            return DBColumn(
                name=di[PARAMETER_NAME].strip('"'), type=di[PARAMETER_TYPE].strip('"')
            )

        return [
            format_parameter(m.groupdict())
            for m in self.parameter_extract_pattern.finditer(params)
        ]

    @abstractmethod
    def get_func_query(self, schema_name: str, func_name: str) -> str:
        """
        Builds a query requesting metadata for a given function or script.
        """

    @abstractmethod
    def extract_parameters(self, info: dict[str, Any]) -> DBFunction:
        """
        Parses the text of a function or a UDF script, extracting its input and output
        parameters and/or return type.

        Should raise a ValueError if the parsing fails.

        Note: This function does not validate the entire function or script text. It is
        only looking at its header.

        Args:
            info:
                The function or script information obtained from reading the relevant
                row in respectively SYS.EXA_ALL_FUNCTIONS or SYS.EXA_ALL_SCRIPTS table.
        """


class FuncParameterParser(ParameterParser):

    def __init__(self, connection: DbConnection, settings: McpServerSettings) -> None:
        super().__init__(connection)
        self._func_pattern: re.Pattern | None = None
        self._meta_query = ExasolMetaQuery(settings)

    def get_func_query(self, schema_name: str, func_name: str) -> str:
        return self._meta_query.get_object_metadata(
            MetaType.FUNCTION, schema_name, func_name
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
            rf"\((?P<{FUNCTION_INPUT}>{parameter_list_pattern})\)\s*"
            rf"RETURN\s+(?P<{FUNCTION_RETURNS}>{exa_type_pattern})\s+"
        )
        self._func_pattern = re.compile(pattern, flags=regex_flags)
        return self._func_pattern

    def extract_parameters(self, info: dict[str, Any]) -> DBFunction:
        m = self.func_pattern.match(info["FUNCTION_TEXT"])
        if m is None:
            raise ValueError(
                "Failed to parse the text of the function "
                f'{info["FUNCTION_SCHEMA"]}.{info["FUNCTION_NAME"]}.'
            )
        return DBReturnFunction(
            schema=info["FUNCTION_SCHEMA"],
            name=info["FUNCTION_NAME"],
            comment=info["FUNCTION_COMMENT"],
            input=self.parse_parameter_list(m.group(FUNCTION_INPUT)),
            dynamic_input=False,
            returns=m.group(FUNCTION_RETURNS),
        )


class ScriptParameterParser(ParameterParser):

    def __init__(self, connection: DbConnection, settings: McpServerSettings) -> None:
        super().__init__(connection)
        self._emit_pattern: re.Pattern | None = None
        self._return_pattern: re.Pattern | None = None
        self._meta_query = ExasolMetaQuery(settings)

    def get_func_query(self, schema_name: str, func_name: str) -> str:
        return self._meta_query.get_object_metadata(
            MetaType.SCRIPT, schema_name, func_name
        )

    def _udf_pattern(self, emits: bool) -> re.Pattern:
        """Compiles a pattern for parsing a UDF script."""

        # The parameter matching pattern should account for the possibility of the
        # variadic syntax: ...
        dynamic_list_pattern = rf"(?:\s*...\s*|{parameter_list_pattern})"

        output_pattern = (
            rf"EMITS\s*\((?P<{FUNCTION_EMITS}>{dynamic_list_pattern})\)\s*"
            if emits
            else rf"RETURNS\s+(?P<{FUNCTION_RETURNS}>{exa_type_pattern})\s+"
        )
        language_pattern = identifier_pattern
        # The schema is optional.
        udf_schema_pattern = rf"(?:{quoted_identifier_pattern}\s*\.\s*)?"
        udf_name_pattern = quoted_identifier_pattern

        pattern = (
            rf"\A\s*CREATE\s+{language_pattern}\s+(?:SCALAR|SET)\s+SCRIPT\s+"
            rf"{udf_schema_pattern}{udf_name_pattern}\s*"
            rf"\((?P<{FUNCTION_INPUT}>{dynamic_list_pattern})\)\s*"
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

    @staticmethod
    def _get_variadic_note(variadic_input: bool, variadic_emit: bool) -> str:
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
            f"The function comment may give a hint on what parameters are "
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
        self,
        input_type: str,
        func_name: str,
        input_params: list[DBColumn],
        variadic_input: bool = False,
        output_params: list[DBColumn] | None = None,
        variadic_emit: bool = False,
    ) -> str:
        """
        Generates call example for a given UDF. For the examples of the
        generated texts see `test_get_udf_call_example` unit test.
        """
        emit = variadic_emit or output_params
        emit_size = len(output_params) if output_params else 0
        func_type = "scalar" if input_type.upper() == "SCALAR" else "aggregate"
        if variadic_input:
            input_params = '"INPUT_1", "INPUT_2"'
        else:
            input_params = ", ".join(f'"{param.name}"' for param in input_params)
        if variadic_emit:
            output_params = '"OUTPUT_1", "OUTPUT_2"'
        elif emit:
            output_params = ", ".join(f'"{param.name}"' for param in output_params)
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

    def extract_return_udf_parameters(self, info: dict[str, Any]) -> DBReturnFunction:
        m = self.return_udf_pattern.match(info["SCRIPT_TEXT"])
        if m is None:
            raise ValueError(
                "Failed to parse the text of the RETURN UDF script "
                f'{info["SCRIPT_SCHEMA"]}.{info["SCRIPT_NAME"]}.'
            )
        input_param_text = m.group(FUNCTION_INPUT)
        dynamic_input = self.is_variadic(input_param_text)
        input_params = (
            self.parse_parameter_list(input_param_text) if not dynamic_input else []
        )
        return DBReturnFunction(
            schema=info["SCRIPT_SCHEMA"],
            name=info["SCRIPT_NAME"],
            comment=info["SCRIPT_COMMENT"],
            input=input_params,
            dynamic_input=dynamic_input,
            returns=m.group(FUNCTION_RETURNS),
            usage=self.get_udf_call_example(
                input_type=info["SCRIPT_INPUT_TYPE"],
                func_name=info["SCRIPT_NAME"],
                input_params=input_params,
                variadic_input=dynamic_input,
            ),
        )

    def extract_emit_udf_parameters(self, info: dict[str, Any]) -> DBEmitFunction:
        m = self.emit_udf_pattern.match(info["SCRIPT_TEXT"])
        if m is None:
            raise ValueError(
                "Failed to parse the text of the EMIT UDF script "
                f'{info["SCRIPT_SCHEMA"]}.{info["SCRIPT_NAME"]}.'
            )
        input_param_text = m.group(FUNCTION_INPUT)
        dynamic_input = self.is_variadic(input_param_text)
        input_params = (
            self.parse_parameter_list(input_param_text) if not dynamic_input else []
        )
        output_param_text = m.group(FUNCTION_EMITS)
        dynamic_output = self.is_variadic(output_param_text)
        output_params = (
            self.parse_parameter_list(output_param_text) if not dynamic_output else []
        )
        return DBEmitFunction(
            schema=info["SCRIPT_SCHEMA"],
            name=info["SCRIPT_NAME"],
            comment=info["SCRIPT_COMMENT"],
            input=input_params,
            dynamic_input=dynamic_input,
            emits=output_params,
            dynamic_output=dynamic_output,
            usage=self.get_udf_call_example(
                input_type=info["SCRIPT_INPUT_TYPE"],
                func_name=info["SCRIPT_NAME"],
                input_params=input_params,
                variadic_input=dynamic_input,
                output_params=output_params,
                variadic_emit=dynamic_output,
            ),
        )

    def extract_parameters(self, info: dict[str, Any]) -> DBFunction:

        if info["SCRIPT_RESULT_TYPE"] == "EMITS":
            return self.extract_emit_udf_parameters(info)
        else:
            return self.extract_return_udf_parameters(info)
