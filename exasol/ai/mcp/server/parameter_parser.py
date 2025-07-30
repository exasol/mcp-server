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
    quoted_identifier_pattern,
    quoted_parameter_pattern,
    regex_flags,
)
from exasol.ai.mcp.server.server_settings import MetaParameterSettings
from exasol.ai.mcp.server.utils import sql_text_value

parameter_list_pattern = rf"(?:,?\s*{quoted_parameter_pattern}\s*(?=\)|,))*"
r"""
A parameter list matching pattern, matching zero, one or more parameters. In the text,
the list should be enclosed in parentheses. A parameter may follow a comma, if it's not
the first one: The lookahead symbol after the parameter should be either a comma or a
closing parenthesis: (?=\)|,).
"""


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
        if params == "...":
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

    def extract_parameters(self, info: dict[str, Any]) -> dict[str, Any]:
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
