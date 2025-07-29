import json
import re
import traceback
from abc import (
    ABC,
    abstractmethod,
)
from textwrap import dedent
from typing import Any

from mcp.types import TextContent
from pyexasol import ExaConnection

from exasol.ai.mcp.server.parameter_pattern import (
    exa_type_pattern,
    identifier_pattern,
    quoted_identifier_pattern,
    regex_flags,
)
from exasol.ai.mcp.server.server_settings import MetaParameterSettings
from exasol.ai.mcp.server.utils import (
    report_error,
    sql_text_value,
)


class ParameterParser(ABC):
    def __init__(
        self, connection: ExaConnection, conf: MetaParameterSettings, tool_name: str
    ) -> None:
        self.connection = connection
        self.conf = conf
        self.tool_name = tool_name

    def _execute_query(self, query: TextContent) -> list[dict[str, Any]]:
        return self.connection.meta.execute_snapshot(query=query).fetchall()

    def describe(
        self,
        schema_name: str,
        func_name: str,
    ) -> TextContent:
        """
        Requests and parses metadata for the specified function or script.
        """
        if not self.conf.enable:
            return report_error(self.tool_name, "Parameter listing is disabled.")
        schema_name = schema_name or self.connection.current_schema()
        if not schema_name:
            return report_error(self.tool_name, "Schema name is not provided.")
        if not func_name:
            return report_error(
                self.tool_name, "Function or script name is not provided."
            )

        query = self.get_func_query(schema_name, func_name)
        try:
            result = self._execute_query(query=query)
            if result:
                script_info = result[0]
                result = self.extract_parameters(script_info)
                if result is not None:
                    result_json = json.dumps(result)
                    return TextContent(type="text", text=result_json)
            return report_error(self.tool_name, "The function or script not found.")
        except Exception:  # pylint: disable=broad-exception-caught
            return report_error(self.tool_name, traceback.format_exc())

    def parse_parameter_list(
        self, params: str, allow_dynamic: bool = True, allow_double_quotes: bool = True
    ) -> str | list[dict[str, str]]:
        """
        Breaks the input string into parameter definitions. The input string should be
        extracted from a text of a function or a script and contain a list of parameters,
        where each parameter consists of a name and a type. The list can be either an
        input or, in case of an EMIT UDF, the emit list.

        Normally, the function returns a list of dictionaries, where each dictionary
        describes a parameter and includes its name and type. The dictionary keys are
        defined by the provided configuration. The double quotes in the parameter name
        gets removed.

        Args:
            params:
                Comma-separated list of parameters.
            allow_dynamic:
                If True, the parameters may be dynamic (variadic). This is designated
                with "...". In such case, the function simply returns "...".
            allow_double_quotes:
                If True, the parameter name may be enclosed in double quotes.
        """

        def remove_double_quotes(di: dict[str, str]) -> dict[str, str]:
            if allow_double_quotes:
                return {key: val.strip('"') for key, val in di.items()}
            return di

        params = params.lstrip()
        if allow_dynamic and params == "...":
            return params

        # Parameter matching pattern is looking for a sequence that starts either
        # from the beginning or from comma: (?:^|,), and consists of the parameter
        # name and SQL type. That should follow either by the end of the string or
        # comma: \s*(?=\Z|,).
        param_name_pattern = (
            quoted_identifier_pattern if allow_double_quotes else identifier_pattern
        )
        pattern = (
            rf"(?:^|,)\s*(?P<{self.conf.name_field}>{param_name_pattern})"
            rf"\s+(?P<{self.conf.type_field}>{exa_type_pattern})\s*(?=\Z|,)"
        )
        return [
            remove_double_quotes(m.groupdict())
            for m in re.finditer(pattern, params, flags=regex_flags)
        ]

    def format_return_type(self, param: str) -> str | dict[str, str]:
        return {self.conf.type_field: param}

    @abstractmethod
    def get_func_query(self, schema_name: str, func_name: str) -> str:
        """
        Builds a query requesting metadata for a given function or script.
        """

    @abstractmethod
    def extract_parameters(self, info: dict[str, Any]) -> dict[str, Any] | None:
        """
        Parses the text of a function or a UDF script, extracting its input and output
        parameters and/or return type. Returns the result in a json form. If the text
        cannot be parsed, returns None.

        Note: This function does not validate the entire function or script text. It is
        only looking at its header.

        Args:
            info:
                The function or script information obtained from reading the relevant
                row in respectively SYS.EXA_ALL_FUNCTIONS or SYS.EXA_ALL_SCRIPTS table.
        """


class FuncParameterParser(ParameterParser):

    def get_func_query(self, schema_name: str, func_name: str) -> str:
        return dedent(
            f"""
            SELECT * FROM SYS.EXA_ALL_FUNCTIONS
            WHERE FUNCTION_SCHEMA = {sql_text_value(schema_name)} AND
                FUNCTION_NAME = {sql_text_value(func_name)}
        """
        )

    def extract_parameters(self, info: dict[str, Any]) -> dict[str, Any] | None:
        # The pattern matches zero, one or more instances of parameter pairs
        # (name, type). A pair may start from a comma (for a parameter other than
        # the first one): ,?. The lookahead symbol after the pair should be either
        # a comma or a closing bracket: (?=\)|,).
        parameter_list_pattern = (
            rf"(?:,?\s*{identifier_pattern}\s+" rf"{exa_type_pattern}\s*(?=\)|,))*"
        )
        pattern = (
            r"\A\s*FUNCTION\s+"
            rf'(?:(?:{info["FUNCTION_SCHEMA"]}|"{info["FUNCTION_SCHEMA"]}")\s*\.\s*)?'
            rf'(?:{info["FUNCTION_NAME"]}|"{info["FUNCTION_NAME"]}")\s*'
            rf"\((?P<{self.conf.input_field}>{parameter_list_pattern})\)\s*"
            rf"RETURN\s+(?P<{self.conf.return_field}>{exa_type_pattern})\s+"
        )
        m = re.match(pattern, info["FUNCTION_TEXT"], flags=regex_flags)
        if m is None:
            return None
        return {
            self.conf.input_field: self.parse_parameter_list(
                m.group(self.conf.input_field),
                allow_dynamic=False,
                allow_double_quotes=False,
            ),
            self.conf.return_field: self.format_return_type(
                m.group(self.conf.return_field)
            ),
        }


class ScriptParameterParser(ParameterParser):

    def get_func_query(self, schema_name: str, func_name: str) -> str:
        return dedent(
            f"""
            SELECT * FROM SYS.EXA_ALL_SCRIPTS
            WHERE SCRIPT_SCHEMA = {sql_text_value(schema_name)} AND
                SCRIPT_NAME = {sql_text_value(func_name)}
        """
        )

    def extract_parameters(self, info: dict[str, Any]) -> dict[str, Any] | None:
        # The pattern matches the parameter list, as in FuncParameterParser, but with
        # quoted parameter names.
        parameter_list_pattern = (
            rf"(?:,?\s*{quoted_identifier_pattern}\s+"
            rf"{exa_type_pattern}\s*(?=\)|,))*"
        )
        # The pattern should also include the possibility of the variadic syntax: ...
        dynamic_list_pattern = rf"(?:\s*...\s*|{parameter_list_pattern})"
        if info["SCRIPT_RESULT_TYPE"] == "EMITS":
            output_pattern = (
                rf"\s*\((?P<{self.conf.emit_field}>{dynamic_list_pattern})\)\s*"
            )
        else:
            output_pattern = rf"\s+(?P<{self.conf.return_field}>{exa_type_pattern})\s+"
        pattern = (
            rf'\A\s*CREATE\s+{info["SCRIPT_LANGUAGE"]}\s+'
            rf'{info["SCRIPT_INPUT_TYPE"]}\s+SCRIPT\s+'
            rf'(?:(?:{info["SCRIPT_SCHEMA"]}|"{info["SCRIPT_SCHEMA"]}")\s*\.\s*)?'
            rf'(?:{info["SCRIPT_NAME"]}|"{info["SCRIPT_NAME"]}")\s*'
            rf"\((?P<{self.conf.input_field}>{dynamic_list_pattern})\)\s*"
            rf'{info["SCRIPT_RESULT_TYPE"]}{output_pattern}AS\s+'
        )
        m = re.match(pattern, info["SCRIPT_TEXT"], flags=regex_flags)
        if m is None:
            return None
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
