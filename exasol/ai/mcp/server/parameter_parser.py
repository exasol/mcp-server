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
                if len(result) > 1:
                    return report_error(self.tool_name, "Script metadata is ambiguous.")
                script_info = result[0]
                result = self.extract_parameters(script_info)
                if result is not None:
                    result_json = json.dumps(result)
                    return TextContent(type="text", text=result_json)
            return report_error(
                self.tool_name, "Failed to get the function or script metadata."
            )
        except Exception:  # pylint: disable=broad-exception-caught
            return report_error(self.tool_name, traceback.format_exc())

    def parse_parameter_list(self, params: str) -> str | list[dict[str, str]]:
        """
        Breaks the input string into parameter definitions. The input string should be
        extracted from a text of a function or a script and contain a list of parameters,
        where each parameter consists of a name and a type. The name may or may not be
        enclosed in double quotes. The list can be either an input or, in case of an EMIT
        UDF, the emit list.

        The parameters of a UDF script can be variadic, designated with "...". In such
        case, the function simply returns "...".

        Normally, the function returns a list of dictionaries, where each dictionary
        describes a parameter and includes its name and type. The dictionary keys are
        defined by the provided configuration. The double quotes in the parameter name
        gets removed.

        Note: This function does not validate the parameter types.
        """

        def remove_double_quotes(di: dict[str, str]) -> dict[str, str]:
            return {key: val.strip('"') for key, val in di.items()}

        params = params.lstrip()
        if params == "...":
            return params
        type_pattern = r"(?:\s*\w+(?:\s*\([\s\w,]*\))?)+"
        pattern = (
            rf'(?:^|,)\s*(?P<{self.conf.name_field}>\w+|"\w+")'
            rf"\s+(?P<{self.conf.type_field}>{type_pattern})"
        )
        return [
            remove_double_quotes(m.groupdict())
            for m in re.finditer(pattern, params, re.IGNORECASE)
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

        Note: This function does not validate the input text. In case the text is not
        a valid SQL function or script, the function may still return some seemingly
        meaningful results.

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
        # Unfortunately, a simple regular expression based parsing doesn't always
        # extract the exact return type. It is limited to a single term with optional
        # parameters in brackets, e.g. DECIMAL(10,5). More complex data types will
        # get truncated. For instance, "TIMESTAMP(10) WITH LOCAL TIME ZONE" will be
        # truncated to "TIMESTAMP(10)". This is because there is no easy way to tell
        # where the return type ends and the local variable declaration begins. The
        # term "IS" is optional.
        pattern = (
            r"\A\s*FUNCTION\s+"
            rf'(?:(?:{info["FUNCTION_SCHEMA"]}|"{info["FUNCTION_SCHEMA"]}")\s*\.\s*)?'
            rf'(?:{info["FUNCTION_NAME"]}|"{info["FUNCTION_NAME"]}")\s*'
            rf"\((?P<{self.conf.input_field}>[\w\(\)\s,]*)\)\s*"
            rf"RETURN\s+(?P<{self.conf.return_field}>\w*(?:\s*\([\w\s,]+\))?)\s+"
        )
        m = re.match(pattern, info["FUNCTION_TEXT"], re.IGNORECASE | re.MULTILINE)
        if m is None:
            return None
        return {
            self.conf.input_field: self.parse_parameter_list(
                m.group(self.conf.input_field)
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
        # Luckily, with UDFs we don't have the same problem as with functions.
        # All extracted types should be exact.
        main_pattern = (
            rf'\A\s*CREATE\s+{info["SCRIPT_LANGUAGE"]}\s+'
            rf'{info["SCRIPT_INPUT_TYPE"]}\s+SCRIPT\s+'
            rf'(?:(?:{info["SCRIPT_SCHEMA"]}|"{info["SCRIPT_SCHEMA"]}")\s*\.\s*)?'
            rf'(?:{info["SCRIPT_NAME"]}|"{info["SCRIPT_NAME"]}")\s*'
            rf'\((?P<{self.conf.input_field}>[\w"\s\(\),\.]*)\)\s*'
            rf'{info["SCRIPT_RESULT_TYPE"]}'
        )
        if info["SCRIPT_RESULT_TYPE"] == "EMITS":
            pattern = rf'{main_pattern}\s*\((?P<{self.conf.emit_field}>[\w"\s\(\),\.]*)\)\s*AS\s+'
        else:
            pattern = (
                rf'{main_pattern}\s+(?P<{self.conf.return_field}>[\w"\s\(\),]*)\s+AS\s+'
            )
        m = re.match(pattern, info["SCRIPT_TEXT"], re.IGNORECASE | re.MULTILINE)
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
