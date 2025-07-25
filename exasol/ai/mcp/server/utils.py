import json
import logging

from mcp.types import TextContent

logger = logging.getLogger("exasol_mcp_server")


def report_error(tool_name: str, error_message: str) -> TextContent:
    logger.error("Error in %s: %s", tool_name, error_message)
    error_json = json.dumps({"error": error_message})
    return TextContent(type="text", text=error_json)


def sql_text_value(text: str) -> str:
    return f"""'{text.replace("'", "''")}'"""
