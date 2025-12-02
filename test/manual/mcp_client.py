"""
A simple CLI test utility that runs an MCP Client and executes specified tool.
It can run with or without the hack shortcutting the authorization UI.
"""

import asyncio
from test.integration.test_mcp_oidc import OAuthHeadless

import click
from fastmcp import Client
from fastmcp.client.auth.oauth import OAuth
from fastmcp.client.transports import StreamableHttpTransport


async def _run_tool_async(
    http_server_url: str,
    tool_name: str,
    auto_auth: bool,
    **kwargs,
) -> str:
    if auto_auth:
        oauth = OAuthHeadless(mcp_url=http_server_url)
    else:
        oauth = OAuth(mcp_url=http_server_url)
    async with Client(
        transport=StreamableHttpTransport(http_server_url), auth=oauth
    ) as client:
        assert await client.ping()
        result = await client.call_tool(tool_name, kwargs)
        return result.content[0].text


def _run_tool(http_server_url: str, tool_name: str, auto_auth: bool, **kwargs) -> str:
    result_text = asyncio.run(
        _run_tool_async(http_server_url, tool_name, auto_auth, **kwargs)
    )
    return result_text


@click.command()
@click.option(
    "--host",
    default="localhost",
    help="The host where the MCP container is running (default: localhost).",
)
@click.option("--port", type=click.IntRange(min=1), help="The MCP server port.")
@click.option("--https", is_flag=True, help="Use secure connection, i.e. HTTPS.")
@click.option("--tool", required=True, help="Name of the MCP tool to run.")
@click.option(
    "--auto-auth", is_flag=True, help="Automatically authorize the MCP tool to run."
)
@click.option(
    "--kwarg",
    multiple=True,
    help="Named argument to call the tool will. Can be passed multiple times.",
)
def call_mcp(host, port, https, tool, auto_auth, kwarg) -> None:
    protocol = "https" if https else "http"
    server = f"{host}:{port}" if port else host
    server_url = f"{protocol}://{server}/mcp"
    kwargs = {kw[0]: kw[1] for kw in [item.split("=") for item in kwarg]}
    result = _run_tool(server_url, tool, auto_auth, **kwargs)
    print(result)


if __name__ == "__main__":
    call_mcp()
