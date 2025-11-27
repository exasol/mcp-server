"""
A CLI test utility that starts a mock identity server for running manual tests.
"""

from test.integration.test_mcp_oidc import start_oidc_server
from test.utils.mcp_oidc_constants import OIDC_USER_SUB

if __name__ == "__main__":
    with start_oidc_server() as oidc_server:
        print("Mock identity server started successfully at:", oidc_server)
        print("Registered subject:", OIDC_USER_SUB)
        print("\nFor RemoteAuthProvider set")
        print("FASTMCP_SERVER_AUTH=exa.fastmcp.server.auth.auth.RemoteAuthProvider")
        print(f"EXA_AUTH_AUTHORIZATION_SERVERS={oidc_server}")
        print(f"EXA_AUTH_JWKS_URI={oidc_server}/jwks")
        print("EXA_AUTH_BASE_URL=<mcp-server-url>")
        print("\nFor OAuthProxy set")
        print("FASTMCP_SERVER_AUTH=exa.fastmcp.server.auth.oauth_proxy.OAuthProxy")
        print(
            f"EXA_AUTH_UPSTREAM_AUTHORIZATION_ENDPOINT={oidc_server}/oauth2/authorize"
        )
        print(f"EXA_AUTH_UPSTREAM_TOKEN_ENDPOINT={oidc_server}/oauth2/token")
        print("EXA_AUTH_UPSTREAM_CLIENT_ID=whatever")
        print("EXA_AUTH_UPSTREAM_CLIENT_SECRET=whatever")
        print(f"EXA_AUTH_JWKS_URI={oidc_server}/jwks")
        print("EXA_AUTH_BASE_URL=<mcp-server-url>")
        print("\nPress any key to terminate...")
        input()
