# By importing `mcp_server` into the package's __init__.py, developers allow users to
# import it directly from the package rather than navigating deep internal submodules.
# To preserve this from our automatic formatting tools, we ignore the F401
# (unused import).
from exasol.ai.mcp.server.main import mcp_server  # noqa: F401
