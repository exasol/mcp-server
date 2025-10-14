from test.utils.mcp_oidc_constants import DOCKER_JWK_URL

import nox

# imports all nox task provided by the toolbox
from exasol.toolbox.nox.tasks import *

# default actions to be run if nothing is explicitly specified with the -s option
nox.options.sessions = ["project:fix"]


@nox.session(name="test:integration_mcp", python=False)
def integration_mcp(session: nox.Session):
    extended_args = list(session.posargs) + [
        "---itde-additional-db-parameter",
        DOCKER_JWK_URL,
    ]

    # Call the tests session with combined arguments
    session.notify("test:integration", extended_args)
