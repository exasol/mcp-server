import os

import nox

# imports all nox task provided by the toolbox
from exasol.toolbox.nox.tasks import *

# default actions to be run if nothing is explicitly specified with the -s option
nox.options.sessions = ["project:fix"]


@nox.session(name="test:print_cwd", python=False)
def print_cwd(session: nox.Session):
    print("Current directory:", os.getcwd())
    contents = os.listdir()
    print("Current directory content:")
    for item in contents:
        print(item)
