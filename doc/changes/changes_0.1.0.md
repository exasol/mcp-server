# 0.1.0 - 2025-08-05

## Features

* #2: Added metadata listing tools.
* #6: Added `describe_function` and `describe_script` tools.
* #7: Added `execute_query` tool.
* #11: Foreign key now contains the reference to the primary column.

## Refactoring

* #9: Changed the tool return values to the structured output - the dataclasses and dictionaries.
* #10: Raising or not catching the exceptions, and letting the FastMCP to handle them.
* #15: Split `MetaSettings` into listable and non-listable classes.

## Documentation

* #16: Added the README.md
* #17: Allowed providing the settings in a json file.
