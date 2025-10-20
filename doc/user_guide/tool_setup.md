# Tool setup:

This guide provides detailed information on how to configure Exasol MCP Server tools
to address the requirements of a specific use case.

## Enable SQL queries

Most importantly, the server configuration specifies if reading the data using SQL
queries is enabled. Note that reading is disabled by default. To enable the data
reading, set the `enable_read_query` property to true:
```json
{
  "enable_read_query": true
}
```

## Set DB object listing filters

The server configuration settings can also be used to enable/disable or filter the
listing of a particular type of database objects. Similar settings are defined for
the following object types:
```
schemas,
tables,
views,
functions,
scripts
```
The settings include the following properties:
- `enable`: a boolean flag that enables or disables the listing.
- `like_pattern`: filters the output by applying the specified SQL LIKE condition to
the object name.
- `regexp_pattern`: filters the output by matching the object name with the specified
regular expression.

In the following example, the listing of schemas is limited to only one schema,
the listings of functions and scripts are disabled and the visibility of tables is
limited to tables with certain name pattern.

```json
{
  "schemas": {
    "like_pattern": "MY_SCHEMA"
  },
  "tables": {
    "like_pattern": "MY_TABLE%"
  },
  "functions": {
    "enable": false
  },
  "scripts": {
    "enable": false
  }
}
```

## Set the language

The language, if specified, can help the tools execute more precise search of requested
database object. This should be the language of communication with the LLM and also the
language used for naming and documenting the database objects. The language must be set
to its english name, e.g. "spanish", not "español".
Below is an example of configuration settings that sets the language to English.

```json
{
  "language": "english"
}
```

## Set the case-sensitive search option

By default, the database objects are searched in case-insensitive way, i.e. it is assumed
that the names "My_Table" and "MY_TABLE" refer to the same table. If this is undesirable,
the configuration setting `case_sensitive` should be set to true, as in the example below.

```json
{
  "case_sensitive": true
}
```

## Add the server configuration to the MCP Client configuration

The customised settings can be specified directly in the MCP Client configuration file
using another environment variable - `EXA_MCP_SETTINGS`:
```json
{
  "env": {
    "EXA_DSN": "my-dsn",
    "EXA_USER": "my-user-name",
    "EXA_PASSWORD": "my-password",
    "EXA_MCP_SETTINGS": "{\"schemas\": {\"like_pattern\": \"MY_SCHEMA\"}"
  }
}
```
Note that double quotes in the json text must be escaped, otherwise the environment
variable value will be interpreted, not as a text, but as a part of the outer json.

Alternatively, the settings can be written in a json file. In this case, the
`EXA_MCP_SETTINGS` should contain the path to this file, e.g.
```json
{
  "env": {
    "EXA_DSN": "my-dsn",
    "EXA_USER": "my-user-name",
    "EXA_PASSWORD": "my-password",
    "EXA_MCP_SETTINGS": "path_to_settings.json"
  }
}
```

## Default server settings

The following json shows the default settings.
```json
{
  "schemas": {
    "enable": true,
    "like_pattern": "",
    "regexp_pattern": ""
  },
  "tables": {
    "enable": true,
    "like_pattern": "",
    "regexp_pattern": ""
  },
  "views": {
    "enable": false,
    "like_pattern": "",
    "regexp_pattern": ""
  },
  "functions": {
    "enable": true,
    "like_pattern": "",
    "regexp_pattern": ""
  },
  "scripts": {
    "enable": true,
    "like_pattern": "",
    "regexp_pattern": ""
  },
  "enable_read_query": false,
  "language": ""
}
```
The default values do not need to be repeated in the customised settings.
