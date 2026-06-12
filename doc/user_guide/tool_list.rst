Exasol MCP Server Tools
=======================

.. contents::
   :local:
   :depth: 2

Tools Providing Information About Custom Database Objects
---------------------------------------------------------

list_exasol_schemas
~~~~~~~~~~~~~~~~~~~

:Description:
    Lists database schemas. Visibility of schemas can be restricted in the settings.
    Can be hidden globally via ``enable_list_tools`` in the settings (see :doc:`tool_setup`).

:Returns:
    - **Type**: ``list``
    - **Data**:
        - ``name``: name of the schema
        - ``comment``: schema comment, if available

find_exasol_schemas
~~~~~~~~~~~~~~~~~~~

:Description:
    Finds database schemas by looking for the specified keywords in their names and comments.
    Visibility of schemas can be restricted in the settings.
    Can be hidden globally via ``enable_find_tools`` in the settings (see :doc:`tool_setup`).

:Returns:
    - **Type**: ``list``
    - **Data**:
        - ``name``: name of the schema
        - ``comment``: schema comment, if available

list_exasol_tables_and_views
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Lists tables and views in the specified database schema.
    Visibility of tables and views can be restricted in the settings.
    Can be hidden globally via ``enable_list_tools`` in the settings (see :doc:`tool_setup`).

:Returns:
    - **Type**: ``list``
    - **Data**:
        - ``schema``: name of the schema where the table or view is located
        - ``name``: name of the table or view
        - ``comment``: table or view comment, if available

find_exasol_tables_and_views
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Finds tables and views by looking for the specified keywords in their names and comments.
    Optionally, limits the search to one specified schema.
    Visibility of tables and views can be restricted in the settings.
    Can be hidden globally via ``enable_find_tools`` in the settings (see :doc:`tool_setup`).

:Returns:
    - **Type**: ``list``
    - **Data**:
        - ``schema``: name of the schema where the table or view is located
        - ``name``: name of the table or view
        - ``comment``: table or view comment, if available

list_exasol_custom_functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Lists custom functions in the specified database schema.
    Visibility of functions can be restricted in the settings.
    Can be hidden globally via ``enable_list_tools`` in the settings (see :doc:`tool_setup`).

:Returns:
    - **Type**: ``list``
    - **Data**:
        - ``schema``: name of the schema where the function is located
        - ``name``: name of the function
        - ``comment``: function comment, if available

find_exasol_custom_functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Finds custom functions by looking for the specified keywords in their names and comments.
    Optionally, limits the search to one specified schema.
    Visibility of functions can be restricted in the settings.
    Can be hidden globally via ``enable_find_tools`` in the settings (see :doc:`tool_setup`).

:Returns:
    - **Type**: ``list``
    - **Data**:
        - ``schema``: name of the schema where the function is located
        - ``name``: name of the function
        - ``comment``: function comment, if available

list_exasol_user_defined_functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Lists User Defined Functions (UDF) in the specified database schema.
    Visibility of UDFs can be restricted in the settings.
    Can be hidden globally via ``enable_list_tools`` in the settings (see :doc:`tool_setup`).

:Returns:
    - **Type**: ``list``
    - **Data**:
        - ``schema``: name of the schema where the UDF is located
        - ``name``: name of the UDF
        - ``comment``: UDF comment, if available

find_exasol_user_defined_functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Finds User Defined Functions (UDF) by looking for the specified keywords in their names and comments.
    Optionally, limits the search to one specified schema.
    Visibility of UDFs can be restricted in the settings.
    Can be hidden globally via ``enable_find_tools`` in the settings (see :doc:`tool_setup`).

:Returns:
    - **Type**: ``list``
    - **Data**:
        - ``schema``: name of the schema where the UDF is located
        - ``name``: name of the UDF
        - ``comment``: UDF comment, if available

list_exasol_preprocessors
~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Lists available SQL preprocessor scripts and reports which one is currently
    active in the session.
    Can be disabled in the settings (see :doc:`tool_setup`).

:Returns:
    - **Type**: ``dict``
    - **Data**:
        - ``preprocessors``: list of preprocessor scripts, each with:
            - ``schema``: name of the schema where the preprocessor is located
            - ``name``: name of the preprocessor script
            - ``comment``: script comment, if available
        - ``current_preprocessor``: fully-qualified name of the active preprocessor
          (e.g. ``MY_SCHEMA.MY_PREPROCESSOR``), or ``null`` if none is set

set_exasol_preprocessor
~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Activates a SQL preprocessor script at the session level.
    This setting is not persistent: the MCP server manages the database connection
    independently and may reconnect without notice, silently resetting the active
    preprocessor. Before running queries that depend on a preprocessor, the agent
    should verify the active setting with ``list_exasol_preprocessors`` and
    re-apply if necessary.
    Can be disabled in the settings (see :doc:`tool_setup`).

:Arguments:
    - ``schema_name``: name of the schema containing the preprocessor script
    - ``script_name``: name of the preprocessor script

:Returns:
    - **Type**: ``string``
    - **Data**: fully-qualified name of the newly-activated preprocessor

describe_exasol_table_or_view
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Provides full available information about the specified table or view.

:Returns:
    - **Type**: ``dict``
    - **Data**:
        - ``schema``: name of the schema where the table or view is located
        - ``name``: name of the table or view
        - ``comment``: table or view comment, if available
        - ``columns``: list of table or view columns
            - ``name``: column name
            - ``comment``: column comment, if available
            - ``type``: SQL type, e.g. "VARCHAR(2000)"
        - ``constraints``: for tables only, list of table constraints if there are any:
            - ``name``: constraint name
            - ``constraint_type``: constraint type - either "PRIMARY KEY" or "FOREIGN_KEY"
            - ``columns``: comma separated list of columns the constraint is applied to
            - ``referenced_schema``: schema referenced in the FOREIGN KEY constraint
            - ``referenced_table``: table referenced in the FOREIGN KEY constraint
            - ``referenced_columns``: comma separated list of columns in the referenced table in the FOREIGN KEY constraint

summarize_exasol_table
~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Summarizes the content of a table or view.
    Returns the total row count and a configurable number of sample rows (default: 10).
    For each column returns:

    - most common distinct values in descending frequency order
    - number of distinct non-NULL values
    - presence of NULL values and their percentage
    - minimum and maximum values (numeric columns only)

    Must be explicitly enabled in the settings (see :doc:`tool_setup`).

:Parameters:
    - ``schema_name``: name of the schema
    - ``table_name``: name of the table or view
    - ``sample_size`` *(optional, default 10)*: number of sample rows to include, between 1 and 100
    - ``top_values`` *(optional, default 5)*: number of most common distinct values to return per column, between 1 and 100

:Returns:
    - **Type**: ``dict``
    - **Data**:
        - ``schema``: name of the schema where the table or view is located
        - ``name``: name of the table or view
        - ``comment``: table or view comment, if available
        - ``row_count``: total number of rows in the table or view
        - ``columns``: list of column statistics, each column contains:
            - ``name``: column name
            - ``comment``: column comment, if available
            - ``type``: SQL type, e.g. "DECIMAL(18,0)"
            - ``distinct_count``: number of distinct non-NULL values
            - ``min``: minimum value for numeric columns, ``null`` otherwise
            - ``max``: maximum value for numeric columns, ``null`` otherwise
            - ``top_values``: most common distinct values in descending frequency order; empty list if all values are NULL
            - ``has_nulls``: ``true`` if the column contains at least one NULL value
            - ``null_percentage``: percentage of NULL values rounded to whole percent
        - ``sample``: list of sample rows, each row is a dict with column names as keys

describe_exasol_custom_function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Provides full available information about the specified custom function.

:Returns:
    - **Type**: ``dict``
    - **Data**:
        - ``input``: list of input parameters
            - ``name``: parameter name
            - ``type``: SQL type, e.g. "VARCHAR(2000)"
        - ``returns``: returned SQL type

describe_exasol_user_defined_function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Provides full available information about the specified User Defined Function (UDF).

:Returns:
    - **Type**: ``dict``
    - **Data**:
        - ``input``: list of input parameters
            - ``name``: parameter name
            - ``type``: SQL type, e.g. "VARCHAR(2000)"
        - ``dynamic_input``: indication that the UDF accepts dynamic input
        - ``returns``: for return type UDF, returned SQL type
        - ``emits``: for emit type UDF, the list of output columns:
            - ``name``: parameter name
            - ``type``: SQL type
        - ``dynamic_output``: for emit type UDF, indication that the UDF emits dynamic output

Tools Executing a Query
-----------------------

execute_exasol_query
~~~~~~~~~~~~~~~~~~~~

:Description:
    Executes the specified query, which must be a SELECT statement.
    The query should not modify the data. SELECT INTO command is not allowed.
    An optional ``row_limit`` parameter can be used to preview a sample of results
    without fetching all rows. The query is then wrapped in
    ``SELECT * FROM (<query>) LIMIT <row_limit>``.

:Returns:
    - **Type**: ``list``
    - **Data**:
        - selected rows in a form of dictionaries, with column names as keys

profile_exasol_query
~~~~~~~~~~~~~~~~~~~~

:Description:
    Runs the specified SELECT query with profiling enabled and returns a breakdown
    of the execution plan. Use this to understand why a query is slow.

:Returns:
    - **Type**: ``list``
    - **Data**:
        - ``PART_NAME``: name of the execution step
        - ``PART_INFO``: additional information about the step
        - ``OBJECT_SCHEMA``: schema of the database object involved
        - ``OBJECT_NAME``: name of the database object involved
        - ``OBJECT_ROWS``: number of rows processed
        - ``DURATION``: duration of the step in milliseconds
        - ``CPU``: CPU time in milliseconds

execute_exasol_write_query
~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Executes the specified DML or DDL query.
    Normally, this tool can be used only if the MCP Client supports elicitation.
    The user must review and approve the query execution. The elicitation also allows altering the query.

:Returns:
    - **Type**: ``str or None``
    - **Data**:
        None if the query was executed it its original form, otherwise modified query

Tools for Reading, Writing and Deleting Files in BucketFS
---------------------------------------------------------

list_bucketfs_directories
~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Lists subdirectories of the specified BucketFS directory.

:Returns:
    - **Type**: ``list``
    - **Data**:
        subdirectory paths relative to the bucket root or the root path provided in the settings

list_bucketfs_files
~~~~~~~~~~~~~~~~~~~

:Description:
    Lists files in the specified BucketFS directory.
    Can be hidden globally via ``enable_list_tools`` in the settings (see :doc:`tool_setup`).

:Returns:
    - **Type**: ``list``
    - **Data**:
        file paths relative to the bucket root or the root path provided in the settings

find_bucketfs_files
~~~~~~~~~~~~~~~~~~~

:Description:
    Finds files in the specified BucketFS directory by looking for the provided keywords in their paths.
    Files are searched in the given directory and all its descendant subdirectories.
    Can be hidden globally via ``enable_find_tools`` in the settings (see :doc:`tool_setup`).

:Returns:
    - **Type**: ``list``
    - **Data**:
        file paths relative to the bucket root or the root path provided in the settings

read_bucketfs_text_file
~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Reads the content of a BucketFS text file.

:Returns:
    - **Type**: ``str``
    - **Data**:
        file content

write_text_to_bucketfs_file
~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Writes the provided text to a file in BucketFS.
    The new file will overwrite an existing file.
    Normally, this tool can be used only if the MCP Client supports elicitation.
    The user must confirm the operation via elicitation.

:Returns:
    - **Type**: ``None``

download_file
~~~~~~~~~~~~~

:Description:
    Downloads a file from a given url and saves it at the specified path in BucketFS.
    The new file will overwrite an existing file.
    Normally, this tool can be used only if the MCP Client supports elicitation.
    The user must confirm the operation via elicitation.

:Returns:
    - **Type**: ``None``

delete_bucketfs_file
~~~~~~~~~~~~~~~~~~~~

:Description:
    Deletes BucketFS file at the specified path.
    Normally, this tool can be used only if the MCP Client supports elicitation.
    The user must confirm the operation via elicitation.

:Returns:
    - **Type**: ``None``

delete_bucketfs_directory
~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Deletes a BucketFS directory at the specified path.
    This operation will recursively delete all files and all subdirectories in this directory.
    Normally, this tool can be used only if the MCP Client supports elicitation.
    The user must confirm the operation via elicitation.

:Returns:
    - **Type**: ``None``

Tools Providing Information About Exasol SQL Dialect
----------------------------------------------------

list_exasol_sql_types
~~~~~~~~~~~~~~~~~~~~~

:Description:
    Lists Exasol SQL types and their parameters.

:Returns:
    - **Type**: ``list``
    - **Data**:
        - ``type``: SQL type, e.g. "DECIMAL(10,5)"
        - ``create_params``: type parameters to be specified when creating a table column
        - ``precision``: default precision where applicable

list_exasol_system_tables
~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Lists Exasol system tables in the SYS schema.

:Returns:
    - **Type**: ``list``
    - **Data**:
        - table name

describe_exasol_system_table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Provides full available information about the specified system table.

:Returns:
    - **Type**: ``dict``
    - **Data**:
        - ``schema``: name of the schema - SYS
        - ``name``: name of the system table
        - ``comment``: table comment

list_exasol_statistics_tables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Lists Exasol statistics tables in the EXA_STATISTICS schema.

:Returns:
    - **Type**: ``list``
    - **Data**:
        - table name

describe_exasol_statistics_table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Provides full available information about the specified statistics table.

:Returns:
    - **Type**: ``dict``
    - **Data**:
        - ``schema``: name of the schema - EXA_STATISTICS
        - ``name``: name of the statistics table
        - ``comment``: table comment


list_exasol_keywords
~~~~~~~~~~~~~~~~~~~~

:Description:
    Lists Exasol keywords that start with a given letter.
    A tool argument specifies if the output should include keywords that are reserved words
    or not reserved words.

:Returns:
    - **Type**: ``list``
    - **Data**:
        - keyword

list_exasol_built_in_function_categories
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Lists built-in function categories.

:Returns:
    - **Type**: ``list``
    - **Data**:
        - function category

list_exasol_built_in_functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Lists built-in functions in the specified category.

:Returns:
    - **Type**: ``list``
    - **Data**:
        - function name

describe_exasol_built_in_function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:Description:
    Provides full available information about the specified built-in function.
    Returns a list that in most cases includes one data structure.
    However, in few cases, when a function with the same name works with different types of data,
    the tool returns multiple structures.

:Returns:
    - **Type**: ``list``
    - **Data**:
        - ``name``: function name
        - ``alias``: alternative function name, if available
        - ``types``: comma-separated list of categories the function belongs to
        - ``description``: description of data returned by the function
        - ``purpose``: more detailed description of the function, if available
        - ``syntax``: call syntax, if available
        - ``usage``: guidelines, restrictions and limitations, if applicable
        - ``example``: one or more call examples
