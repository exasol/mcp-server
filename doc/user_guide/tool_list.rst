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

:Returns:
    - **Type**: ``list``
    - **Data**:
        - ``schema``: name of the schema where the UDF is located
        - ``name``: name of the UDF
        - ``comment``: UDF comment, if available

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

:Returns:
    - **Type**: ``list``
    - **Data**:
        - selected rows in a form of dictionaries, with column names as keys

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

:Returns:
    - **Type**: ``list``
    - **Data**:
        file paths relative to the bucket root or the root path provided in the settings

find_bucketfs_files
~~~~~~~~~~~~~~~~~~~

:Description:
    Finds files in the specified BucketFS directory by looking for the provided keywords in their paths.
    Files are searched in the given directory and all its descendant subdirectories.

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
