import re


def _exa_type_pattern() -> str:
    # A number surrounded by spaces
    n = r"\s*\d+\s*"
    # An optional number in brackets
    _1n_ = rf"(?:\s*\({n}\))?"
    # An optional two numbers in brackets
    _2n_ = rf"(?:\s*\({n}(?:,{n})?\))?"
    # An optional character set
    _char_set = rf"(?:(?:\s+CHARACTER\s+SET)?\s+(?:ASCII|UTF8))?"
    # An optional number of characters followed by an optional character set.
    _char_ = rf"(?:\s*\({n}(?:CHAR\s*)?\))?{_char_set}"

    exa_type_list = [
        "BOOL(?:EAN)?",
        f"DEC(?:IMAL)?{_2n_}",
        "INT(?:EGER)?",
        "BIGINT",
        "SHORTINT",
        "SMALLINT",
        "TINYINT",
        f"NUMBER{_2n_}",
        f"NUMERIC{_2n_}",
        r"DOUBLE(?:\s+PRECISION)?",
        "FLOAT",
        "REAL",
        rf"CHAR(?:ACTER)?(?:\s+VARYING)?{_char_}",
        rf"VARCHAR2?{_char_}",
        rf"CHARACTER\s+LARGE\s+OBJECT{_char_}",
        f"CLOB{_char_}",
        rf"LONG\s+VARCHAR{_char_set}",
        f"NCHAR{_char_}",
        f"NVARCHAR2?{_char_}",
        "DATE",
        rf"TIMESTAMP{_1n_}(?:\s+(?:WITH\s+LOCAL|WITHOUT)\s+TIME\s+ZONE)?",
        rf"INTERVAL\s+DAY{_1n_}\s+TO\s+SECOND{_1n_}",
        rf"INTERVAL\s+YEAR{_1n_}\s+TO\s+MONTH",
        rf"GEOMETRY{_1n_}",
        rf"HASHTYPE(?:\s*\(\s*\d+\s+(?:BIT|BYTE)\s*\))?",
    ]
    type_choice = "|".join(exa_type_list)
    return f"(?:{type_choice})"


exa_type_pattern = _exa_type_pattern()
"""
The exact RE pattern of an Exasol SQL type.
"""

identifier_pattern = r"[^\W\d]\w*"
"""
The RE pattern of an identifier, e.g. a parameter name.
"""

quoted_identifier_pattern = f'(?:{identifier_pattern}|"(?:[^"]|"")+")'
"""
The RE pattern of an identifier optionally enclosed in double quotes.
The double quotes may enclose any text. The double quote characters in it
are escaped with double double quotes.
"""

parameter_pattern = rf"{identifier_pattern}\s+{exa_type_pattern}"
"""
The RE pattern of a function parameter, consisting of the parameter name and SQL type.
"""

quoted_parameter_pattern = rf"{quoted_identifier_pattern}\s+{exa_type_pattern}"
"""
The RE pattern of a script parameter, consisting of the parameter name, optionally
enclosed in double quotes, and SQL type.
"""

regex_flags = re.IGNORECASE | re.MULTILINE | re.UNICODE
"""
The RE flags required to match the parameter names and SQL types.
"""
