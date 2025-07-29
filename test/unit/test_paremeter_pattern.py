import re

import pytest

from exasol.ai.mcp.server.parameter_pattern import (
    exa_type_pattern,
    identifier_pattern,
    quoted_identifier_pattern,
    regex_flags,
)


def _get_char_type_examples(
    extra_space: int = 0, use_char: bool = False, char_set: str = ""
) -> list[str]:
    """
    Args:
        extra_space:
            An extra space to be inserted where possible.
        use_char:
            In all character type where size is specified use (n CHAR) notation.
        char_set:
            Explicit character set: [CHARACTER SET] ASCII|UTF8
    """
    s = " " * extra_space
    c = f"{s} CHAR" if use_char else ""
    cset = f"{s} {char_set}" if char_set else ""
    return [
        f"CHAR{cset}",
        f"CHAR{s}({s}30{c}{s}){cset}",
        f"CHAR{s} VARYING{s}({s}200{c}{s}){cset}",
        f"CHARACTER{cset}",
        f"CHARACTER{s} VARYING{s}({s}40{c}{s}){cset}",
        f"VARCHAR{s}({s}1000{c}{s}){cset}",
        f"VARCHAR2{s}({s}700{c}{s}){cset}",
        f"CHARACTER{s} LARGE{s} OBJECT{cset}",
        f"CHARACTER{s} LARGE{s} OBJECT{s}({s}300{c}{s}){cset}",
        f"CLOB{cset}",
        f"CLOB{s}({s}500{c}{s}){cset}",
        f"LONG{s} VARCHAR{cset}",
        f"NCHAR{cset}",
        f"NCHAR{s}({s}50{c}{s}){cset}",
        f"NVARCHAR{s}({s}100{c}{s}){cset}",
        f"NVARCHAR2{s}({s}100{c}{s}){cset}",
    ]


def _get_type_examples(extra_space: int = 0) -> list[str]:
    """
    Args:
        extra_space:
            An extra space to be inserted where possible.
    """
    s = " " * extra_space
    return [
        "BOOL",
        "BOOLEAN",
        "DEC",
        f"DEC{s}({s}10{s})",
        f"DEC{s}(18{s},{s}4{s})",
        "DECIMAL",
        f"DECIMAL{s}({s}5{s})",
        f"DECIMAL{s}({s}7{s},{s}3{s})",
        "INT",
        "INTEGER",
        "BIGINT",
        "SHORTINT",
        "SMALLINT",
        "TINYINT",
        "NUMBER",
        f"NUMBER{s}({s}8{s})",
        f"NUMBER{s}({s}9,{s}3{s})",
        "NUMERIC",
        f"NUMERIC{s}({s}5{s})",
        f"NUMERIC{s}({s}5{s},{s}2{s})",
        "DOUBLE",
        f"DOUBLE{s} PRECISION",
        "FLOAT",
        "REAL",
        "DATE",
        "TIMESTAMP",
        f"TIMESTAMP{s} WITH{s} LOCAL{s} TIME{s} ZONE",
        f"TIMESTAMP{s} WITHOUT{s} TIME{s} ZONE",
        f"TIMESTAMP{s}({s}8{s})",
        f"TIMESTAMP{s}({s}8{s}){s} WITH{s} LOCAL{s} TIME{s} ZONE",
        f"TIMESTAMP{s}({s}4{s}){s} WITHOUT{s} TIME{s} ZONE",
        f"INTERVAL{s} DAY{s} TO{s} SECOND",
        f"INTERVAL{s} DAY{s}({s}4{s}){s} TO{s} SECOND",
        f"INTERVAL{s} DAY{s}({s}4{s}){s} TO{s} SECOND{s}({s}8{s})",
        f"INTERVAL{s} YEAR{s} TO{s} MONTH",
        f"INTERVAL{s} YEAR{s}({s}4{s}){s} TO{s} MONTH",
        "GEOMETRY",
        f"GEOMETRY{s}({s}1234{s})",
        "HASHTYPE",
        f"HASHTYPE{s}({s}32{s} BIT{s})",
        f"HASHTYPE{s}({s}4{s} BYTE{s})",
    ] + _get_char_type_examples(extra_space=extra_space)


@pytest.fixture(scope="module")
def compiled_type_pattern() -> re.Pattern:
    pattern = f"^{exa_type_pattern}$"
    return re.compile(pattern, regex_flags)


@pytest.fixture(scope="module")
def compiled_identifier_pattern() -> re.Pattern:
    pattern = f"^{identifier_pattern}$"
    return re.compile(pattern, regex_flags)


@pytest.fixture(scope="module")
def compiled_quoted_identifier_pattern() -> re.Pattern:
    pattern = f"^{quoted_identifier_pattern}$"
    return re.compile(pattern, regex_flags)


@pytest.mark.parametrize("type_example", _get_type_examples())
def test_exa_type_pattern(compiled_type_pattern, type_example):
    assert compiled_type_pattern.match(type_example) is not None


@pytest.mark.parametrize("type_example", _get_type_examples(extra_space=2))
def test_exa_type_pattern_extra_space(compiled_type_pattern, type_example):
    assert compiled_type_pattern.match(type_example) is not None


@pytest.mark.parametrize(
    ["extra_space", "use_char", "char_set"],
    [
        (0, True, ""),
        (0, False, "UTF8"),
        (1, True, "ASCII"),
        (1, False, "CHARACTER SET UTF8"),
        (0, True, "CHARACTER SET ASCII"),
    ],
    ids=[
        "(n CHAR)",
        "UTF8",
        "(n CHAR) ASCII",
        "CHARACTER SET UTF8",
        "(n CHAR) CHARACTER SET ASCII",
    ],
)
def test_exa_char_type_pattern(compiled_type_pattern, extra_space, use_char, char_set):
    for type_example in _get_char_type_examples(
        extra_space=extra_space, use_char=use_char, char_set=char_set
    ):
        assert compiled_type_pattern.match(type_example) is not None


@pytest.mark.parametrize(
    "type_example",
    [
        "BOOLEAN(2)",
        "DECIM",
        "INT32",
        "INTEGER(32)",
        "DOUBLE NUMBER",
        "DATE AND TIME",
        "TIMESTAMP(6, 2)",
        "TIMESTAMP WITHOUT LOCAL TIME ZONE",
        "INTERVAL DAY TO MINUTE",
        "INTERVAL(5) DAY(4) TO SECOND",
        "INTERVAL YEAR TO MONTH(5)",
        "ALGEBRA(1234)",
        "HASHTYPE(1 KILOBYTE)",
        "VARCHAR(1000) CHARACTER SET UTF32",
    ],
)
def test_exa_type_pattern_invalid(compiled_type_pattern, type_example):
    assert compiled_type_pattern.match(type_example) is None


@pytest.mark.parametrize(
    "name_example",
    [
        "my_param",
        "_my_param",
        "my_param_1234",
        "_my_param_1234",
        "_1234",
        "mi_parámetro",
        "η_παράμετρος_μου",
        "мой_параметр",
    ],
)
def test_identifier_pattern(compiled_identifier_pattern, name_example):
    assert compiled_identifier_pattern.match(name_example) is not None


@pytest.mark.parametrize(
    "name_example",
    ["my param", "1234", "mi parámetro", "η παράμετρος μου", "мой параметр"],
)
def test_identifier_pattern_invalid(compiled_identifier_pattern, name_example):
    assert compiled_identifier_pattern.match(name_example) is None


@pytest.mark.parametrize(
    "name_example",
    [
        "my_param",
        "_1234",
        '"my param"',
        '"~^%(xyz)"',
        '"my ""param"""',
        '"mi parámetro"',
        '"η παράμετρος μου"',
        '"мой параметр"',
    ],
)
def test_quoted_identifier_pattern(compiled_quoted_identifier_pattern, name_example):
    assert compiled_quoted_identifier_pattern.match(name_example) is not None


@pytest.mark.parametrize(
    "name_example",
    [
        "my param",
        "1234",
        '"my "param""',
    ],
)
def test_quoted_identifier_pattern_invalid(
    compiled_quoted_identifier_pattern, name_example
):
    assert compiled_quoted_identifier_pattern.match(name_example) is None
