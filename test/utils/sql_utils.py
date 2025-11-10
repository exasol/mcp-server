from typing import Any


def sql_text_value(text: str) -> str:
    return f"""'{text.replace("'", "''")}'"""


def format_table_rows(rows: list[tuple[Any, ...]]) -> str:
    def format_value(val: Any) -> str:
        if isinstance(val, str):
            return sql_text_value(val)
        return str(val)

    def format_row(row: tuple[Any, ...]) -> str:
        column_list = ", ".join(map(format_value, row))
        return f"({column_list})"

    return ", ".join(map(format_row, rows))
