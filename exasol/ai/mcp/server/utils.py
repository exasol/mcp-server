def sql_text_value(text: str) -> str:
    return f"""'{text.replace("'", "''")}'"""


def join_lines(*lines) -> str:
    return "\n".join(line for line in lines if line)
