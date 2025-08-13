def sql_text_value(text: str) -> str:
    return f"""'{text.replace("'", "''")}'"""
