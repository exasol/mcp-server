import re


def collapse_spaces(text: str) -> str:
    text = re.sub(r"\s+", " ", text)
    # Remove leading and trailing spaces in brackets.
    text = re.sub(r"\(\s+", "(", text)
    text = re.sub(r"\s+\)", ")", text)
    return text.strip()
