import re
from collections.abc import (
    Generator,
    Iterable,
)
from functools import cache
from typing import Any


def sql_text_value(text: str) -> str:
    return f"""'{text.replace("'", "''")}'"""


@cache
def _get_word_split_pattern() -> re.Pattern:
    r"""
    The pattern splits camel case and snake case words into multiple words. However,
    the camel case splitting only works for ASCII characters. More precisely, it will
    not work if a word starts or ends with a diacritic or non-latin character.
    The issue can be fixed using the regex library and the pattern
    (?<=\p{Ll})(?=\p{Lu})|_+
    """
    pattern = r"(?<=[a-z])(?=[A-Z])|_+"
    return re.compile(pattern, flags=re.MULTILINE)


def _extract_words(texts: Iterable[Any]) -> Generator[None, None, str]:
    """
    Extracts words from all texts in the provided collection.
    All words in camel case or with underscores get split into multiple words.
    The words are returned in lowercase.
    Going forward, the words can be stemmed or lemmatized.
    """
    for text_ in texts:
        text = str(text_)
        for word in _get_word_split_pattern().sub(" ", text).split():
            if word:
                yield word.lower()


def _get_match_scores(
    input_words: list[Iterable[str]], keywords: list[str]
) -> list[int]:
    """
    For each bag of words in a list assigns a keyword matching score.
    For the moment, it's just a simple count of words matching one of the keywords.
    Going forward, this can be done with the TF-IDF or a similar algorithm.
    """
    return [sum(1 for word in words if word in keywords) for words in input_words]


def keyword_filter(
    input_data: list[dict[str, Any]], keywords: list[str]
) -> list[dict[str, Any]]:
    """
    For each dictionary with texts in the input list, computes the keyword matching
    score. Returns the list ordered by the score in descending order, removing the
    elements with zero score.
    A possible improvement will be clustering the scores and returning the texts
    in the highest score cluster.
    """
    keywords = list(_extract_words(keywords))
    input_words = [_extract_words(texts.values()) for texts in input_data]
    scores = _get_match_scores(input_words, keywords)
    return [
        texts
        for texts, score in sorted(
            zip(input_data, scores), key=lambda x: x[1], reverse=True
        )
        if score > 0
    ]
