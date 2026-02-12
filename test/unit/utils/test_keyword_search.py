import pytest

from exasol.ai.mcp.server.utils.keyword_search import (
    extract_words,
    get_match_scores,
    keyword_filter,
    top_score_indices,
)


def test_extract_words():
    input_sentences = [
        "I've got a BagOfApples",
        "They are from MY_ALLOTMENT, which I got last year.",
    ]
    expected_words = [
        "i",
        "ve",
        "got",
        "a",
        "bag",
        "of",
        "apples",
        "they",
        "are",
        "from",
        "my",
        "allotment",
        "which",
        "i",
        "got",
        "last",
        "year",
    ]
    words = extract_words(input_sentences)
    assert words == expected_words


def test_extract_words_english():
    input_sentences = [
        "I've got a BagOfApples",
        "They are from MY_ALLOTMENT, which I got last year.",
    ]
    expected_words = [
        "ve",
        "got",
        "bag",
        "apples",
        "allotment",
        "got",
        "last",
        "year",
    ]
    words = extract_words(input_sentences, "english")
    assert words == expected_words


def test_get_match_scores():
    corpus = [
        ["apples", "bananas", "pears", "cherries", "apples"],
        ["pears", "bananas", "pears"],
        ["bananas", "cherries", "pears"],
    ]
    keywords = ["apples", "pears"]
    scores = get_match_scores(corpus, keywords)
    # We don't know what the scores will be, but the common sense suggest
    # that the 1st text should have the highest score, since it has both
    # keywords, the 2nd should have second highest, since 2 out of 3
    # words there are keywords, and the 3rd should have the lowest score.
    sorted_scores = sorted(scores, reverse=True)
    assert sorted_scores == scores


@pytest.mark.parametrize(
    ["scores", "expected_result"],
    [
        ([0.3, 0.4, 0.1, 0.8, 0.2, 0.4], [3]),
        ([0.3, 0.3, 0.1, 0.7, 0.5, 0.6, 0.3, 0.2, 0.2], [3, 5, 4]),
        ([0.2, 0.7, 0.8, 0.9, 0.75, 0.1, 0.85], [3, 6, 2, 4, 1]),
        ([0.8] + [0.4] * 50 + [0.2] * 50, [0]),
        ([0.8, 0.7] + [0.4] * 50 + [0.2] * 50, [0, 1]),
        ([0.5], [0]),
    ],
    ids=["top-1", "top-3", "most", "one-outlier", "two-outliers", "single-point"],
)
def test_top_score_indices(scores, expected_result):
    result = top_score_indices(scores)
    assert result == expected_result


def test_top_score_indices_flat():
    result = top_score_indices([0.5, 0.5, 0.5, 0.5])
    assert sorted(result) == list(range(len(result)))


@pytest.mark.parametrize(
    ["input_data", "keywords", "expected_output_data"],
    [
        (
            [
                {"name": "size sq meters", "comment": "size of my allotment"},
                {"name": "BagOfApples", "comment": "apples from my allotment"},
                {"name": "BagOfPears", "comment": "one kilo of pears"},
            ],
            ["Apples", "Allotment"],
            [
                {"name": "BagOfApples", "comment": "apples from my allotment"},
            ],
        ),
        (
            [
                {"name": "supermarket", "comment": "supermarket location"},
                {"name": "Market_Pears", "comment": "pears on sale"},
                {"name": "stall_market_location", "comment": "stall market location"},
            ],
            ["STALL_MARKET", "PEARS"],
            [
                {"name": "stall_market_location", "comment": "stall market location"},
                {"name": "Market_Pears", "comment": "pears on sale"},
            ],
        ),
    ],
    ids=["camel cases", "underscores"],
)
def test_keyword_filter(input_data, keywords, expected_output_data):
    output_data = keyword_filter(input_data, keywords)
    assert output_data == expected_output_data
