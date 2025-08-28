from exasol.ai.mcp.server.keyword_search import extract_words


def test_extract_words(spacy_pipeline):
    input_sentences = [
        "I've got a BagOfApples",
        "They are from MY_ALLOTMENTS, which I got last year.",
    ]
    expected_words = ["get", "bag", "apple", "allotment", "get", "year"]
    words = extract_words(input_sentences, model_name=spacy_pipeline)
    assert words == expected_words
