import pytest
from spacy.cli.download import download as spacy_download

from exasol.ai.mcp.server.keyword_search import extract_words

SPACY_PIPELINE = "en_core_web_sm"
SPACY_PIPELINE_VERSION = "3.7.1"


@pytest.fixture(scope="session")
def spacy_pipeline() -> str:
    spacy_download(f"{SPACY_PIPELINE}-{SPACY_PIPELINE_VERSION}", direct=True)
    return SPACY_PIPELINE


def test_extract_words(spacy_pipeline):
    input_sentences = [
        "I've got a BagOfApples",
        "They are from MY_ALLOTMENTS, which I got last year.",
    ]
    expected_words = ["get", "bag", "apple", "allotment", "get", "year"]
    words = extract_words(input_sentences, model_name=spacy_pipeline)
    assert words == expected_words
