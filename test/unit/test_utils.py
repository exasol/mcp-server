import pytest

from exasol.ai.mcp.server.utils import keyword_filter


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
                {"name": "size sq meters", "comment": "size of my allotment"},
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
