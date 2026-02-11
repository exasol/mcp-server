"""
Builds the builtin functions resource = exasol_builtin_functions.json

Requires the following packages:

- Sentence Transformers
pip install sentence-transformers

- Beautiful Soup
pip install beautifulsoup4
This package should be installed anyway, as a dependency of sphinx-toolbox
"""

import importlib.resources
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from functools import cache
from itertools import product
from typing import Any

import requests
from bs4 import (
    BeautifulSoup,
    Tag,
)
from sentence_transformers import (
    SentenceTransformer,
    util,
)

from exasol.ai.mcp.server.tools.dialect_tools import (
    BUILTIN_FUNCTIONS_JSON,
    PACKAGE_RESOURCES,
)

MAX_PURPOSE_SIMILARITY = 0.95


@dataclass
class FuncInfo:
    name: str
    description: str
    url: str
    alias: str | None = None


@dataclass
class FuncInCategory:
    key: str
    category: str


# IS_* functions have to be hardcoded.
is_functions = [
    {
        "name": "IS_BOOLEAN",
        "types": ["conversion"],
        "description": "Returns TRUE if string can be converted to a BOOLEAN.",
        "usage-notes": ["If the argument is NULL, then NULL is returned."],
        "example": "SELECT IS_BOOLEAN('xyz') IS_BOOLEAN;",
    },
    {
        "name": "IS_DATE",
        "types": ["conversion", "datetime"],
        "description": "Returns TRUE if string can be converted to a DATE.",
        "usage-notes": [
            "If the argument is NULL, then NULL is returned.",
            "If a format is specified, then the rules of the TO_DATE function apply.",
        ],
        "example": "SELECT IS_DATE('12.13.2011', 'DD.MM.YYYY') IS_DATE;",
    },
    {
        "name": "IS_DSINTERVAL",
        "types": ["conversion", "datetime"],
        "description": "Returns TRUE if string can be converted to an INTERVAL DAY TO SECOND.",
        "usage-notes": ["If the argument is NULL, then NULL is returned."],
    },
    {
        "name": "IS_NUMBER",
        "types": ["conversion", "numeric"],
        "description": "Returns TRUE if string can be converted to a DECIMAL OR DOUBLE.",
        "usage-notes": [
            "If the argument is NULL, then NULL is returned.",
            "If a format is specified, then the rules of the TO_NUMBER function apply.",
        ],
        "example": "SELECT IS_NUMBER('+12.34') IS_NUMBER;",
    },
    {
        "name": "IS_TIMESTAMP",
        "types": ["conversion", "datetime"],
        "description": "Returns TRUE if string can be converted to a TIMESTAMP.",
        "usage-notes": [
            "If the argument is NULL, then NULL is returned.",
            "If a format is specified, then the rules of the TO_TIMESTAMP function apply.",
        ],
    },
    {
        "name": "IS_YMINTERVAL",
        "types": ["conversion", "datetime"],
        "description": "Returns TRUE if string can be converted to an INTERVAL YEAR TO MONTH.",
        "usage-notes": ["If the argument is NULL, then NULL is returned."],
    },
]


@cache
def compile_pattern(pattern: str) -> re.Pattern:
    return re.compile(pattern, re.DOTALL)


def clean_text(text: str) -> str:
    # Replace multiple spaces with one.
    pattern = compile_pattern(r"[\u00a0|\r|\n|\t]+")
    text = pattern.sub(" ", text).strip()
    # Escape double quotes.
    pattern = compile_pattern(r'(?<!\\)"')
    text = pattern.sub(r"\"", text)
    return text


def get_content(url: str) -> str:
    response = requests.get(url)
    response.raise_for_status()
    content = str(response.content, encoding="utf-8")
    return content


def extract_purpose(soup: BeautifulSoup) -> str | None:
    h2_element = soup.find("h2", id="Purpose")
    if h2_element:
        p_element = h2_element.find_next("p")
        if p_element:
            return clean_text(p_element.text)
    return None


def extract_usage_notes(soup: BeautifulSoup) -> list[str] | None:
    def find_in_h2(h2: BeautifulSoup, target: str) -> Tag | None:
        target_element = h2.find_next(target)
        if target_element:
            # The Usage Notes section might be empty, in which case the found element
            # could be in the sibling h2 or elsewhere. Need to watch out for that.
            if target_element not in h2.parent.find_all(recursive=True):
                return None
            next_h2 = h2.find_next("h2")
            if next_h2:
                next_target_element = next_h2.find_next(target)
                if next_target_element == target_element:
                    return None
            return target_element
        return None

    h2_element = soup.find("h2", id="UsageNotes")
    if not h2_element:
        h2_element = soup.find("h2", id="Usagenotes")
    if h2_element:
        ul_element = find_in_h2(h2_element, "ul")
        if ul_element:
            li_elements = ul_element.find_all("li")
            if li_elements:
                notes = [clean_text(li.text) for li in li_elements]
                return notes
        else:
            p_element = find_in_h2(h2_element, "p")
            if p_element:
                return [clean_text(p_element.text)]
    return None


def extract_examples(soup: BeautifulSoup) -> str | None:
    h2_element = soup.find("h2", id="Example")
    if h2_element is None:
        h2_element = soup.find("h2", id="Examples")
    if h2_element:
        code_element = h2_element.find_next("code")
        if code_element:
            text = clean_text(code_element.text)
            # Make sure there is a space between examples
            pattern = compile_pattern(r";(?!\s)")
            text = pattern.sub("; ", text)
            return text.strip()
    return None


def extract_info(func_info: FuncInfo, model: SentenceTransformer) -> dict[str, Any]:
    def format_func_text(text: str) -> str:
        prefix_to_cut = "this function "
        if text.lower().startswith(prefix_to_cut):
            text = text[len(prefix_to_cut) :]
        return f"{func_info.name}: {text}"

    content = get_content(func_info.url)
    soup = BeautifulSoup(content, "html.parser")
    extra_info = {
        "usage-notes": extract_usage_notes(soup),
        "example": extract_examples(soup),
    }
    purpose = extract_purpose(soup)
    if purpose:
        # Compare purpose to description, and add it only if it is not very similar.
        embeddings = model.encode(
            [format_func_text(purpose), format_func_text(func_info.description)]
        )
        similarity = util.cos_sim(*embeddings).item()
        if similarity <= MAX_PURPOSE_SIMILARITY:
            extra_info["purpose"] = purpose
    return extra_info


def alphabetical_list() -> dict[str, FuncInfo]:
    alias_pattern = compile_pattern(r"\[\w+\]")
    description_pattern = compile_pattern(r"[\n|\t]+")

    def create_func_info(div) -> tuple[str, FuncInfo]:
        p_elements = div.find_all("p")
        a_element = p_elements[0].find("a")

        key = clean_text(p_elements[0].text)
        ref_name = key.split()[0]
        name = ref_name.replace("[", "").replace("]", "")
        alias = None if name == ref_name else alias_pattern.sub("", ref_name)
        description = clean_text(p_elements[1].text)
        description = description_pattern.sub(" ", description)

        return key, FuncInfo(
            name,
            description,
            "https://docs.exasol.com/db/latest/sql_references/functions/"
            + a_element.get("href"),
            alias,
        )

    url = "https://docs.exasol.com/db/latest/sql_references/functions/all_functions.htm"
    content = get_content(url)
    soup = BeautifulSoup(content, "html.parser")
    h1_element = soup.find("h1", id="AlphabeticalListofFunctions")
    div1_element = h1_element.find_next("div")
    div2_elements = div1_element.find_all("div")
    if not div2_elements:
        raise RuntimeError(f"Cannot load the alphabetic list of functions")
    func_infos = {
        key: info for key, info in [create_func_info(div) for div in div2_elements]
    }

    # Missed entries, that have to be hardcoded.
    func_infos["WIDTH_BUCKET"] = FuncInfo(
        "WIDTH_BUCKET",
        "Returns a bucket number for an operand, based on 2 boundaries and the amount of equi-width buckets between them.",
        "https://docs.exasol.com/db/latest/sql_references/functions/alphabeticallistfunctions/width_bucket.htm",
    )
    return func_infos


def scalar_list() -> list[FuncInCategory]:
    url = (
        "https://docs.exasol.com/db/latest/sql_references/functions/scalarfunctions.htm"
    )
    content = get_content(url)
    soup = BeautifulSoup(content, "html.parser")
    func_list: list[FuncInCategory] = []

    for category, html_id in [
        ("numeric", "Numericfunctions"),
        ("string", "Stringfunctions"),
        ("datetime", "Datetimefunctions"),
        ("conversion", "Conversionfunctions"),
        ("json", "JSONfunctions"),
        ("other-scalar", "Otherscalarfunctions"),
    ]:
        h2_element = soup.find("h2", id=html_id)
        p_elements = h2_element.find_next("div").find_all("p")
        if not p_elements:
            raise RuntimeError(f"Cannot load the list of {category} functions")
        func_list.extend(FuncInCategory(p.text.strip(), category) for p in p_elements)

    # There is annoying special case - "CONNECT BY" functions.
    h3_element = soup.find("h3", string="Functions for hierarchical queries")
    p_elements = h3_element.find_next("div").find_all("p")
    if not p_elements:
        raise RuntimeError("Cannot load the list of connect-by functions")
    func_list.extend(FuncInCategory(p.text.strip(), "connect-by") for p in p_elements)
    return func_list


def aggregate_list() -> list[FuncInCategory]:
    url = "https://docs.exasol.com/db/latest/sql_references/functions/aggregatefunctions.htm"
    content = get_content(url)
    soup = BeautifulSoup(content, "html.parser")
    h1_element = soup.find("h1", id="AggregateFunctions")
    p_elements = h1_element.find_next("div").find_all("p")
    if not p_elements:
        raise RuntimeError("Cannot load the list of aggregate functions")
    return [FuncInCategory(p.text.strip(), "aggregate") for p in p_elements]


def analytic_list() -> list[FuncInCategory]:
    url = "https://docs.exasol.com/db/latest/sql_references/functions/analyticfunctions.htm"
    content = get_content(url)
    soup = BeautifulSoup(content, "html.parser")
    h2_element = soup.find("h2", id="SupportedAnalyticFunctions")
    p_elements = h2_element.find_next("div").find_all("p")
    if not p_elements:
        raise RuntimeError("Cannot load the list of analytic functions")
    return [FuncInCategory(p.text.strip(), "analytic") for p in p_elements]


def geospatial_functions() -> list[dict[str, Any]]:
    url = "https://docs.exasol.com/db/latest/sql_references/geospatialdata/geospatial_functions.htm"
    content = get_content(url)
    soup = BeautifulSoup(content, "html.parser")
    tr_elements = soup.find_all(
        "tr", class_="TableStyle-Standard-Body-Body1"
    ) + soup.find_all("tr", class_="TableStyle-Standard-Body-Body2")
    if not tr_elements:
        raise RuntimeError("Cannot load the information on geospatial functions")
    syntax_pattern = compile_pattern(r"\s*\(\s*\w+[,\s*\w+]*\s*\)")

    def extract_func_info(tr) -> dict[str, Any]:
        td = tr.find_next("td")
        syntax = clean_text(td.text)
        name = syntax_pattern.sub("", syntax)
        td = td.find_next("td")
        description = clean_text(td.text)
        return {
            "name": name,
            "types": ["geospatial"],
            "syntax": syntax,
            "description": description,
        }

    func_data = [extract_func_info(tr) for tr in tr_elements]

    # Extract examples
    h6_element = soup.find("h6", class_="ForExamples")
    if h6_element:
        examples = [clean_text(code.text) for code in h6_element.find_all_next("code")]
        # Attach examples to the info
        for func_info, example in product(func_data, examples):
            pattern = compile_pattern(rf"{func_info['name']}\s*\(")
            if pattern.search(example):
                func_example = (
                    f"{func_info['example']}\n{example}"
                    if "example" in func_info
                    else example
                )
                func_info["example"] = func_example
    return func_data


def build_json() -> list[dict[str, Any]]:
    # Load function lists for all categories
    plain_list = scalar_list() + aggregate_list() + analytic_list()
    func_key_cats = defaultdict(list)
    # Group by the key, creating a list of categories for each function.
    for fun_cat in plain_list:
        func_key_cats[fun_cat.key].append(fun_cat.category)

    model = SentenceTransformer("all-MiniLM-L6-v2")

    # Load the functions' information from the alphabetical summary page
    # and merge with the list of categories.
    func_infos = alphabetical_list()
    func_data: list[dict[str, Any]] = []
    for key, cats in func_key_cats.items():
        if not key.startswith("IS_") and not key.startswith("ST_"):
            func_info = func_infos[key]
            func_data.append(
                {
                    "name": func_info.name,
                    "alias": func_info.alias,
                    "types": cats,
                    "description": func_info.description,
                    **extract_info(func_info, model),
                }
            )

    # Add special cases
    func_data.extend(geospatial_functions())
    func_data.extend(is_functions)

    return func_data


def write_json(func_data: list[dict[str, Any]]) -> None:
    with importlib.resources.path(PACKAGE_RESOURCES, BUILTIN_FUNCTIONS_JSON) as path:
        with open(path, "w") as f:
            json.dump(func_data, f, indent=4, ensure_ascii=False)

        # Verify that it can be read back
        with open(path) as f:
            json.load(f)


if __name__ == "__main__":
    write_json(build_json())
