import re
from collections import Counter
from collections.abc import (
    Generator,
    Iterable,
)
from functools import cache
from itertools import chain
from math import log
from typing import Any

import numpy as np


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


@cache
def _get_word_extract_pattern() -> re.Pattern:
    pattern = r"\w+"
    return re.compile(pattern, flags=re.MULTILINE | re.UNICODE | re.IGNORECASE)


def _extract_raw_words(sentences: Iterable[str]) -> Generator[None, None, str]:
    for sentence in sentences:
        for match in _get_word_extract_pattern().finditer(
            _get_word_split_pattern().sub(" ", sentence)
        ):
            yield match.group().lower()


def extract_words(sentences: Iterable[str], model_name: str | None = None) -> list[str]:
    """
    Extracts words from all texts in the provided collection.
    All words in camel case or snake case get split into multiple words.
    All extracted words are returned lowercased.
    """
    return list(_extract_raw_words(sentences))


def get_match_scores(corpus: list[list[str]], keywords: list[str]) -> list[float]:
    """
    For each text in the corpus assigns a keyword matching score.
    Computes the score as the cosine similarity between the TF-IDF vectors
    of the keywords and each of the text in the corpus.

    Here is an implementation of the vanilla TF-IDF algorithm. Alternatively, the
    scikit-learn `TfidfVectorizer` could be used. This would allow using additional
    options and features, including preprocessing. Whether it will justify adding the
    scikit-learn dependency is an open question. In regard to preprocessing, the method
    of preference is the spaCy pipeline.

    Also to consider, is replacing TF-IDF with a better algorithm, e.g. BM25.
    """
    text_counters = [Counter(text) for text in chain(corpus, [keywords])]
    vocab_counter = Counter()
    for counter in text_counters:
        vocab_counter.update(counter.keys())
    vocab_index = {word: index for index, word in enumerate(vocab_counter)}
    vocab_size = len(vocab_index)
    corpus_size = len(corpus) + 1

    def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

    def tf_idf(c: Counter, num_words: int) -> np.ndarray:
        u = np.zeros(vocab_size)
        for word, freq in c.items():
            tf = freq / num_words
            idf = log(corpus_size / vocab_counter[word])
            word_index = vocab_index[word]
            u[word_index] = tf * idf
        return u

    kw = tf_idf(text_counters[-1], len(keywords))
    return [
        cosine_similarity(kw, tf_idf(counter, len(text)))
        for counter, text in zip(text_counters, corpus)
    ]


def _clipped_k_means(points: np.ndarray, max_iters=100) -> np.ndarray:
    """
    A simple implementation of K-means clustering for the special case:
        * 1-dimensional points,
        * 2 clusters,
        * the first centroid is clipped at the highest point value.
    The algorithm aims at picking one or a few outliers at the top end of the point
    value spectrum. The normal k-means may not work well in this case.
    """
    centroids = np.expand_dims(np.array([points.max(), points.mean()]), 1)
    prev_centroid = centroids[1, 0]
    labels = np.zeros_like(points, dtype=int)

    for _ in range(max_iters):
        # If both centroids collapsed into one point, stop
        if np.isclose(centroids[0, 0], centroids[1, 0]):
            break

        # Normal assignment step: assign each point to the closest centroid
        distances = np.square(points - centroids)
        labels = np.argmin(distances, axis=0)

        # If all points converged to cluster 0 (very unlikely case), then stop.
        if np.sum(labels) == 0:
            break

        # Update step, only update the centroid 1
        centroids[1, 0] = points[labels == 1].mean()

        # Check for convergence
        if np.isclose(centroids[1, 0], prev_centroid):
            break
        prev_centroid = centroids[1, 0]
    return labels


def top_score_indices(scores: list[float]) -> list[int]:
    """
    Divides the scores in the list into two clusters. Returns indices of scores in
    the cluster with higher scores. The returned indices are sorted by the score
    in the descending order.
    """
    labels = _clipped_k_means(np.asarray(scores))
    res = np.where(labels == 0)[0].tolist()
    return sorted(res, key=lambda i: scores[i], reverse=True)


def keyword_filter(
    input_rows: list[dict[str, Any]], key_phrases: list[str]
) -> list[dict[str, Any]]:
    """
    For each row in the input list, computes the keyword matching score. Returns the
    rows ordered by the score in descending order. Filters out rows with relatively
    low scores.

    Args:
        input_rows:
            list of input rows, formatted as {column_name: column_value} dictionaries.
        key_phrases:
            list of key_phrases.
    """
    keywords = extract_words(key_phrases)
    corpus = [
        extract_words(filter(lambda v: isinstance(v, str), di.values()))
        for di in input_rows
    ]
    scores = get_match_scores(corpus, keywords)
    indices = top_score_indices(scores)
    return [input_rows[i] for i in indices]
