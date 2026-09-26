"""Anomaly scoring from k-NN neighbors (spec §4.4)."""

from collections.abc import Iterable


def anomaly_score(neighbors: Iterable[tuple[float, str]]) -> float:
    """Similarity-weighted share of neighbors labeled "bot", from 0 to 1.

    `neighbors` are (cosine similarity, label) pairs. Features are non-negative,
    so similarities are in [0, 1]; negatives are clamped to 0 regardless.
    No neighbors (an empty index) scores 0.
    """
    total = bot = 0.0
    for similarity, label in neighbors:
        weight = max(similarity, 0.0)
        total += weight
        if label == "bot":
            bot += weight
    return bot / total if total > 0 else 0.0
