import pytest

from app.scoring import anomaly_score


def test_all_bot_neighbors():
    assert anomaly_score([(0.9, "bot"), (0.8, "bot")]) == 1.0


def test_weighted_by_similarity():
    assert anomaly_score([(0.9, "bot"), (0.3, "human")]) == pytest.approx(0.75)


def test_empty_and_negative():
    assert anomaly_score([]) == 0.0
    assert anomaly_score([(-0.5, "bot"), (0.5, "human")]) == 0.0
