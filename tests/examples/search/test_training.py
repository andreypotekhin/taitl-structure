import pytest

from examples.search.algorithms.training import LinearTraining, TrainingExample


def test_linear_training_is_deterministic_and_orders_ties_by_document() -> None:
    examples = [
        TrainingExample("q", "b", 0.0, {"lexical": 0.0}),
        TrainingExample("q", "a", 1.0, {"lexical": 1.0}),
        TrainingExample("q", "c", 1.0, {"lexical": 1.0}),
    ]

    trainer = LinearTraining(epochs=100, learning_rate=0.1, l2=0.0)
    first = trainer.train(examples)
    second = trainer.train(list(reversed(examples)))

    assert first == second
    assert [example.document_id for example in first.rank("q", examples)] == ["a", "c", "b"]


def test_training_rejects_invalid_examples_and_settings() -> None:
    with pytest.raises(ValueError, match="nonblank"):
        TrainingExample("", "d", 1.0, {})
    with pytest.raises(ValueError, match="positive"):
        LinearTraining(epochs=0)
    with pytest.raises(ValueError, match="at least one"):
        LinearTraining().train([])
