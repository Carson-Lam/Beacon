"""
Promotion gate for beacon-direction

Metric:
- mean walk-forward log loss across all test folds
- Can't be worse than baseline

"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Decision:
    promote: bool
    reason: str


def pick_candidate(mean_log_loss: dict[str, float], order: list[str]) -> str:
    """
    Variants are nested and each group is kept only if it lowers mean log loss against the variant without it
    """
    best = order[0]
    for variant in order[1:]:
        if mean_log_loss[variant] >= mean_log_loss[best]:
            break
        best = variant
    return best


def decide(candidate: float, always_up: float, champion: float | None) -> Decision:
    """
    Decide whether to promote a candidate based on its log loss compared to the baseline and the current champion.
    """
    if candidate >= always_up:
        return Decision(False, f"mean log loss {candidate:.4f} does not beat always-up {always_up:.4f}")
    if champion is not None and candidate >= champion:
        return Decision(False, f"mean log loss {candidate:.4f} does not beat the champion's {champion:.4f}")
    return Decision(True, f"mean log loss {candidate:.4f} beats always-up {always_up:.4f}"
                          + ("" if champion is None else f" and the champion's {champion:.4f}"))