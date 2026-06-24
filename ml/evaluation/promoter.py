from dataclasses import dataclass
from enum import Enum
import logging

from ml.evaluation.evaluator import EvalResult

logger = logging.getLogger(__name__)


class Decision(str, Enum):
    """
    Enum representing the possible decisions for model promotion.
    """
    PROMOTE = "PROMOTE",
    KEEP = "KEEP",
    NO_CHAMPION = "NO_CHAMPION",  # first run, no champion model exists yet


@dataclass
class PromotionVerdict:
    """
    Dataclass representing the verdict of a model promotion decision.
    """
    decision: Decision
    reason: str
    challenger_f1: float
    champion_f1: float | None


def decide(
    challenger: EvalResult,
    champion: EvalResult | None,
    min_f1: float,
    min_delta: float,
) -> PromotionVerdict:
    """
    Decide whether to promote a challenger model based on its F1 score compared to the champion model.

    Args:
        challenger (EvalResult): The evaluation result of the challenger model.
        champion (EvalResult | None): The evaluation result of the champion model, or None if no champion exists.
        min_f1 (float): The minimum F1 score required for promotion.
        min_delta (float): The minimum F1 score improvement required for promotion.

    Returns:
        PromotionVerdict: The verdict of the promotion decision.
    """
    if champion is None:
        return PromotionVerdict(
            decision=Decision.NO_CHAMPION,
            reason="No champion model exists yet. Promoting challenger unconditionally",
            challenger_f1=challenger.f1,
            champion_f1=None,
        )

    if challenger.f1 < min_f1:
        return PromotionVerdict(
            decision=Decision.KEEP,
            reason=f"Challenger F1 score {challenger.f1:.4f} is below the minimum threshold of {min_f1:.4f}",
            challenger_f1=challenger.f1,
            champion_f1=champion.f1,
        )

    delta = challenger.f1 - champion.f1
    if delta >= min_delta:
        return PromotionVerdict(
            decision=Decision.PROMOTE,
            reason=f"Challenger F1 score {challenger.f1:.4f} exceeds champion F1 score {champion.f1:.4f} by {delta:.4f}, which is above the minimum delta of {min_delta:.4f}",
            challenger_f1=challenger.f1,
            champion_f1=champion.f1,
        )

    return PromotionVerdict(
        decision=Decision.KEEP,
        reason=f"Challenger F1 score {challenger.f1:.4f} does not exceed champion F1 score {champion.f1:.4f} by the minimum delta of {min_delta:.4f}",
        challenger_f1=challenger.f1,
        champion_f1=champion.f1,
    )
