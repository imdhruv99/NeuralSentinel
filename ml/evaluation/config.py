from pydantic import Field
from pydantic_settings import BaseSettings

from config.settings import _MODEL_CONFIG, _MLFlowSettings, _PostgresSettings


class EvalConfig(_PostgresSettings, _MLFlowSettings, BaseSettings):

    model_config = _MODEL_CONFIG

    promote_min_f1_score: float = Field(
        default=0.0, alias="PROMOTE_MIN_F1_SCORE")
    promote_min_delta: float = Field(default=0.01, alias="PROMOTE_MIN_DELTA")
    eval_validation_ratio: float = Field(
        default=0.2, alias="EVAL_VALIDATION_RATIO")
