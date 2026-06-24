import argparse
import json
import logging

import mlflow
import mlflow.tracking
import psycopg

from config.logging import setup_logging
from ml.evaluation.config import EvalConfig
from ml.evaluation.evaluator import EvalResult, evaluate
from ml.evaluation.promoter import decide, Decision

logger = logging.getLogger(__name__)


_MODEL_NAMES = {
    "iforest": "neural-sentinel-isolation-forest-model",
    "lstm": "neural-sentinel-lstm-autoencoder-model",
}


def _get_version_by_stage(client: mlflow.tracking.MlflowClient, model_name: str, stage: str) -> int | None:
    """
    Get the version of a registered model by stage.

    Args:
        client: An instance of MlflowClient.
        model_name: The name of the registered model.
        stage: The stage of the model version (e.g., "Production", "Staging").

    Returns:
        The version number of the model in the specified stage, or None if not found.
    """
    versions = client.get_latest_versions(model_name, stages=[stage])
    return int(versions[0].version) if versions else None


def run_evaluation(model_key: str, cfg: EvalConfig) -> None:
    """
    Run evaluation for a given model key (iforest or lstm) and decide whether to promote the challenger model.

    Args:
        - model_key: A string key representing the model type ("iforest" or "lstm").
        - cfg: EvalConfig object containing evaluation parameters.

    Raises:
        - ValueError: If the model_key is not recognized.
    """
    model_name = _MODEL_NAMES[model_key]
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    client = mlflow.tracking.MlflowClient()

    # Find the challenger (latest Staging Version)
    challenger_version = _get_version_by_stage(client, model_name, "Staging")
    if challenger_version is None:
        versions = client.search_model_versions(f"name='{model_name}'")
        if not versions:
            raise SystemExit(f"No versions found for {model_name} in MLflow")
        challenger_version = max(int(v.version) for v in versions)
        logger.info(
            "no Staging version found — using latest version %d as challenger", challenger_version)
    # Find the champion (current Production Version)
    champion_version = _get_version_by_stage(client, model_name, "Production")

    logger.info(
        "model=%s  challenger_version=%d  champion_version=%s",
        model_name, challenger_version, champion_version,
    )

    # Evaluate
    logger.info("evaluating challenger (version %d)...", challenger_version)
    challenger_result = evaluate(model_name, challenger_version, cfg)
    logger.info("challenger metrics: %s", challenger_result.metrics)

    champion_result: EvalResult | None = None
    if champion_version is not None:
        logger.info("evaluating champion (version %d)...", champion_version)
        champion_result = evaluate(model_name, champion_version, cfg)
        logger.info("champion metrics: %s", champion_result.metrics)

    # Decide whether to promote challenger to Production
    verdict = decide(challenger_result, champion_result,
                     cfg.promote_min_f1_score, cfg.promote_min_delta)
    logger.info("decision=%s  reason=%s", verdict.decision, verdict.reason)

    # Act on the verdict
    if verdict.decision in (Decision.PROMOTE, Decision.NO_CHAMPION):
        logger.info("promoting version %d to Production", challenger_version)
        client.transition_model_version_stage(
            name=model_name,
            version=str(challenger_version),
            stage="Production",
            archive_existing_versions=True,  # moves old Production to Archived
        )
    else:
        logger.info("keeping current champion (version %d)", champion_version)

    # Log the evaluation and promotion decision to MLflow
    _log_promotion(
        cfg=cfg,
        model_name=model_name,
        challenger_version=challenger_version,
        champion_version=champion_version,
        verdict=verdict,
        challenger_metrics=challenger_result.metrics,
        champion_metrics=champion_result.metrics if champion_result else None,
    )


def _log_promotion(
    cfg: EvalConfig,
    model_name: str,
    challenger_version: int,
    champion_version: int | None,
    verdict,
    challenger_metrics: dict,
    champion_metrics: dict | None,
) -> None:
    """
    Logs the promotion decision and metrics to the database.

    Args:
        cfg: EvalConfig object containing evaluation parameters.
        model_name: Name of the model being evaluated.
        challenger_version: Version number of the challenger model.
        champion_version: Version number of the champion model, if any.
        verdict: Decision object containing the promotion decision and reason.
        challenger_metrics: Dictionary of metrics for the challenger model.
        champion_metrics: Dictionary of metrics for the champion model, if any.

    Raises:
        - psycopg.Error: If there is an error connecting to or executing the SQL command
    """
    sql = """
        INSERT INTO model_promotions
            (model_name, challenger_version, champion_version,
             decision, reason, challenger_metrics, champion_metrics)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with psycopg.connect(cfg.pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                model_name,
                challenger_version,
                champion_version,
                verdict.decision.value,
                verdict.reason,
                json.dumps(challenger_metrics),
                json.dumps(champion_metrics) if champion_metrics else None,
            ))
        conn.commit()
    logger.info("audit record written to model_promotions")


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(
        prog="python -m ml.evaluation.main",
        description="NeuralSentinel champion-challenger evaluation and promotion.",
    )
    parser.add_argument(
        "model",
        choices=list(_MODEL_NAMES.keys()),
        help="Which model to evaluate: iforest or lstm-ae",
    )
    args = parser.parse_args()

    cfg = EvalConfig()
    run_evaluation(args.model, cfg)


if __name__ == "__main__":
    main()
