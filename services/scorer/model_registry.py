"""
Live model registry with hot-reload on Production version changes.

Maintains one loaded model + calibration artifact per registered model name.
Calling refresh() checks MLflow for the current Production version; if the
version changed the new model is loaded and atomically swapped into the
in-memory slot - zero-downtime promotion handling without a process restart.

Thread-safety: a single RLock guards all reads and writes to the internal
model map.  Model loads are infrequent (seconds), so coarse-grained locking
is an acceptable trade-off over a read-write lock here.
"""

import json
import logging
import tempfile
import threading
from dataclasses import dataclass
from typing import Any

import mlflow
import mlflow.artifacts
import mlflow.pytorch
import mlflow.sklearn
import mlflow.tracking

from services.scorer.config import ScorerConfig

logger = logging.getLogger(__name__)


@dataclass
class LoadedModel:
    """
    Snapshot of a Production model at a specific MLflow version.

    Immutable after construction — the registry replaces the slot entirely
    rather than mutating in place, which keeps the swap race-free under the lock.
    """

    name: str
    version: int
    model: Any        # sklearn Pipeline (iforest) | TorchScript module (lstm)
    calibration: dict
    model_type: str   # "iforest" | "lstm"


class ModelRegistry:
    """
    Thread-safe, hot-reloadable wrapper around the MLflow model registry.

    Lifecycle
    ---------
    1. Call warm_up() once at startup to load all Production models.
    2. Call refresh() periodically from the scoring loop to detect promotions.
       Any model whose Production version has changed since the last check is
       reloaded and atomically swapped; all other models are untouched.
    3. Call get(model_name) from the hot path to retrieve the current snapshot.

    The registry silently skips models with no Production version (e.g. before
    the first promotion) and continues scoring with any model that is available.
    """

    _CALIBRATION_ARTIFACT: dict[str, str] = {
        "neural-sentinel-isolation-forest-model": "calibration/isolation_forest_threshold.json",
        "neural-sentinel-lstm-autoencoder-model": "calibration/lstm_ae_threshold.json",
    }
    _MODEL_TYPE: dict[str, str] = {
        "neural-sentinel-isolation-forest-model": "iforest",
        "neural-sentinel-lstm-autoencoder-model": "lstm",
    }

    def __init__(self, cfg: ScorerConfig) -> None:
        self._cfg = cfg
        self._lock = threading.RLock()
        self._models: dict[str, LoadedModel] = {}
        mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
        self._client = mlflow.tracking.MlflowClient(cfg.mlflow_tracking_uri)

    # Public interface
    def warm_up(self) -> None:
        """
        Load all Production models synchronously.  Called once on startup.
        Logs a warning for any model without a Production version rather than
        raising, so a partial deployment (one model promoted, the other not yet)
        doesn't block the service from starting.
        """
        for name in (self._cfg.iforest_model_name, self._cfg.lstm_model_name):
            self._load_if_changed(name)

    def refresh(self) -> None:
        """
        Check MLflow for Production version changes and hot-swap any that changed.

        Errors during a single model's refresh are caught and logged so a
        transient MLflow connectivity issue doesn't take down the whole loop.
        The previous model version continues serving until the next successful
        refresh.
        """
        for name in (self._cfg.iforest_model_name, self._cfg.lstm_model_name):
            try:
                self._load_if_changed(name)
            except Exception:
                logger.exception(
                    "model refresh failed for %s — keeping current version", name
                )

    def get(self, model_name: str) -> LoadedModel | None:
        """Return the currently loaded model snapshot, or None if not yet loaded."""
        with self._lock:
            return self._models.get(model_name)

    # Internal helpers
    def _current_production_version(self, model_name: str) -> int | None:
        try:
            versions = self._client.get_latest_versions(
                model_name, stages=["Production"]
            )
            return int(versions[0].version) if versions else None
        except Exception:
            logger.exception(
                "failed to query Production version for %s", model_name
            )
            return None

    def _load_if_changed(self, model_name: str) -> None:
        version = self._current_production_version(model_name)
        if version is None:
            logger.warning(
                "no Production version found for %s — scoring unavailable for this model",
                model_name,
            )
            return

        with self._lock:
            current = self._models.get(model_name)
            if current is not None and current.version == version:
                return  # already current; nothing to do

        logger.info("loading %s v%d from MLflow", model_name, version)
        loaded = self._load(model_name, version)

        with self._lock:
            self._models[model_name] = loaded

        logger.info("hot-swapped %s → v%d", model_name, version)

    def _load(self, model_name: str, version: int) -> LoadedModel:
        model_uri = f"models:/{model_name}/{version}"
        model_type = self._MODEL_TYPE[model_name]

        if model_type == "iforest":
            model = mlflow.sklearn.load_model(model_uri)
        else:
            model = mlflow.pytorch.load_model(model_uri)

        mv = self._client.get_model_version(model_name, str(version))
        artifact_path = self._CALIBRATION_ARTIFACT[model_name]

        with tempfile.TemporaryDirectory() as tmpdir:
            local = mlflow.artifacts.download_artifacts(
                run_id=mv.run_id,
                artifact_path=artifact_path,
                dst_path=tmpdir,
            )
            with open(local) as fh:
                calibration = json.load(fh)

        return LoadedModel(
            name=model_name,
            version=version,
            model=model,
            calibration=calibration,
            model_type=model_type,
        )
