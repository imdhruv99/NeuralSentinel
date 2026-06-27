from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ScoreRow(BaseModel):
    """
    Represents a row in the scores table.

    Attributes:
        entity_id (str): The ID of the entity.
        window_end (datetime): The end of the time window for the score.
        model_name (str): The name of the model that generated the score.
        model_version (str): The version of the model that generated the score.
        anomaly_score (float): The anomaly score for the entity in the given time window.
        is_anomaly (bool): Whether the score indicates an anomaly.
        scored_at (datetime): The timestamp when the score was generated.
    """
    model_config = ConfigDict(from_attributes=True)

    entity_id: str
    window_end: datetime
    model_name: str
    model_version: int
    anomaly_score: float
    is_anomaly: bool
    scored_at: datetime


class AlertPage(BaseModel):
    """
    Represents a page of alerts fetched from the database.

    Attributes:
        items (list[ScoreRow]): A list of ScoreRow objects representing the alerts.
        count (int): The total number of alerts available.
        next_cursor (str | None): A cursor for fetching the next page of alerts, if available.
    """
    items: list[ScoreRow]
    count: int
    next_cursor: str | None


class EntitySeries(BaseModel):
    """
    Represents a series of scores for a specific entity.

    Attributes:
        entity_id (str): The ID of the entity.
        items (list[ScoreRow]): A list of ScoreRow objects representing the scores for the entity.
        count (int): The total number of scores available for the entity.
        next_cursor (str | None): A cursor for fetching the next page of scores, if available.
    """
    entity_id: str
    items: list[ScoreRow]
    count: int
    next_cursor: str | None


class CurrentModel(BaseModel):
    """
    Represents the current model configuration.

    Attributes:
        model_name (str): The name of the model.
        version (str): The version of the model.
        decision (str): The decision associated with the model (e.g., "promoted", "rejected").
        promoted_at (datetime): The timestamp when the model was promoted.
    """
    model_config = ConfigDict(from_attributes=True)

    model_name: str
    version: int
    decision: str
    promoted_at: datetime


class HealthResponse(BaseModel):
    """
    Represents the health status of the service.

    Attributes:
        status (str): The health status of the service (e.g., "healthy", "unhealthy").
        db (str): The status of the database connection (e.g., "connected", "disconnected").
    """
    status: str
    db: str
