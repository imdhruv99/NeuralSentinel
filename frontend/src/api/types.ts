export interface ScoreRow {
    entity_id: string;
    window_end: string;
    model_name: string;
    model_version: number;
    anomaly_score: number;
    is_anomaly: boolean;
    scored_at: string;
}

export interface AlertPage {
    items: ScoreRow[];
    count: number;
    next_cursor: string | null;
}

export interface EntitySeries {
    entity_id: string;
    items: ScoreRow[];
    count: number;
    next_cursor: string | null;
}

export interface CurrentModel {
    model_name: string;
    version: number;
    decision: string;
    promoted_at: string;
}
