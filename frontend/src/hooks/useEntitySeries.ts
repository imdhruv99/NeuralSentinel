import { useEffect, useState } from "react";

import { apiFetch } from "../api/client";
import type { EntitySeries, ScoreRow } from "../api/types";

export function useEntitySeries(entityId: string | null) {
    const [data, setData] = useState<ScoreRow[] | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        if (!entityId) return;

        // eslint-disable-next-line react-hooks/set-state-in-effect
        setLoading(true);
        setError(null);

        apiFetch<EntitySeries>(
            `/entities/series?entity_id=${encodeURIComponent(entityId)}&limit=200`
        )
            .then((body) => {
                setData(body.items);
            })
            .catch((err) => {
                setError(err.message);
            })
            .finally(() => {
                setLoading(false);
            });
    }, [entityId]);

    return {
        data: entityId ? (data ?? []) : [],
        loading,
        error,
    };
}
