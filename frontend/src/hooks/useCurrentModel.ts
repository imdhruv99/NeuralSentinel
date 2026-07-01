import { useEffect, useState } from "react";
import { apiFetch } from "../api/client";
import type { CurrentModel } from "../api/types";

export function useCurrentModel() {
    const [models, setModels] = useState<CurrentModel[]>([]);

    useEffect(() => {
        apiFetch<CurrentModel[]>("models/current")
            .then(setModels)
            .catch((error) => {
                console.error("Failed to fetch current models:", error);
            });
    }, []);

    return models;
}
