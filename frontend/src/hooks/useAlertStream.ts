import { useEffect, useRef, useState } from 'react';
import { fetchEventSource } from '@microsoft/fetch-event-source';
import { API_BASE, API_KEY } from '../api/client';
import type { ScoreRow } from '../api/types';

const MAX_ALERTS = 100;

export function useAlertStream() {
    const [alerts, setAlerts] = useState<ScoreRow[]>([]);
    const [connected, setConnected] = useState(false);
    // AbortController lets us cancel the stream when the component unmounts
    const ctrl = useRef<AbortController | null>(null);

    useEffect(() => {
        ctrl.current = new AbortController();

        fetchEventSource(`${API_BASE}/alerts/stream`, {
            headers: { 'X-API-Key': API_KEY },
            signal: ctrl.current.signal,
            onopen: async (res) => {
                if (res.ok) setConnected(true);
            },
            onmessage: (event) => {
                if (!event.data) return;
                try {
                    const alert = JSON.parse(event.data) as ScoreRow;
                    setAlerts(prev => [alert, ...prev].slice(0, MAX_ALERTS));
                } catch {
                    // malformed event; ignore and continue
                }
            },
            onclose: () => setConnected(false),
            onerror: (err) => {
                setConnected(false);
                // fetchEventSource retries automatically on transient errors.
                // Throwing here would stop retries, keep trying.
                console.warn('SSE error, will retry:', err);
            },
        });

        return () => ctrl.current?.abort();
    }, []);  // [] = run once on mount, abort on unmount

    return { alerts, connected };
}
