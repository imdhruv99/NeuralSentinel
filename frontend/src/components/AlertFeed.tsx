import { useAlertStream } from '../hooks/useAlertStream';

export function AlertFeed() {
    const { alerts, connected } = useAlertStream();

    return (
        <aside style={{ borderLeft: '1px solid #e2e8f0', width: 320, flexShrink: 0, display: 'flex', flexDirection: 'column' }}>
            <div style={{ padding: '12px 16px', borderBottom: '1px solid #e2e8f0', display: 'flex', alignItems: 'center', gap: 8 }}>
                <span style={{ fontWeight: 600, fontSize: 14 }}>Live Alerts</span>
                <span style={{ fontSize: 11, color: connected ? '#22c55e' : '#94a3b8' }}>
                    {connected ? '● live' : '○ connecting'}
                </span>
            </div>

            {alerts.length === 0 ? (
                <div style={{ padding: 16, color: '#94a3b8', fontSize: 13 }}>
                    Waiting for anomalies...
                </div>
            ) : (
                <ul style={{ margin: 0, padding: 0, listStyle: 'none', overflowY: 'auto', flex: 1 }}>
                    {alerts.map((a, i) => (
                        <li key={`${a.entity_id}-${a.scored_at}-${i}`}
                            style={{ padding: '10px 16px', borderBottom: '1px solid #f1f5f9', fontSize: 12 }}>
                            <div style={{ fontWeight: 600, marginBottom: 2 }}>{a.entity_id.split('/').pop()}</div>
                            <div style={{ color: '#64748b' }}>
                                score {a.anomaly_score.toFixed(4)} · {a.model_name.replace('neural-sentinel-', '')}
                            </div>
                            <div style={{ color: '#94a3b8', marginTop: 2 }}>
                                {new Date(a.scored_at).toLocaleTimeString()}
                            </div>
                        </li>
                    ))}
                </ul>
            )}
        </aside>
    );
}
