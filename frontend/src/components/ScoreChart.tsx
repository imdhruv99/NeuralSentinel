import {
    CartesianGrid, Line, LineChart,
    ReferenceDot, ResponsiveContainer,
    Tooltip, XAxis, YAxis,
} from 'recharts';
import { useEntitySeries } from '../hooks/useEntitySeries';

interface Props {
    entityId: string | null;
}

export function ScoreChart({ entityId }: Props) {
    const { data, loading, error } = useEntitySeries(entityId);

    if (!entityId) {
        return (
            <div style={{ padding: '24px', color: '#94a3b8', fontSize: 14 }}>
                Enter an entity ID above to view its anomaly score history.
            </div>
        );
    }

    if (loading) return <div style={{ padding: 24 }}>Loading...</div>;
    if (error) return <div style={{ padding: 24, color: '#ef4444' }}>Error: {error}</div>;
    if (!data.length) return <div style={{ padding: 24, color: '#94a3b8' }}>No scores found for this entity.</div>;

    // Recharts needs the data in ascending time order for correct rendering
    const sorted = [...data].reverse();
    const anomalies = sorted.filter(d => d.is_anomaly);

    return (
        <div style={{ padding: '0 24px 24px' }}>
            <h2 style={{ fontSize: 14, fontWeight: 600, marginBottom: 8, color: '#475569' }}>
                Anomaly score — {entityId}
            </h2>
            <ResponsiveContainer width="100%" height={260}>
                <LineChart data={sorted}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                    <XAxis
                        dataKey="window_end"
                        tickFormatter={v => new Date(v).toLocaleTimeString()}
                        tick={{ fontSize: 11 }}
                    />
                    <YAxis tick={{ fontSize: 11 }} width={50} />
                    <Tooltip
                        labelFormatter={v => new Date(v as string).toLocaleString()}
                        formatter={(v: number) => [v.toFixed(4), 'score']}
                    />
                    <Line
                        type="monotone"
                        dataKey="anomaly_score"
                        stroke="#3b82f6"
                        dot={false}
                        strokeWidth={1.5}
                        isAnimationActive={false}
                    />
                    {anomalies.map(d => (
                        <ReferenceDot
                            key={d.window_end}
                            x={d.window_end}
                            y={d.anomaly_score}
                            r={4}
                            fill="#ef4444"
                            stroke="none"
                        />
                    ))}
                </LineChart>
            </ResponsiveContainer>
            <p style={{ margin: '4px 0 0', fontSize: 11, color: '#94a3b8' }}>
                {data.length} windows · {anomalies.length} anomalies (red dots)
            </p>
        </div>
    );
}
