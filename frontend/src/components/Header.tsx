import { useCurrentModel } from '../hooks/useCurrentModel';

export function Header() {
    const models = useCurrentModel();

    return (
        <header style={{ borderBottom: '1px solid #e2e8f0', padding: '12px 24px', display: 'flex', alignItems: 'center', gap: 24 }}>
            <h1 style={{ margin: 0, fontSize: 18, fontWeight: 700 }}>NeuralSentinel</h1>
            <div style={{ display: 'flex', gap: 16 }}>
                {models.map(m => (
                    <span key={m.model_name} style={{ fontSize: 13, color: '#64748b' }}>
                        <strong>{m.model_name.replace('neural-sentinel-', '')}</strong>
                        {' '}v{m.version}
                        {' '}<span style={{ color: '#22c55e' }}>● Production</span>
                    </span>
                ))}
            </div>
        </header>
    );
}
