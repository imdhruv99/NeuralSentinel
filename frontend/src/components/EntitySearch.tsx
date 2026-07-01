interface Props {
    value: string;
    onChange: (v: string) => void;
    onSubmit: () => void;
}

export function EntitySearch({
    value,
    onChange,
    onSubmit,
}: Props) {
    return (
        <div style={{ padding: '16px 24px' }}>
            <label
                style={{
                    display: 'block',
                    marginBottom: 6,
                    fontSize: 13,
                    fontWeight: 600,
                }}
            >
                Entity ID
            </label>

            <input
                type="text"
                value={value}
                onChange={(e) => onChange(e.target.value)}
                onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                        onSubmit();
                    }
                }}
                placeholder="NAB/realAdExchange/exchange-2_cpc_results"
                style={{
                    width: '100%',
                    maxWidth: 500,
                    padding: '8px 12px',
                    border: '1px solid #cbd5e1',
                    borderRadius: 6,
                    fontSize: 13,
                }}
            />

            <p
                style={{
                    margin: '4px 0 0',
                    fontSize: 12,
                    color: '#94a3b8',
                }}
            >
                Full entity ID as stored in the scores table. Scores load on submit
                (press Enter).
            </p>
        </div>
    );
}
