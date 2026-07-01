import { useState } from 'react';
import { AlertFeed } from './components/AlertFeed';
import { EntitySearch } from './components/EntitySearch';
import { Header } from './components/Header';
import { ScoreChart } from './components/ScoreChart';

export default function App() {
  // The selected entity drives ScoreChart. It lives here because
  // EntitySearch (to set it) and ScoreChart (to use it) are siblings.
  const [entityInput, setEntityInput] = useState('');
  const [selectedEntity, setSelectedEntity] = useState<string | null>(null);

  return (
    <div style={{ fontFamily: 'system-ui, sans-serif', minHeight: '100vh', display: 'flex', flexDirection: 'column' }}>
      <Header />

      <div style={{ flex: 1, display: 'flex' }}>
        {/* Main content */}
        <main style={{ flex: 1, overflow: 'auto' }}>
          <EntitySearch
            value={entityInput}
            onChange={setEntityInput}
            onSubmit={() => setSelectedEntity(entityInput.trim() || null)}
          />
          <ScoreChart entityId={selectedEntity} />
        </main>

        {/* Sidebar */}
        <AlertFeed />
      </div>
    </div>
  );
}
