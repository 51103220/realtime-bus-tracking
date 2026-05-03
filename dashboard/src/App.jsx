import { useState } from "react";
import BusMap from "./components/BusMap";
import BusInfo from "./components/BusInfo";
import useWebSocket from "./hooks/useWebSocket";

export default function App() {
  const { buses, connected } = useWebSocket();
  const [selectedBus, setSelectedBus] = useState(null);

  return (
    <>
      <header className="app-header">
        <h1>HCMC Real-Time Bus Tracker</h1>
        <span className={`status-badge ${connected ? "connected" : "disconnected"}`}>
          {connected ? `● Live — ${buses.size} buses` : "○ Disconnected"}
        </span>
      </header>

      <div className="app-body">
        <div className="map-container">
          <BusMap
            buses={buses}
            onSelectBus={setSelectedBus}
          />
        </div>

        <aside className="sidebar">
          <BusInfo
            buses={buses}
            selectedBus={selectedBus}
            onClose={() => setSelectedBus(null)}
          />
        </aside>
      </div>
    </>
  );
}
