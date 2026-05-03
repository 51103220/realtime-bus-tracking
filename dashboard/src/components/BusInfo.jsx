import { useState } from "react";
import "./BusInfo.css";

function fmt(val, unit = "") {
  if (val == null || val === "" || val === "undefined") return "—";
  return `${val}${unit}`;
}

function boolIcon(val) {
  if (val == null || val === "") return "—";
  return val === "1" || val === "true" ? "✓" : "✗";
}

function tsToTime(ts) {
  if (!ts) return "—";
  const d = new Date(parseInt(ts) * 1000);
  return isNaN(d) ? "—" : d.toLocaleTimeString();
}

export default function BusInfo({ buses, selectedBus, onClose }) {
  const [showAll, setShowAll] = useState(false);

  // per-route counts
  const routeCounts = new Map();
  for (const bus of buses.values()) {
    const r = bus.route_no || "UNKNOWN";
    routeCounts.set(r, (routeCounts.get(r) || 0) + 1);
  }
  const sortedRoutes = Array.from(routeCounts.entries()).sort((a, b) => b[1] - a[1]);
  const displayRoutes = showAll ? sortedRoutes : sortedRoutes.slice(0, 8);

  return (
    <div className="sidebar-inner">
      {/* ── Summary panel ──────────────────────────────────────────── */}
      <div className="panel">
        <div className="panel-title">Fleet Summary</div>
        <div className="stat-row">
          <span>Active buses</span>
          <span className="stat-val">{buses.size}</span>
        </div>
        <div className="stat-row">
          <span>Routes</span>
          <span className="stat-val">{routeCounts.size}</span>
        </div>
        <div className="routes-list">
          {displayRoutes.map(([route, count]) => (
            <div key={route} className="route-row">
              <span className="route-badge">{route}</span>
              <span className="route-count">{count}</span>
            </div>
          ))}
          {sortedRoutes.length > 8 && (
            <button className="show-more" onClick={() => setShowAll((s) => !s)}>
              {showAll ? "Show less" : `+${sortedRoutes.length - 8} more`}
            </button>
          )}
        </div>
      </div>

      {/* ── Selected bus detail ─────────────────────────────────────── */}
      {selectedBus && (
        <div className="panel detail-panel">
          <div className="panel-header">
            <div className="panel-title">Bus Detail</div>
            <button className="close-btn" onClick={onClose}>✕</button>
          </div>

          <div className="detail-id" title={selectedBus.vehicle}>
            {selectedBus.vehicle?.slice(0, 16)}…
          </div>

          <div className="detail-grid">
            <span className="label">Route</span>
            <span>{fmt(selectedBus.route_no)}</span>

            <span className="label">Speed</span>
            <span>{fmt(selectedBus.speed, " km/h")}</span>

            <span className="label">Lat / Lon</span>
            <span>
              {fmt(selectedBus.lat)} / {fmt(selectedBus.lon)}
            </span>

            <span className="label">Heading</span>
            <span>{fmt(selectedBus.heading, "°")}</span>

            <span className="label">Last seen</span>
            <span>{tsToTime(selectedBus.datetime)}</span>

            <span className="label">Ignition</span>
            <span>{boolIcon(selectedBus.ignition)}</span>

            <span className="label">Air con</span>
            <span>{boolIcon(selectedBus.aircon)}</span>

            <span className="label">Door front</span>
            <span>{boolIcon(selectedBus.door_up)}</span>

            <span className="label">Door rear</span>
            <span>{boolIcon(selectedBus.door_down)}</span>
          </div>
        </div>
      )}

      <div className="hint">Click a bus marker to inspect it</div>
    </div>
  );
}
