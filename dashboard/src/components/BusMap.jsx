import { useEffect, useRef } from "react";
import { MapContainer, TileLayer, Marker, Tooltip } from "react-leaflet";
import L from "leaflet";

// HCMC center
const HCMC = [10.7769, 106.7009];

// deterministic HSL color from route number string
function routeColor(routeNo) {
  if (!routeNo || routeNo === "UNKNOWN") return "#888";
  let hash = 0;
  for (let i = 0; i < routeNo.length; i++) {
    hash = routeNo.charCodeAt(i) + ((hash << 5) - hash);
  }
  const hue = Math.abs(hash) % 360;
  return `hsl(${hue}, 80%, 55%)`;
}

function makeBusIcon(color) {
  return L.divIcon({
    className: "",
    html: `<div style="
      width:12px;height:12px;
      border-radius:50%;
      background:${color};
      border:2px solid rgba(255,255,255,0.7);
      box-shadow:0 0 4px ${color};
    "></div>`,
    iconSize: [12, 12],
    iconAnchor: [6, 6],
  });
}

export default function BusMap({ buses, onSelectBus }) {
  const markersRef = useRef(new Map());

  return (
    <MapContainer
      center={HCMC}
      zoom={12}
      style={{ height: "100%", width: "100%", background: "#1a1a2e" }}
    >
      <TileLayer
        url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        attribution='&copy; <a href="https://carto.com/">CARTO</a>'
        maxZoom={19}
      />
      {Array.from(buses.values()).map((bus) => {
        const lat = parseFloat(bus.lat);
        const lon = parseFloat(bus.lon);
        if (isNaN(lat) || isNaN(lon)) return null;

        const color = routeColor(bus.route_no);
        const icon  = makeBusIcon(color);

        return (
          <Marker
            key={bus.vehicle}
            position={[lat, lon]}
            icon={icon}
            eventHandlers={{ click: () => onSelectBus(bus) }}
          >
            <Tooltip>
              Route {bus.route_no || "?"} &mdash; {bus.vehicle?.slice(0, 8)}
            </Tooltip>
          </Marker>
        );
      })}
    </MapContainer>
  );
}
