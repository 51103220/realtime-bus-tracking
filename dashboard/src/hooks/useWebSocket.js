import { useEffect, useRef, useState } from "react";

const WS_URL =
  window.location.hostname === "localhost"
    ? "ws://localhost:8000/ws/buses"
    : `ws://${window.location.hostname}:8000/ws/buses`;

const API_URL =
  window.location.hostname === "localhost"
    ? "http://localhost:8000"
    : `http://${window.location.hostname}:8000`;

export default function useWebSocket() {
  const [buses, setBuses]           = useState(new Map());
  const [connected, setConnected]   = useState(false);
  const wsRef                       = useRef(null);
  const reconnectTimer              = useRef(null);

  function connect() {
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      clearTimeout(reconnectTimer.current);
    };

    ws.onmessage = (evt) => {
      try {
        const data = JSON.parse(evt.data);
        if (data.type === "ping") return;
        if (!data.vehicle) return;

        setBuses((prev) => {
          const next = new Map(prev);
          next.set(data.vehicle, data);
          return next;
        });
      } catch {
        // ignore malformed messages
      }
    };

    ws.onclose = () => {
      setConnected(false);
      // reconnect after 3s
      reconnectTimer.current = setTimeout(connect, 3000);
    };

    ws.onerror = () => ws.close();
  }

  useEffect(() => {
    fetch(`${API_URL}/buses`)
      .then((res) => res.json())
      .then((items) => {
        setBuses(new Map(items.filter((bus) => bus.vehicle).map((bus) => [bus.vehicle, bus])));
      })
      .catch(() => {
        // WebSocket updates will still populate the map if the initial fetch fails.
      });

    connect();
    return () => {
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, []);

  return { buses, connected };
}
