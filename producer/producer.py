"""
Kafka GPS producer — replays HCMC bus dataset files to a Kafka topic.

Replay speed is controlled by REPLAY_SPEED env var:
  0    → max throughput (flood Kafka, best for demos)
  1.0  → real-time (respects original event timestamps)
  N    → Nx accelerated (e.g. 10 means 10x faster than real-time)
"""

import json
import os
import re
import time
import glob
import sys
from confluent_kafka import Producer, KafkaException

# ── Config ─────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC             = os.getenv("KAFKA_TOPIC", "bus-gps-events")
DATA_PATH         = os.getenv("DATA_PATH", "/data")
REPLAY_SPEED      = float(os.getenv("REPLAY_SPEED", "0"))

# ── Kafka producer config ──────────────────────────────────────────────────
producer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "linger.ms": 5,
    "batch.size": 65536,
    "compression.type": "lz4",
    "queue.buffering.max.messages": 500000,
    "queue.buffering.max.kbytes": 524288,
}


def delivery_report(err, msg):
    if err:
        print(f"[producer] Delivery failed: {err}", file=sys.stderr)


def find_files(data_path: str) -> list[str]:
    """Collect all sub_raw_*.json files, sorted by numeric suffix."""
    patterns = [
        os.path.join(data_path, "part1", "part1", "sub_raw_*.json"),
        os.path.join(data_path, "part2", "part2", "sub_raw_*.json"),
    ]
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))

    def sort_key(path):
        m = re.search(r"sub_raw_(\d+)\.json$", path)
        # prefix part index so part1 files sort before part2
        part = 1 if "part1" in path else 2
        return (part, int(m.group(1)) if m else 0)

    return sorted(files, key=sort_key)


def records_from_file(path: str) -> list[dict]:
    """Load JSON array, discard records missing vehicle or datetime."""
    with open(path, "r") as f:
        data = json.load(f)

    valid = []
    for item in data:
        payload = item.get("msgBusWayPoint", {})
        if payload.get("vehicle") and payload.get("datetime"):
            valid.append(payload)
    return valid


def run():
    files = find_files(DATA_PATH)
    if not files:
        print(f"[producer] No data files found under {DATA_PATH}", file=sys.stderr)
        sys.exit(1)

    print(f"[producer] Found {len(files)} files. REPLAY_SPEED={REPLAY_SPEED}")
    print(f"[producer] Sending to topic '{TOPIC}' on {BOOTSTRAP_SERVERS}")

    producer = Producer(producer_conf)

    total_sent  = 0
    prev_event_time: float | None = None
    prev_wall_time:  float | None = None

    for file_idx, file_path in enumerate(files, start=1):
        records = records_from_file(file_path)
        if not records:
            continue

        # sort records within each file by datetime for correct replay order
        records.sort(key=lambda r: r["datetime"])

        print(f"[producer] [{file_idx}/{len(files)}] {os.path.basename(file_path)} "
              f"— {len(records):,} records")

        for rec in records:
            # ── Replay speed timing ────────────────────────────────────────
            if REPLAY_SPEED > 0 and prev_event_time is not None:
                event_gap   = rec["datetime"] - prev_event_time
                elapsed     = time.monotonic() - prev_wall_time
                target_sleep = event_gap / REPLAY_SPEED - elapsed
                if target_sleep > 0:
                    time.sleep(target_sleep)

            value = json.dumps({
                "msgType": "MsgType_BusWayPoint",
                "msgBusWayPoint": rec
            }).encode()

            key = rec["vehicle"].encode()

            producer.produce(TOPIC, value=value, key=key, on_delivery=delivery_report)
            producer.poll(0)

            prev_event_time = rec["datetime"]
            prev_wall_time  = time.monotonic()
            total_sent += 1

            if total_sent % 50_000 == 0:
                producer.flush()
                print(f"[producer]   sent {total_sent:,} total records")

        producer.flush()

    producer.flush()
    print(f"[producer] Done. Total records sent: {total_sent:,}")


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("[producer] Interrupted.")
    except KafkaException as e:
        print(f"[producer] Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
