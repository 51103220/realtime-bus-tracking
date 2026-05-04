# Ho Chi Minh City Real-Time Bus Tracking

**Dataset:** 32 GB, 517 JSON files, ~109 M GPS records, 440 buses, 31 routes, 50 days (March 20 – May 10 2025)  
**Stack:** Kafka → Flink (Java 17) → Redis + MinIO → FastAPI + React/Leaflet → Prometheus + Grafana  
**Deployment:** Single `docker-compose up --build` — 17 Docker services, no cloud required

## Quick Start
### Required ports

The following host ports must be free before running `./run.sh`. Check for conflicts with `lsof -iTCP:<port> -sTCP:LISTEN`.

| Port | Service | Used for |
|------|---------|----------|
| **2182** | Zookeeper | Kafka coordination (host-side only; internal port is 2181) |
| **29092** | Kafka | External broker access from host tools |
| **8888** | Kafka UI | Topic browser |
| **6379** | Redis | Current bus state, Pub/Sub, anomaly list |
| **9000** | MinIO | S3-compatible API (SDK access) |
| **9001** | MinIO | Web console |
| **8081** | Flink JobManager | Web UI + REST API |
| **9249** | Flink JobManager | Prometheus metrics scrape |
| **9250** | Flink TaskManager | Prometheus metrics scrape |
| **8000** | FastAPI | REST endpoints + WebSocket |
| **3002** | Dashboard | React + Leaflet live map |
| **9090** | Prometheus | Metrics storage + query |
| **3001** | Grafana | Monitoring dashboard |

```bash

./run.sh          # checks prerequisites, builds JAR, starts all 17 services, waits until ready
```

See `./run.sh --help` for additional modes:

```
./run.sh            — full setup: check prereqs, build JAR, start everything
./run.sh --stop     — stop all containers (keep data volumes)
./run.sh --clean    — stop all containers AND delete all data volumes
./run.sh --rebuild  — rebuild JAR + Docker images, then (re)start
./run.sh --logs     — tail logs from the key services
./run.sh --status   — show which services are up
```

**Environment Config:** open `.env` and set `DATASET_PATH` to where the bus GPS dataset lives on your machine:

```bash
# .env
DATASET_PATH=/path/to/BigData-Bus-DataSet   # must contain sub_raw_*.json files
```

---

## System Architecture

![Pipeline Architecture](architecture.png)

```
Bus GPS Files (32 GB)
       │  producer.py — replay at REPLAY_SPEED=0/1.0/N
       ▼
  Apache Kafka  (topic: bus-gps-events, 3 partitions)
       │
       ▼
  Apache Flink  (streaming pipeline — 5 stages)
  ┌────────────────────────────────────────────────────────┐
  │ 1. CoordinateValidatorProcess  (data quality gate)     │
  │ 2. BloomFilterDeduplicator     (probabilistic dedup)   │
  │ 3. AnomalyDetector             (speed + GPS jump)      │
  │ 4a. RedisSink                  (current state)         │
  │ 4b. SlidingEventTimeWindows    (rolling speed)         │
  │ 4c. EventTimeSessionWindows    (trip segmentation)     │
  │ 4d. TumblingEventTimeWindows   (historical archive)    │
  │ 4e. TumblingEventTimeWindows 1h (feature vectors)      │
  └────────────────────────────────────────────────────────┘
       │                         │
       ▼                         ▼
    Redis                      MinIO (local S3)
  current state              historical data +
  Pub/Sub relay              feature vectors +
  anomaly list               trip segments
       │
       ▼
  FastAPI (REST + WebSocket)
       │
       ▼
  React + Leaflet dashboard
  (440+ live bus markers on HCMC map)

  Prometheus + Grafana — 8-panel monitoring dashboard
```

---

## Big Data Techniques Applied

### 1. Data Quality Enhancement

#### 1.1 GPS Coordinate Validation
**Class:** `CoordinateValidatorProcess.java`  
**Type:** `ProcessFunction` with side output  
**Theory:** Data quality gate — the first line of defence in any ETL pipeline. All incoming events are checked against:
- HCMC bounding box: longitude [106.4°, 107.1°], latitude [10.3°, 11.2°]
- Speed plausibility: rejects `speed > 120 km/h` (hardware fault) and `speed < 0`
- Null coordinate guard: events without `x`/`y` are routed to side output

Invalid events are emitted on a `INVALID_TAG` side output — they are never silently dropped. This separation is the "filter and route" pattern: bad data is observable, auditable, and recoverable.

**Key detail — side outputs:** Flink side outputs allow a single operator to produce multiple typed output streams. The main output carries valid events; the side output carries `InvalidBusEvent(event, reason, detectedAt)` objects. This avoids data loss while keeping the main pipeline clean.

#### 1.2 Bloom Filter Deduplication
**Class:** `BloomFilterDeduplicator.java`  
**Type:** `KeyedProcessFunction` (keyed by vehicle)  
**Theory:** A Bloom filter is a probabilistic data structure that answers membership queries with:
- **No false negatives** — if it says "not seen", it has definitely not been seen
- **Bounded false positives** — if it says "seen", it is probably (not certainly) a duplicate

Configured at 1% false positive rate for 10,000 expected insertions per vehicle:

| Method | Memory per vehicle partition | Accuracy |
|---|---|---|
| `HashMap<String, Long>` (previous approach) | ~5–8 MB | 100% exact |
| Bloom filter (1% FPP, 10K inserts) | ~958 KB | 99% exact |
| **Savings** | **~8×** | 1% false positive rate |

The filter is serialised to `ValueState<byte[]>` (Guava `BloomFilter.writeTo/readFrom`) so it survives Flink checkpoints and TaskManager restarts. Route enrichment (joining vehicle hash → route number via `vehicle_route_mapping.csv`) is performed here, loading the CSV once per TaskManager in `open()`.

**Dataset finding:** Confirmed 0.6% duplicate rate in sample data. Bloom filter drops these efficiently with negligible false positive impact on 440 vehicles × 50 days.

#### 1.3 Dedup Metrics Reporting
Every 5,000 events, `BloomFilterDeduplicator` emits a `DedupMetrics` record on `METRICS_TAG` side output containing: `totalSeen`, `duplicatesDropped`, `duplicateRate`, `bloomFilterSizeBytes`. This makes deduplication performance observable in real time.

---

### 2. Stream Processing — Windowing

Three distinct window types are used simultaneously, demonstrating the full taxonomy:

#### 2.1 Tumbling Event-Time Windows (Historical Archive)
**Where:** `keyBy(routeNo)` → `TumblingEventTimeWindows(60s)` → `MinIOSink`  
**Theory:** Non-overlapping fixed-duration windows. Each event belongs to exactly one window. Memory cost = O(1) per window. Used for partitioning the raw event stream into manageable MinIO objects: `{date}/{HH-mm}/{routeNo}_{windowStart}.json`.

#### 2.2 Sliding Event-Time Windows (Rolling Route Speed)
**Where:** `keyBy(routeNo)` → `SlidingEventTimeWindows(5min, 30s)` → `RouteSpeedRedisSink`  
**Theory:** Overlapping windows. Each event belongs to `window_size / slide_interval = 10` windows simultaneously. This is the memory cost of smoothness. The `AggregateFunction` + `ProcessWindowFunction` combination uses O(1) accumulator memory (not O(events)), so the 10× overlap does not mean 10× memory.

**Contrast with tumbling:** A tumbling 60s window updates the route speed once per minute with a step function. A sliding 5min/30s window updates every 30 seconds with a smooth rolling average — much better for real-time dashboard display.

**Route speed written to Redis:** `route:speed:{routeNo}` → `{ avgSpeed, minSpeed, maxSpeed, vehicleCount, windowEndMs }` with 120s TTL.

#### 2.3 Session Windows (Trip Segmentation)
**Where:** `keyBy(vehicle)` → `EventTimeSessionWindows(gap=5min)` → `TripSegmentMinIOSink`  
**Theory:** Session windows are the only Flink window type driven by **data inactivity** rather than clock time. A session window closes when no event arrives for the configured gap. This models real-world bus trips: a bus idles at a terminal for ≥5 minutes between outbound and inbound runs.

**Why not tumbling?** A fixed-duration window would split a single trip across two windows if the trip duration is not a multiple of the window size. Session windows respect the natural semantic boundary of a trip.

**Output — `TripSegment`:**
- `tripStart`, `tripEnd`, `durationSeconds`
- `totalDistanceKm` (haversine chain)
- `avgSpeedKmh`, `stopCount` (events with speed < 2 km/h)
- Start/end coordinates for route visualisation

---

### 3. Dimensionality Reduction — PCA

#### 3.1 Feature Engineering (Streaming)
**Class:** `VehicleFeatureExtractor.java`  
**Type:** `ProcessWindowFunction` over `TumblingEventTimeWindows(1h)`, keyed by vehicle  
**Where:** In-stream feature extraction; output written to `MinIO features/vehicle-hourly/`

Each vehicle produces one **12-dimensional feature vector per hour**:

| # | Feature | What it captures |
|---|---------|-----------------|
| 0 | `eventCount` | Data volume (GPS reporting frequency) |
| 1 | `avgSpeed` | Mean operating speed |
| 2 | `maxSpeed` | Peak speed (route type indicator) |
| 3 | `stdSpeed` | Speed variability (traffic turbulence) |
| 4 | `idleFraction` | Fraction at bus stops (stop density) |
| 5 | `totalDistanceKm` | Route length proxy |
| 6 | `avgSamplingIntervalS` | GPS device reporting rate |
| 7 | `samplingIrregularity` | Data quality indicator |
| 8 | `hourSin` | Cyclic hour encoding — sin(2πh/24) |
| 9 | `hourCos` | Cyclic hour encoding — cos(2πh/24) |
| 10 | `headingChanges` | Turn frequency (route shape complexity) |
| 11 | `ignitionOnFraction` | Operational intensity |

**Why cyclic hour encoding?** Raw `hourOfDay` as an integer treats hour 23 and hour 0 as maximally different (distance = 23). The `(sin, cos)` encoding places them adjacent in 2D space — the correct topology for time-of-day distances used in PCA and clustering.

#### 3.2 PCA (Offline — Notebook 4)
**Notebook:** `notebooks/04_pca_analysis.ipynb`  
**Libraries:** `sklearn.decomposition.PCA`, `sklearn.preprocessing.StandardScaler`

**Pipeline:**
1. Load `VehicleFeatureVector` JSON objects from MinIO `features/vehicle-hourly/`
2. Build matrix X of shape `(N_vehicle_hours × 12)`
3. `StandardScaler` — zero-mean, unit-variance per feature
4. `PCA(n_components=12).fit_transform(X_scaled)`
5. Inspect `explained_variance_ratio_` (scree plot)
6. Scatter plot PC1 vs PC2, coloured by `routeNo`
7. Loadings heatmap — which features dominate which components

**Expected result:** PC1 ≈ speed/activity axis (loadings dominated by `avgSpeed`, `maxSpeed`, `totalDistanceKm`). PC2 ≈ temporal/regularity axis (`samplingIrregularity`, `idleFraction`). Vehicles on the same route should cluster, validating that route assignment is the dominant source of behavioural variance. April 30 parade hours should appear as outliers far from the typical distribution.

---

### 4. Anomaly Detection (Streaming)

**Class:** `AnomalyDetector.java`  
**Type:** `KeyedProcessFunction` (keyed by vehicle) with `ANOMALY_TAG` side output  
**Written to:** Redis `anomalies:recent` list (capped at 500), Pub/Sub channel `bus-anomalies`

Two threshold-based anomaly types:

#### 4.1 Speed Excess
Threshold: `speed > 80 km/h`  
Justification: HCMC urban speed limit is 60 km/h. The 80 km/h threshold adds a 20 km/h margin for GPS speed smoothing error while catching genuine runaways. Distinct from the hardware-fault threshold in `CoordinateValidator` (120 km/h) — this catches plausible-but-suspicious speeds.

#### 4.2 GPS Jump (Teleportation Detection)
Threshold: `distance > 500m AND time_delta ≤ 30s` (implied speed ≥ 60 km/h sustained)  
Uses **haversine distance formula** for geodesic accuracy on the Earth's surface.  
Catches: GPS unit resets, coordinate transmission errors, device cold-start artefacts.

**Key design:** Anomalous events are **forwarded** on the main output — they are not dropped. The anomaly is emitted as metadata on the side output. This preserves GPS record completeness while making the anomaly observable.

**Offline analysis — Notebook 5:** `notebooks/05_anomaly_analysis.ipynb` reads the Redis anomaly list and produces: anomaly type distribution, anomalies per route, temporal clustering, per-vehicle anomaly counts (GPS hardware quality indicator).

---

### 5. Spatio-Temporal Offline Analysis

#### 5.1 Stop Detection — Notebook 1
**Algorithm:** Two-threshold state machine (MOVING / STOPPED) per vehicle, then DBSCAN spatial clustering  
**Hysteresis:** STOPPED when `speed < 2 km/h` for ≥60s; MOVING resumes only when `speed > 5 km/h`  
**Clustering:** DBSCAN with `eps = 0.0005°` (≈55m), `min_samples = 10`  
**Output:** Recovered bus stop map overlaid on HCMC — validated against known stop locations

#### 5.2 Travel Time Estimation — Notebook 2
**Algorithm:** Load `TripSegment` records from MinIO; group by `routeNo × hourOfDay`; compute median, P25, P75 trip duration  
**Visualisation:** Heatmap (route × hour, colour = median duration in minutes)  
**Key finding:** April 30 military parade impact — compare `durationMin` on 2025-04-30 vs. 49-day baseline for same routes

#### 5.3 Congestion Heatmap — Notebook 3
**Algorithm:** Spatial binning — snap GPS coordinates to 0.005° grid (~500m cells); aggregate mean speed per cell per hour  
**Visualisation:** Folium `HeatMap` layer (inverted speed = congestion intensity)  
**Key comparison:** Hour 8 (morning rush) vs. hour 14 (off-peak) vs. April 30 parade corridor

---

## Streaming vs. Batch Division

This project demonstrates the **Lambda Architecture** split:

| Layer | Technology | Characteristics |
|---|---|---|
| **Speed layer** (streaming) | Flink | Per-event, stateful, low-latency, in-order constraints |
| **Batch layer** (offline) | Jupyter + MinIO | Global statistics, model fitting, spatial aggregation |

**Streaming is used when:**
- Computation must be low-latency (current bus state, live anomaly alerts)
- State is per-key and must persist across events (dedup, anomaly detection)
- The output drives a real-time visualisation

**Batch is used when:**
- The algorithm requires the full dataset (PCA needs all vectors to fit the model)
- Spatial aggregation benefits from global context (DBSCAN stop clustering needs all stop events)
- Retrospective analysis (travel time impact of April 30 is only meaningful after the fact)

---

## Dataset-Specific Big Data Challenges Addressed

| Challenge | Source | Technique Applied |
|---|---|---|
| 0.6% duplicate records | GPS device transmission retries | Bloom Filter Deduplication |
| Out-of-order events (network delay) | GPS message routing | Event-time watermarks (30s tolerance) |
| Missing optional fields (speed: 44%, heading: 7%) | Intermittent GPS signal | Null-safe field access in all operators |
| Irregular sampling (5s–60s gaps) | Variable GPS reporting rate | `samplingIrregularity` feature + session window gap tolerance |
| GPS coordinate errors | Device cold-start / reset | Bounding box validation + GPS jump detection |
| April 30 parade disruption (structural break) | Military event, road closures | Modelled explicitly in travel time analysis |

---

## Project File Structure

```
big-data/
├── docker-compose.yml          17-service stack
├── .env                        all tunables (REPLAY_SPEED, etc.)
│
├── flink-job/
│   ├── pom.xml                 Maven fat JAR (Kafka, Jedis, AWS SDK, Guava)
│   └── src/main/java/com/bustrack/
│       ├── BusTrackingJob.java           main — full pipeline wiring
│       ├── deserialization/
│       │   └── BusEventDeserializer.java
│       ├── model/              BusEvent, RouteInfo, InvalidBusEvent,
│       │                       AnomalyEvent, TripSegment, RouteSpeedSnapshot,
│       │                       VehicleFeatureVector, DedupMetrics
│       ├── functions/
│       │   ├── CoordinateValidatorProcess.java   data quality gate
│       │   ├── CoordinateValidator.java          validation logic (static)
│       │   ├── BloomFilterDeduplicator.java      probabilistic dedup + enrichment
│       │   ├── BusDeduplicator.java              original exact dedup (kept for reference)
│       │   ├── AnomalyDetector.java              speed + GPS jump detection
│       │   ├── TripSegmentFunction.java          session window → TripSegment
│       │   ├── SpeedAggregator.java              AggregateFunction (O(1) memory)
│       │   ├── RouteSpeedWindowFunction.java     adds window metadata
│       │   └── VehicleFeatureExtractor.java      12-dim feature vector per vehicle-hour
│       └── sink/
│           ├── RedisSink.java                    HSET + EXPIRE + ZADD + PUBLISH
│           ├── RouteSpeedRedisSink.java           route:speed:{routeNo}
│           ├── AnomalyRedisSink.java              anomalies:recent list
│           ├── MinIOSink.java                     tumbling window → S3 PUT
│           ├── TripSegmentMinIOSink.java           trips/{routeNo}/...
│           └── FeatureVectorMinIOSink.java         features/vehicle-hourly/...
│
├── producer/producer.py        replays 517 files; REPLAY_SPEED configurable
├── api/main.py                 FastAPI REST + WebSocket + Redis Pub/Sub
├── dashboard/                  React + Leaflet; 440+ live markers
├── monitoring/                 Prometheus + Grafana (8-panel dashboard)
│
└── notebooks/
    ├── 01_stop_detection.ipynb       state machine + DBSCAN
    ├── 02_travel_time.ipynb          trip segments + heatmap + April 30 analysis
    ├── 03_congestion_heatmap.ipynb   spatial binning + Folium HeatMap
    ├── 04_pca_analysis.ipynb         12-dim PCA, scree plot, loadings heatmap
    └── 05_anomaly_analysis.ipynb     anomaly distribution + temporal clustering
```

---

## Demo Walkthrough

---

### 1. Live Bus Map — `http://localhost:3002`

**Description:**  
Within 5–10 seconds of the producer starting, coloured dots appear across the city — one dot per active bus. 

**Try:**
- Watch dots appear and shift position as new GPS events arrive via WebSocket
- Click any dot → sidebar shows: route number, current speed (km/h), ignition / aircon / door status, last seen timestamp
- The top of the page shows: `● Live — 440 buses` (or the current count)
- Sidebar fleet summary shows active bus count and a per-route breakdown

**Processing:**  
Real-time event streaming from Kafka → Flink → Redis Pub/Sub → WebSocket → React. The map updates without any page refresh — every GPS event pushed by Flink immediately updates the marker.

---

### 2. Flink Job Graph — `http://localhost:8081`

**Description:**  
The Flink Web UI. Click **"Running Jobs"** → click the job named **"Bus GPS Tracking Pipeline"**.

**Observe::**
- **Job graph** — shows all operators connected in a DAG:
  ```
  Kafka GPS Source
       → GPS Coordinate Validation
       → Bloom Filter Dedup + Route Enrichment
       → Anomaly Detection
       → [Redis Current State]
       → [Rolling Route Speed → Route Speed Redis Sink]
       → [Trip Segmentation → Trip Segment MinIO Sink]
       → [MinIO Historical Archive]
       → [Vehicle Feature Extraction → Feature Vector MinIO Sink]
  ```
- Click any operator box → **Subtask metrics**: records in/out per second
- Click **"Checkpoints"** tab → shows checkpoint duration (~30s interval), size, alignment lag
- The **"Watermarks"** metric on the Kafka source shows event-time progress

**Processing:**  
Stateful distributed stream processing. The parallelism=2 setting means each operator runs in 2 parallel instances. Point out the checkpoint mechanism — this is what enables fault tolerance and exactly-once dedup state recovery.

---

### 3. Kafka UI — `http://localhost:8888`

**Description:**  
A web interface for inspecting the Kafka cluster.

**Observe::**
- Click **"Topics"** → `bus-gps-events` → **"Messages"** tab
  - Messages appearing in real time, partitioned across 3 partitions by vehicle hash
  - Each message key = vehicle hash (guarantees ordering per vehicle)
  - Message value = JSON GPS event with all fields
- Click **"Consumer Groups"** → `bus-tracking-flink`
  - **Lag** column should show near-zero — Flink is keeping up with the producer
  - If `REPLAY_SPEED=0`, lag may briefly spike before stabilising

**Processing:**  
Partitioned message distribution. Because the producer uses `vehicle_hash` as the Kafka key, all events from one bus always go to the same partition, and Flink's `keyBy(vehicle)` state always lives in the same task slot. This is the foundation of stateful per-vehicle processing.

---

### 4. MinIO Console — `http://localhost:9001`

**Credentials:** `minioadmin` / `minioadmin`

**Description:**  
The MinIO object browser. Click **"Object Browser"** → **"bus-history"** bucket.

**Observe::**
- After ~60–90 seconds: folders `2025-03-22/`, `2025-03-23/`, etc. appear (the dataset's event dates, not today's date — this is event-time)
- Inside each date: `HH-mm/` subfolders → JSON files like `163V_1742636580000.json`
- Folder `trips/` → one JSON file per completed bus trip (from session windows)
- Folder `features/vehicle-hourly/` → one JSON file per vehicle per hour (PCA input)

**Click any JSON file and download it.** You'll see either:
- An array of GPS events (from the tumbling window historical archive)
- A `TripSegment` object with `tripStart`, `durationSeconds`, `totalDistanceKm`, `stopCount`
- A `VehicleFeatureVector` object with all 12 features

**Processing:**  
Dual-layer storage (Lambda Architecture): Redis holds the real-time "speed layer"; MinIO holds the persistent "batch layer" for retrospective analysis. Three different data structures (raw events, trip segments, feature vectors) are written by three different Flink operators simultaneously.

---

### 5. Grafana Dashboard — `http://localhost:3001`

**Credentials:** `admin` / `admin` (prompted on first login, skip the change-password screen)

**Description:**  
Navigate to **Dashboards** → **"Bus GPS Pipeline"** (auto-provisioned).

**8 panels to observe:**

| Panel | What to point out |
|---|---|
| **Flink Records In/sec** | Throughput of the Kafka source; spikes when a new file is loaded |
| **Flink Records Out/sec** | Throughput after dedup; ~0.6% lower than records in |
| **Kafka Consumer Lag** | Should stay near 0; proves Flink keeps pace with producer |
| **Active Buses** | Redis key count; reaches ~440 within the first minute |
| **Redis Commands/sec** | Shows HSET + EXPIRE + PUBLISH volume per second |
| **Flink Checkpoint Duration** | Should be under 1000ms; proves checkpoint is healthy |
| **Kafka Messages In/sec** | Per-partition throughput; should be roughly equal across 3 partitions |
| **Redis Memory Used (MB)** | Grows as more bus states are stored; plateaus when TTL expires old entries |

**Processing:**  
Observability. A production big data system is only trustworthy if its internals are visible. Point out that all three layers (Kafka, Flink, Redis) are simultaneously monitored in one dashboard.

---

### 6. Redis CLI — Inspect Live State

Open a new terminal:

```bash
# Connect to the Redis container
docker exec -it big-data-redis-1 redis-cli

# How many buses are currently tracked?
ZCARD active-buses

# List all bus keys
SCAN 0 MATCH "bus:*" COUNT 20

# Inspect one bus's current state (replace <hash> with any vehicle hash from above)
HGETALL bus:<hash>
# Returns: vehicle, datetime, lat, lon, speed, route_no, ignition, aircon, ...

# Show last 5 detected anomalies
LRANGE anomalies:recent 0 4

# Show rolling speed for a route (replace <routeNo> e.g. 163V)
HGETALL route:speed:<routeNo>
# Returns: avgSpeed, minSpeed, maxSpeed, vehicleCount, windowEndMs
```

**Processing:**  
The Redis data model — three different structures used simultaneously:
- `HSET bus:{vehicle}` — hash (structured, field-level access)
- `ZADD active-buses` — sorted set (score = timestamp, enables time-based queries)
- `LPUSH anomalies:recent` — list (capped log of recent events)
- `HSET route:speed:{route}` — hash per route (sliding window aggregates)

---

### 7. Replay Speed Demo

While the pipeline is running, demonstrate the configurable replay speed:

```bash
# 1. Stop the producer
docker-compose stop producer

# 2. Switch to real-time replay (edit .env: REPLAY_SPEED=1.0)
# Then restart:
docker-compose start producer

# 3. Observe: bus markers on the dashboard update much more slowly now
#    (one GPS event every ~15-20 seconds per bus, matching real cadence)

# 4. Switch back to max speed:
docker-compose stop producer
# Edit .env: REPLAY_SPEED=0
docker-compose start producer
```

**Processing:**  
The producer's event-time replay faithfully simulates both real-time and accelerated conditions. The Flink pipeline behaves identically in both modes — this is the value of event-time processing with watermarks over processing-time.

---

### 8. Stale Bus Detection Demo

```bash
# Stop the producer — no more GPS events will arrive
docker-compose stop producer

# Watch the dashboard: within 60 minutes (bus state TTL), bus markers disappear
# For a faster demo, temporarily set BUS_STATE_TTL_SECONDS=30 in .env and restart:
# docker-compose restart api

# Restart the producer — buses reappear within seconds
docker-compose start producer
```


---

## Offline Analysis Notebooks

Run after the pipeline has been active for at least 10–15 minutes.  
See **[offline-notebooks.md](offline-notebooks.md)** for setup instructions and a per-notebook guide.

---


## Key Numbers

| Metric | Value |
|---|---|
| Dataset size (compressed on disk) | 32 GB |
| Total GPS records | ~109 million |
| Duplicate rate (measured) | 0.6% |
| Unique vehicles | 440 |
| Routes | 31 |
| Collection period | 50 days |
| Bloom filter memory saving | ~8× vs. HashMap |
| Bloom filter false positive rate | 1% |
| Flink operators | 9 |
| Flink side outputs | 3 (invalid events, dedup metrics, anomalies) |
| Docker services | 17 |
| Flink JAR size | 26 MB |
