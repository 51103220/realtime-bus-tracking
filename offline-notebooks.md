# Offline Analysis Notebooks

The notebooks read data from MinIO and Redis. Run them **after the pipeline has been active for at least 10–15 minutes** to ensure enough data has accumulated.

## Install dependencies

```bash
pip install jupyter pandas numpy scikit-learn folium matplotlib seaborn boto3 redis
```

Or using a virtual environment (recommended):

```bash
python3 -m venv venv
source venv/bin/activate
pip install jupyter pandas numpy scikit-learn folium matplotlib seaborn boto3 redis
```

## Start Jupyter

```bash
cd big-data
jupyter notebook notebooks/
```

Run notebooks in order (01 → 05).

---

## Notebook guide

### `01_stop_detection.ipynb` — Bus Stop Recovery
- Loads raw GPS events from MinIO
- Runs state-machine stop detection per vehicle
- Applies DBSCAN to cluster recurring stop locations
- **Expected output:** A Folium map at `/tmp/stop_detection_map.html` showing recovered bus stop locations (red circles) over raw stop points (blue dots)
- **Requires:** ≥ 50 MinIO objects under `2025-*/` prefix (check MinIO console first)

### `02_travel_time.ipynb` — Route Travel Time Heatmap
- Loads `TripSegment` records from `trips/` in MinIO
- **Expected output:** A seaborn heatmap (route × hour) and a bar chart showing April 30 parade impact factor per route
- **Requires:** ≥ 200 trip segment objects in MinIO (visible under `trips/` in MinIO console)

### `03_congestion_heatmap.ipynb` — Congestion by Hour
- Loads raw GPS events from MinIO historical archive
- Bins to 0.005° grid, aggregates mean speed per cell per hour
- **Expected output:** Two Folium maps (`/tmp/congestion_hour08.html`, `/tmp/congestion_hour14.html`) showing congestion at hour 8 (morning rush) and hour 14 (off-peak)
- **Requires:** ≥ 100 MinIO objects under `2025-*/`

### `04_pca_analysis.ipynb` — Dimensionality Reduction
- Loads `VehicleFeatureVector` objects from `features/vehicle-hourly/` in MinIO
- Fits StandardScaler + PCA(12 components)
- **Expected output:** Scree plot, 2D scatter coloured by route, loadings heatmap
- **Requires:** ≥ 500 feature vector objects (one per vehicle per hour — accumulates after ~1 hour at `REPLAY_SPEED=0`)
- **Note:** If not enough vectors yet, lower `max_objects` in the first cell to what's available and re-run

### `05_anomaly_analysis.ipynb` — Anomaly Patterns
- Reads `anomalies:recent` list directly from Redis (no MinIO needed)
- **Expected output:** Bar charts for anomaly type distribution, per-route anomaly counts, speed distribution histogram
- **Requires:** Redis running with anomaly events present (`LRANGE anomalies:recent 0 -1` should return non-empty)
