#!/bin/sh
set -e

FLINK_URL="${FLINK_URL:-http://flink-jobmanager:8081}"
JAR_PATH="${JAR_PATH:-/job/bus-tracking-job.jar}"
ENTRY_CLASS="${ENTRY_CLASS:-com.bustrack.BusTrackingJob}"
PARALLELISM="${FLINK_PARALLELISM:-2}"
TIMEOUT=180

echo "[submitter] Waiting for Flink JobManager at $FLINK_URL ..."
elapsed=0
until curl -sf "$FLINK_URL/overview" > /dev/null 2>&1; do
    sleep 3
    elapsed=$((elapsed + 3))
    if [ $elapsed -ge $TIMEOUT ]; then
        echo "[submitter] Timed out waiting for JobManager"
        exit 1
    fi
done
echo "[submitter] JobManager is up."

echo "[submitter] Waiting for at least one TaskManager to register ..."
elapsed=0
until [ "$(curl -sf "$FLINK_URL/taskmanagers" | grep -o '"id"' | wc -l)" -ge 1 ]; do
    sleep 3
    elapsed=$((elapsed + 3))
    if [ $elapsed -ge $TIMEOUT ]; then
        echo "[submitter] Timed out waiting for TaskManager"
        exit 1
    fi
done
echo "[submitter] TaskManager registered."

echo "[submitter] Uploading JAR ..."
UPLOAD_RESPONSE=$(curl -sf -X POST \
    -H "Expect:" \
    -F "jarfile=@$JAR_PATH" \
    "$FLINK_URL/jars/upload")

JAR_ID=$(echo "$UPLOAD_RESPONSE" | grep -o '"filename":"[^"]*"' | sed 's/"filename":"//;s/"//')
# extract just the basename that Flink uses as the jar id
JAR_ID=$(basename "$JAR_ID")
echo "[submitter] JAR uploaded: $JAR_ID"

echo "[submitter] Submitting job ..."
RUN_RESPONSE=$(curl -sf -X POST \
    -H "Content-Type: application/json" \
    -d "{\"entryClass\":\"$ENTRY_CLASS\",\"parallelism\":$PARALLELISM}" \
    "$FLINK_URL/jars/$JAR_ID/run")

JOB_ID=$(echo "$RUN_RESPONSE" | grep -o '"jobid":"[^"]*"' | sed 's/"jobid":"//;s/"//')
echo "[submitter] Job submitted: $JOB_ID"

echo "[submitter] Waiting for job to reach RUNNING state ..."
elapsed=0
until curl -sf "$FLINK_URL/jobs/$JOB_ID" | grep -q '"status":"RUNNING"'; do
    sleep 3
    elapsed=$((elapsed + 3))
    if [ $elapsed -ge $TIMEOUT ]; then
        echo "[submitter] Timed out waiting for job to start. Check Flink UI at $FLINK_URL"
        exit 1
    fi
done

echo "[submitter] Job $JOB_ID is RUNNING. Pipeline active."
