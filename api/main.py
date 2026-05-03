import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import Any

import boto3
from botocore.client import Config
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as aioredis

# ── Config ─────────────────────────────────────────────────────────────────
REDIS_HOST    = os.getenv("REDIS_HOST",    "redis")
REDIS_PORT    = int(os.getenv("REDIS_PORT", "6379"))
MINIO_ENDPOINT= os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_KEY     = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET  = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
MINIO_BUCKET  = os.getenv("MINIO_BUCKET",      "bus-history")

# ── Global state ───────────────────────────────────────────────────────────
redis_pool: aioredis.Redis | None = None
ws_clients: set[WebSocket] = set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_pool
    redis_pool = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    asyncio.create_task(pubsub_relay())
    yield
    await redis_pool.aclose()


app = FastAPI(title="Bus Tracking API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://dashboard"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Background task: Redis Pub/Sub → WebSocket broadcast ──────────────────
async def pubsub_relay():
    """Subscribe to Redis bus-updates channel and fan-out to all WS clients."""
    pubsub = redis_pool.pubsub()
    await pubsub.subscribe("bus-updates")
    async for message in pubsub.listen():
        if message["type"] != "message":
            continue
        if not ws_clients:
            continue
        dead = set()
        for ws in list(ws_clients):
            try:
                await ws.send_text(message["data"])
            except Exception:
                dead.add(ws)
        ws_clients -= dead


# ── REST endpoints ─────────────────────────────────────────────────────────
@app.get("/buses")
async def get_all_buses() -> list[dict[str, Any]]:
    """Return current state of all active buses."""
    result = []
    cursor = 0
    while True:
        cursor, keys = await redis_pool.scan(cursor, match="bus:*", count=200)
        for key in keys:
            data = await redis_pool.hgetall(key)
            if data:
                result.append(data)
        if cursor == 0:
            break
    return result


@app.get("/bus/{vehicle_id}")
async def get_bus(vehicle_id: str) -> dict[str, Any]:
    """Return current state of a single bus."""
    data = await redis_pool.hgetall(f"bus:{vehicle_id}")
    return data or {}


@app.get("/bus/{vehicle_id}/history")
async def get_bus_history(vehicle_id: str, date: str | None = None) -> list[str]:
    """Return list of MinIO object keys for this vehicle's route history."""
    s3 = _s3_client()
    prefix = date if date else ""
    try:
        resp = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix, MaxKeys=200)
        return [obj["Key"] for obj in resp.get("Contents", [])]
    except Exception as e:
        return [f"error: {e}"]


@app.get("/routes")
async def get_routes() -> list[dict[str, str]]:
    """Return distinct routes from the active-buses sorted set."""
    vehicles = await redis_pool.zrange("active-buses", 0, -1)
    routes: dict[str, str] = {}
    for v in vehicles:
        data = await redis_pool.hgetall(f"bus:{v}")
        rno = data.get("route_no")
        rid = data.get("route_id")
        if rno and rno not in routes:
            routes[rno] = rid or ""
    return [{"route_no": k, "route_id": v} for k, v in sorted(routes.items())]


@app.get("/stats")
async def get_stats() -> dict[str, Any]:
    active = await redis_pool.zcard("active-buses")
    info   = await redis_pool.info("stats")
    return {
        "active_buses": active,
        "total_commands_processed": info.get("total_commands_processed", 0),
        "instantaneous_ops_per_sec": info.get("instantaneous_ops_per_sec", 0),
    }


# ── WebSocket endpoint ─────────────────────────────────────────────────────
@app.websocket("/ws/buses")
async def ws_buses(ws: WebSocket):
    await ws.accept()
    ws_clients.add(ws)
    try:
        while True:
            # keep connection alive; all data comes from pubsub_relay
            await asyncio.sleep(30)
            await ws.send_text('{"type":"ping"}')
    except WebSocketDisconnect:
        pass
    finally:
        ws_clients.discard(ws)


# ── S3/MinIO client ────────────────────────────────────────────────────────
def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
