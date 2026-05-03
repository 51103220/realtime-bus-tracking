package com.bustrack.sink;

import com.bustrack.model.RouteSpeedSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Writes sliding-window route speed aggregates to Redis.
 * Key: route:speed:{routeNo} — HSET with avgSpeed, minSpeed, maxSpeed, vehicleCount, windowEndMs
 * TTL: 120s (two window periods)
 */
public class RouteSpeedRedisSink extends RichSinkFunction<RouteSpeedSnapshot> {

    private static final Logger LOG = LoggerFactory.getLogger(RouteSpeedRedisSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String redisHost;
    private final int    redisPort;
    private transient JedisPool pool;

    public RouteSpeedRedisSink(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public void open(Configuration cfg) {
        pool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort);
    }

    @Override
    public void invoke(RouteSpeedSnapshot snap, Context ctx) {
        if (snap.routeNo == null) return;
        String key = "route:speed:" + snap.routeNo;
        Map<String, String> fields = new HashMap<>();
        fields.put("avgSpeed",     String.format("%.2f", snap.avgSpeedKmh));
        fields.put("minSpeed",     String.format("%.2f", snap.minSpeedKmh));
        fields.put("maxSpeed",     String.format("%.2f", snap.maxSpeedKmh));
        fields.put("vehicleCount", String.valueOf(snap.vehicleCount));
        fields.put("eventCount",   String.valueOf(snap.eventCount));
        fields.put("windowEndMs",  String.valueOf(snap.windowEndMs));
        try (Jedis jedis = pool.getResource()) {
            jedis.hset(key, fields);
            jedis.expire(key, 120);
        } catch (Exception e) {
            LOG.warn("RouteSpeedRedisSink failed for {}: {}", key, e.getMessage());
        }
    }

    @Override
    public void close() {
        if (pool != null) pool.close();
    }
}
