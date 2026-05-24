package com.bustrack.sink;

import com.bustrack.model.BusEvent;
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

public class RedisSink extends RichSinkFunction<BusEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String redisHost;
    private final int redisPort;
    private final int ttlSeconds;

    private transient JedisPool pool;

    public RedisSink(String redisHost, int redisPort, int ttlSeconds) {
        this.redisHost  = redisHost;
        this.redisPort  = redisPort;
        this.ttlSeconds = ttlSeconds;
    }

    @Override
    public void open(Configuration cfg) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(8);
        pool = new JedisPool(poolConfig, redisHost, redisPort);
        LOG.info("RedisSink connected to {}:{}", redisHost, redisPort);
    }

    @Override
    public void invoke(BusEvent event, Context ctx) {
        String key = "bus:" + event.vehicle;

        Map<String, String> fields = new HashMap<>();
        fields.put("vehicle",  event.vehicle);
        fields.put("datetime", String.valueOf(event.datetime));
        if (event.x       != null) fields.put("lon",      String.valueOf(event.x));
        if (event.y       != null) fields.put("lat",      String.valueOf(event.y));
        if (event.speed   != null) fields.put("speed",    String.valueOf(event.speed));
        if (event.heading != null) fields.put("heading",  String.valueOf(event.heading));
        if (event.driver  != null) fields.put("driver",   event.driver);
        if (event.ignition != null) fields.put("ignition", event.ignition ? "1" : "0");
        if (event.aircon   != null) fields.put("aircon",   event.aircon   ? "1" : "0");
        if (event.working  != null) fields.put("working",  event.working  ? "1" : "0");
        if (event.doorUp   != null) fields.put("door_up",  event.doorUp   ? "1" : "0");
        if (event.doorDown != null) fields.put("door_down",event.doorDown ? "1" : "0");
        if (event.routeNo  != null) fields.put("route_no", event.routeNo);
        if (event.routeId  != null) fields.put("route_id", event.routeId);

        try (Jedis jedis = pool.getResource()) {
            jedis.hset(key, fields);
            jedis.expire(key, ttlSeconds);

            // để tra nhanh danh sách xe đang chạy
            jedis.zadd("active-buses", event.datetime, event.vehicle);

            // pub/sub để FastAPI push websocket không cần polling
            String json = MAPPER.writeValueAsString(fields);
            jedis.publish("bus-updates", json);
        } catch (Exception e) {
            LOG.warn("Redis write failed for {}: {}", key, e.getMessage());
        }
    }

    @Override
    public void close() {
        if (pool != null) pool.close();
    }
}
