package com.bustrack.sink;

import com.bustrack.model.AnomalyEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class AnomalyRedisSink extends RichSinkFunction<AnomalyEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyRedisSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String KEY = "anomalies:recent";
    private static final long   MAX_LIST_SIZE = 500;

    private final String redisHost;
    private final int    redisPort;
    private transient JedisPool pool;

    public AnomalyRedisSink(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public void open(Configuration cfg) {
        pool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort);
    }

    @Override
    public void invoke(AnomalyEvent event, Context ctx) {
        try (Jedis jedis = pool.getResource()) {
            String json = MAPPER.writeValueAsString(event);
            jedis.lpush(KEY, json);
            jedis.ltrim(KEY, 0, MAX_LIST_SIZE - 1); // giới hạn 500 cái thôi
            jedis.publish("bus-anomalies", json);
        } catch (Exception e) {
            LOG.warn("AnomalyRedisSink failed: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        if (pool != null) pool.close();
    }
}
