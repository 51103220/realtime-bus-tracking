package com.bustrack;

import com.bustrack.deserialization.BusEventDeserializer;
import com.bustrack.functions.*;
import com.bustrack.model.*;
import com.bustrack.sink.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class BusTrackingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(30_000);

        String kafkaBootstrap   = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String kafkaTopic       = getEnv("KAFKA_TOPIC",             "bus-gps-events");
        String routeMappingPath = getEnv("ROUTE_MAPPING_PATH",      "/opt/flink/data/vehicle_route_mapping.csv");
        String redisHost        = getEnv("REDIS_HOST",              "redis");
        int    redisPort        = Integer.parseInt(getEnv("REDIS_PORT",             "6379"));
        int    redisTtl         = Integer.parseInt(getEnv("BUS_STATE_TTL_SECONDS",  "3600"));
        int    dedupTtl         = Integer.parseInt(getEnv("DEDUP_TTL_SECONDS",      "60"));
        int    watermarkDelay   = Integer.parseInt(getEnv("WATERMARK_DELAY_SECONDS","30"));
        int    windowSize       = Integer.parseInt(getEnv("WINDOW_SIZE_SECONDS",    "60"));
        String minioEndpoint    = getEnv("MINIO_ENDPOINT",   "http://minio:9000");
        String minioAccessKey   = getEnv("MINIO_ACCESS_KEY", "minioadmin");
        String minioSecretKey   = getEnv("MINIO_SECRET_KEY", "minioadmin");
        String minioBucket      = getEnv("MINIO_BUCKET",     "bus-history");

        // 1. đọc từ Kafka
        KafkaSource<BusEvent> kafkaSource = KafkaSource.<BusEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(kafkaTopic)
                .setGroupId("bus-tracking-flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new BusEventDeserializer())
                .build();

        WatermarkStrategy<BusEvent> watermarkStrategy = WatermarkStrategy
                .<BusEvent>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkDelay))
                .withTimestampAssigner((event, ts) -> event.datetime * 1000L);

        DataStream<BusEvent> rawStream = env
                .fromSource(kafkaSource, watermarkStrategy, "Kafka GPS Source");

        // 2. lọc tọa độ hợp lệ
        SingleOutputStreamOperator<BusEvent> validatedStream = rawStream
                .process(new CoordinateValidatorProcess())
                .name("GPS Coordinate Validation");

        // side output: event lỗi — in ra stdout cho demo
        DataStream<InvalidBusEvent> invalidStream = validatedStream
                .getSideOutput(CoordinateValidatorProcess.INVALID_TAG);
        invalidStream
                .map(e -> "INVALID[" + e.reason + "] vehicle=" + (e.event != null ? e.event.vehicle : "null"))
                .print("invalid-events");

        // 3. dedup bloom filter + gắn tuyến
        SingleOutputStreamOperator<BusEvent> dedupedStream = validatedStream
                .keyBy(e -> e.vehicle)
                .process(new BloomFilterDeduplicator(routeMappingPath, dedupTtl))
                .name("Bloom Filter Dedup + Route Enrichment");

        // side output: metrics dedup — in cho dễ theo dõi
        dedupedStream
                .getSideOutput(BloomFilterDeduplicator.METRICS_TAG)
                .map(m -> String.format("DEDUP[%s] seen=%d dropped=%d rate=%.3f bloomBytes=%d",
                        m.vehicle, m.totalSeen, m.duplicatesDropped, m.duplicateRate, m.bloomFilterSizeBytes))
                .print("dedup-metrics");

        // 4. phát hiện bất thường
        SingleOutputStreamOperator<BusEvent> cleanStream = dedupedStream
                .keyBy(e -> e.vehicle)
                .process(new AnomalyDetector())
                .name("Anomaly Detection");

        DataStream<AnomalyEvent> anomalyStream = cleanStream
                .getSideOutput(AnomalyDetector.ANOMALY_TAG);
        anomalyStream
                .addSink(new AnomalyRedisSink(redisHost, redisPort))
                .name("Anomaly Redis Sink");

        // 5a. lưu trạng thái xe vào Redis
        cleanStream
                .addSink(new RedisSink(redisHost, redisPort, redisTtl))
                .name("Redis Current State");

        // 5b. cửa sổ trượt — tốc độ tuyến mỗi 30s
        cleanStream
                .keyBy(e -> e.routeNo != null ? e.routeNo : "UNKNOWN")
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(30)))
                .aggregate(new SpeedAggregator(), new RouteSpeedWindowFunction())
                .name("Rolling Route Speed")
                .addSink(new RouteSpeedRedisSink(redisHost, redisPort))
                .name("Route Speed Redis Sink");

        // 5c. session window — tách chuyến đi (gap 5 phút)
        cleanStream
                .keyBy(e -> e.vehicle)
                .window(EventTimeSessionWindows.withGap(Time.minutes(5)))
                .process(new TripSegmentFunction())
                .name("Trip Segmentation (Session Windows)")
                .addSink(new TripSegmentMinIOSink(minioEndpoint, minioAccessKey, minioSecretKey, minioBucket))
                .name("Trip Segment MinIO Sink");

        // 5d. tumbling window — lưu lịch sử theo tuyến
        cleanStream
                .keyBy(e -> e.routeNo != null ? e.routeNo : "UNKNOWN")
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .process(new MinIOSink(minioEndpoint, minioAccessKey, minioSecretKey, minioBucket))
                .name("MinIO Historical Archive");

        // 5e. tumbling 1h — trích feature vector cho PCA
        cleanStream
                .keyBy(e -> e.vehicle)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new VehicleFeatureExtractor())
                .name("Vehicle Feature Extraction (PCA input)")
                .addSink(new FeatureVectorMinIOSink(minioEndpoint, minioAccessKey, minioSecretKey, minioBucket))
                .name("Feature Vector MinIO Sink");

        env.execute("Bus GPS Tracking Pipeline");
    }

    private static String getEnv(String key, String defaultVal) {
        String val = System.getenv(key);
        return (val != null && !val.isEmpty()) ? val : defaultVal;
    }
}
