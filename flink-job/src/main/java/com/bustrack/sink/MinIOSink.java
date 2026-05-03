package com.bustrack.sink;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.bustrack.model.BusEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class MinIOSink extends ProcessWindowFunction<BusEvent, Void, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(MinIOSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter HOUR_FMT = DateTimeFormatter.ofPattern("HH-mm").withZone(ZoneOffset.UTC);

    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String bucket;

    private transient AmazonS3 s3;

    public MinIOSink(String endpoint, String accessKey, String secretKey, String bucket) {
        this.endpoint  = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket    = bucket;
    }

    private AmazonS3 getS3() {
        if (s3 == null) {
            s3 = AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                    .withCredentials(
                            new AWSStaticCredentialsProvider(
                                    new BasicAWSCredentials(accessKey, secretKey)))
                    .withClientConfiguration(
                            new ClientConfiguration().withSignerOverride("AWSS3V4SignerType"))
                    .withPathStyleAccessEnabled(true)
                    .build();
        }
        return s3;
    }

    @Override
    public void process(String routeNo, Context ctx, Iterable<BusEvent> events, Collector<Void> out) {
        List<BusEvent> batch = new ArrayList<>();
        events.forEach(batch::add);

        if (batch.isEmpty()) return;

        Instant windowStart = Instant.ofEpochMilli(ctx.window().getStart());
        String dateStr  = DATE_FMT.format(windowStart);
        String hourStr  = HOUR_FMT.format(windowStart);
        String s3Key    = dateStr + "/" + hourStr + "/" + routeNo + "_" + ctx.window().getStart() + ".json";

        try {
            String json = MAPPER.writeValueAsString(batch);
            getS3().putObject(bucket, s3Key, json);
            LOG.debug("MinIO PUT {}/{} ({} events)", bucket, s3Key, batch.size());
        } catch (Exception e) {
            LOG.warn("MinIO write failed for key {}: {}", s3Key, e.getMessage());
        }
    }
}
