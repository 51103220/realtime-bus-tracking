package com.bustrack.sink;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.bustrack.model.TripSegment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ghi mỗi chuyến đi vào MinIO: trips/{routeNo}/{vehicle}_{tripStart}.json
public class TripSegmentMinIOSink extends RichSinkFunction<TripSegment> {

    private static final Logger LOG = LoggerFactory.getLogger(TripSegmentMinIOSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String bucket;
    private transient AmazonS3 s3;

    public TripSegmentMinIOSink(String endpoint, String accessKey, String secretKey, String bucket) {
        this.endpoint  = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket    = bucket;
    }

    @Override
    public void open(Configuration cfg) {
        s3 = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withClientConfiguration(new ClientConfiguration().withSignerOverride("AWSS3V4SignerType"))
                .withPathStyleAccessEnabled(true)
                .build();
    }

    @Override
    public void invoke(TripSegment seg, Context ctx) {
        String route = seg.routeNo != null ? seg.routeNo : "UNKNOWN";
        String key = String.format("trips/%s/%s_%d.json", route,
                seg.vehicle != null ? seg.vehicle.substring(0, 8) : "unk",
                seg.tripStart);
        try {
            s3.putObject(bucket, key, MAPPER.writeValueAsString(seg));
        } catch (Exception e) {
            LOG.warn("TripSegmentMinIOSink failed for {}: {}", key, e.getMessage());
        }
    }
}
