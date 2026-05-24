package com.bustrack.sink;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.bustrack.model.VehicleFeatureVector;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ghi feature vector vào MinIO — notebook PCA đọc từ đây
public class FeatureVectorMinIOSink extends RichSinkFunction<VehicleFeatureVector> {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureVectorMinIOSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String bucket;
    private transient AmazonS3 s3;

    public FeatureVectorMinIOSink(String endpoint, String accessKey, String secretKey, String bucket) {
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
    public void invoke(VehicleFeatureVector vec, Context ctx) {
        String route   = vec.routeNo != null ? vec.routeNo : "UNKNOWN";
        String vehicle = vec.vehicle != null ? vec.vehicle.substring(0, 8) : "unk";
        String key = String.format("features/vehicle-hourly/%s/%s_%d.json", route, vehicle, vec.windowStartMs);
        try {
            s3.putObject(bucket, key, MAPPER.writeValueAsString(vec));
        } catch (Exception e) {
            LOG.warn("FeatureVectorMinIOSink failed for {}: {}", key, e.getMessage());
        }
    }
}
