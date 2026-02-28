package io.debezium.postgres2lake.engine.iceberg;

import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.HttpClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Map;

public class InstrumentedS3FileIOAwsClientFactory implements S3FileIOAwsClientFactory {
    private S3FileIOProperties s3FileIOProperties;
    private HttpClientProperties httpClientProperties;
    private AwsClientProperties awsClientProperties;

    public InstrumentedS3FileIOAwsClientFactory() {
        this.s3FileIOProperties = new S3FileIOProperties();
        this.httpClientProperties = new HttpClientProperties();
        this.awsClientProperties = new AwsClientProperties();
    }

    @Override
    public void initialize(Map<String, String> properties) {
        this.s3FileIOProperties = new S3FileIOProperties(properties);
        this.awsClientProperties = new AwsClientProperties(properties);
        this.httpClientProperties = new HttpClientProperties(properties);
    }

    @Override
    public S3Client s3() {
        return S3Client.builder()
                .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                .applyMutation(httpClientProperties::applyHttpClientConfigurations)
                .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                .applyMutation(s3FileIOProperties::applyServiceConfigurations)
                .applyMutation(
                        s3ClientBuilder ->
                                s3FileIOProperties.applyCredentialConfigurations(
                                        awsClientProperties, s3ClientBuilder))
                .applyMutation(s3FileIOProperties::applySignerConfiguration)
                .applyMutation(s3FileIOProperties::applyS3AccessGrantsConfigurations)
                .applyMutation(s3FileIOProperties::applyUserAgentConfigurations)
                .applyMutation(s3FileIOProperties::applyRetryConfigurations)
                .region(Region.of("none"))
                .build();
    }

    @Override
    public S3AsyncClient s3Async() {
        if (s3FileIOProperties.isS3CRTEnabled()) {
            return S3AsyncClient.crtBuilder()
                    .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                    .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
                    .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                    .applyMutation(s3FileIOProperties::applyS3CrtConfigurations)
                    .region(Region.of("none"))
                    .build();
        }
        return S3AsyncClient.builder()
                .applyMutation(awsClientProperties::applyClientRegionConfiguration)
                .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
                .applyMutation(awsClientProperties::applyLegacyMd5Plugin)
                .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
                .region(Region.of("none"))
                .build();
    }
}
