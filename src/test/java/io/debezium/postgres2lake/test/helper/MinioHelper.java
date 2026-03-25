package io.debezium.postgres2lake.test.helper;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.util.List;

public class MinioHelper {
    private final S3Client s3Client;

    private final String endpoint;
    private final String accessKey;
    private final String secretAccessKey;

    public MinioHelper(String endpoint, String accessKey, String secretAccessKey) {
        this.s3Client = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretAccessKey)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();

        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretAccessKey = secretAccessKey;
    }

    public void createBucket(String bucket) {
        try {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        } catch (BucketAlreadyExistsException | BucketAlreadyOwnedByYouException ignored) {
            // ignore already existing bucket
        }
    }

    public void clearBucket(String bucket) {
        var listObjectsRq = ListObjectsV2Request
                .builder()
                .bucket(bucket)
                .build();
        var objects = s3Client.listObjectsV2(listObjectsRq);

        objects.contents().forEach(content -> {
            var deleteObjectRq = DeleteObjectRequest
                    .builder()
                    .bucket(bucket)
                    .key(content.key())
                    .build();

            s3Client.deleteObject(deleteObjectRq);
        });
    }

    public List<String> listObjectKeys(String bucket, String prefix) {
        var request = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .build();
        return s3Client.listObjectsV2(request).contents().stream()
                .map(S3Object::key)
                .toList();
    }

    public byte[] getObjectBytes(String bucket, String key) {
        var request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        return s3Client.getObjectAsBytes(request).asByteArray();
    }

    public String endpoint() {
        return endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }
}
