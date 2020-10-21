package org.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.util.Arrays;
import java.util.List;

public class FileURIsRetriever {
    private String regionName, awsAccessKey, awsSecretKey, sessionToken, bucketName, prefix, extension;
    private AmazonS3 s3Client;

    public static FileURIsRetriever.Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) {
        FileURIsRetriever.Test(
                "EXAMPLE_ACCESS_KEY",
                "EXAMPLE_SECRET_KEY",
                "EXAMPLE_SESSION_TOKEN"
        );
    }

    public static void Test(String awsAccessKey, String awsSecretKey, String sessionToken) {
        FileURIsRetriever fur = FileURIsRetriever.builder()
                .region("us-east-1")
                .credentials(awsAccessKey, awsSecretKey, sessionToken)
                .bucketObjectsInfo("loan-data-bucket-aws", "20")
                .extension(".gz")
                .build();
        String[] retrievedFiles = fur.retrieve();
        Arrays.stream(retrievedFiles).forEach(System.out::println);
    }

    public String[] retrieve() {
        this.s3Client = createClient();
        List<S3ObjectSummary> summaries = getRecursiveObjectSummaries();
        return summaries.stream()
                .map(S3ObjectSummary::getKey)
                .filter(name -> name.endsWith(extension))
                .map(file -> "/" + file)
                .sorted()
                .toArray(String[]::new);
    }

    public AmazonS3 createClient() {
        AWSStaticCredentialsProvider credentials = new AWSStaticCredentialsProvider(
                new BasicSessionCredentials(awsAccessKey, awsSecretKey, sessionToken));

        return AmazonS3ClientBuilder.standard()
                .withRegion(regionName)
                .withCredentials(credentials)
                .build();
    }

    public List<S3ObjectSummary> getRecursiveObjectSummaries() {
        ObjectListing listing = s3Client.listObjects(bucketName, prefix);
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();

        while (listing.isTruncated()) {
            listing = s3Client.listNextBatchOfObjects(listing);
            summaries.addAll(listing.getObjectSummaries());
        }
        return summaries;
    }

    public static class Builder {
        private final FileURIsRetriever builtFileURIsRetriever;

        public Builder() {
            builtFileURIsRetriever = new FileURIsRetriever();
        }

        public Builder region(String regionName) {
            builtFileURIsRetriever.regionName = regionName;
            return this;
        }

        public Builder credentials(String awsAccessKey, String awsSecretKey, String sessionToken) {
            builtFileURIsRetriever.awsAccessKey = awsAccessKey;
            builtFileURIsRetriever.awsSecretKey = awsSecretKey;
            builtFileURIsRetriever.sessionToken = sessionToken;
            return this;
        }

        public Builder bucketObjectsInfo(String bucketName, String prefix) {
            builtFileURIsRetriever.bucketName = bucketName;
            builtFileURIsRetriever.prefix = prefix;
            return this;
        }

        @SuppressWarnings("unused")
        public Builder bucketObjectsInfo(String bucketName) {
            return bucketObjectsInfo(bucketName, "");
        }

        public Builder extension(String extension) {
            builtFileURIsRetriever.extension = extension;
            return this;
        }

        public FileURIsRetriever build() {
            return builtFileURIsRetriever;
        }

    }
}
