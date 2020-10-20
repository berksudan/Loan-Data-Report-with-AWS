package org.example;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.List;

public class FileURIsRetriever {
    private final static String[] LIST_OF_FILE_URLS = {
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-15-55-4ad8e4cf-bc6d-4501-9279-0dd550a3dcea.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-17-27-75b1eaf2-70dc-4744-a738-1db60a4060ba.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-18-59-255a946e-e4f0-44f9-af62-c06525d7adcc.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-20-30-ebe52d33-08be-4411-9286-1a2164b81e11.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-22-01-fbee2c74-f07b-48a3-b19c-69d31af5ac02.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-23-32-bee2b756-4fcf-49ac-b04e-a8c6c4a9315c.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-25-02-13ea2ec7-d312-4508-9fa8-510c9575b36e.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-26-35-b99944d0-cb71-4915-a28d-5dfa0e3c396e.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-28-06-b45f8080-6d12-478d-82a0-baa839bff152.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-29-38-2a7185ef-512c-4595-9bdc-b9feb08a1d93.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-31-09-e16ba17e-bd2a-42c4-aee8-8501440f3e1e.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-32-39-8e623792-4e64-45dc-afcd-aefc25d9b2dd.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-34-11-0cf34f02-1ea3-40d1-8009-0d1739a41757.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-35-43-90c97951-b39e-44fd-8265-ccfb060319c2.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-37-15-a7ec0d24-2727-4a7e-8bef-952830ae3e6a.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-38-47-4735393d-d7cc-43e9-a4d6-64fbe5857624.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-40-19-d319b0ee-7e31-41ba-beaf-61afad493b9c.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-41-49-28963a20-820f-4308-a052-c05984dace43.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-43-20-0b1f5724-a510-42c1-b8a3-7edc2daee251.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-44-52-8b304636-c08d-4ddb-9cd7-c09bf9e35f41.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-46-23-dce44b91-19f4-4fd4-a03b-e11e747a6a27.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-47-54-5b570e6a-e86a-40eb-a9be-f033b73a183d.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-49-27-14334549-230b-4369-8f64-dfcd4917fec6.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-50-57-da10a966-927b-40c4-9536-36b63f0cdfdf.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-52-29-3d24851a-948a-4cc4-a776-2f41396383c4.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-54-00-4a608f87-d963-4495-a5ca-322107639c90.gz",
            "/2020/10/19/09/Loan-Data-Loader-1-2020-10-19-09-55-30-eaeeb3a3-a2e9-4a1b-b81d-52c6d9b0b280.gz"
    };
    private String regionName, awsAccessKey, awsSecretKey, sessionToken, bucketName, prefix, extension;
    private AmazonS3 s3Client;

    public static FileURIsRetriever.Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) {
        FileURIsRetriever fur = FileURIsRetriever.builder()
                .region("us-east-1")
                .credentials("ASIAQTRTW4LTTJMPPD5Q", "04btK/hchuQfHMPJgK7bS5b7ws3u3vWDN2iBeU9q", "FwoGZXIvYXdzECcaDA26do0bD0e0WOBolSLKAVkkFc/aFINT2GUU3P8tZTAof0IWsCx0iPJRra3WGK/+IIJ92pmIdXVuYltl473Vmtb3asWJse+ie3RXy4AQR3O/9mrpnCsFgVeBnyNtF+2wGADlzQ2rK77FNaNRIAXkkY/eAM/6ahIWxRKCja1O84pFFrXhtC2n0UU5AeNinravQMk69F4mL2wtdM8lCPMoh36NovPtPrbR+9HwB4QVM5TqNsVL/sXL6z8MtXzjpGCYrClmi82f9DmOkB/9Zo16LxLyUrXircgM7KkowtG7/AUyLXBshxgxfyBLj3AaM5+jDb7AMG7v6yFdx/AzDKHa7MOZuAQEvj0bsyBofGlmVQ==")
                .bucketObjectsInfo("loan-data-bucket-aws", "20")
                .extension(".gz")
                .build();
        String[] tmps = fur.retrievex();
        for(String s : tmps)
            System.out.println(s);
    }

    public String[] retrievex() {
        this.s3Client = createClient();
        List<S3ObjectSummary> summaries = getRecursiveObjectSummaries();
        return summaries.stream()
                .map(S3ObjectSummary::getKey)
                .filter(name -> name.endsWith(extension))
                .map(file -> "/" + file)
                .sorted()
                .toArray(String[]::new);
    }
    public static String[] retrieve() {
        return null;
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
