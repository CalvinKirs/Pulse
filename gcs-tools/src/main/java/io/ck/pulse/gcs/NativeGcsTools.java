/*
 * Copyright (c) 2026 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */

package io.ck.pulse.gcs;

import io.ck.pulse.common.ConfigUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class NativeGcsTools {

    public static void main(String[] args) {
        try {
            System.setOut(new java.io.PrintStream(System.out, true, StandardCharsets.UTF_8.toString()));
            System.setErr(new java.io.PrintStream(System.err, true, StandardCharsets.UTF_8.toString()));
        } catch (Exception ignored) {
            // Keep default stream settings when UTF-8 setup fails.
        }

        printBanner();

        GcsDiagnosticConfig config;
        try {
            Properties properties = ConfigUtils.loadConfig();
            config = GcsDiagnosticConfig.fromProperties(properties);
        } catch (Exception ex) {
            System.out.println("Configuration error: " + conciseError(ex));
            System.exit(1);
            return;
        }

        DiagnosticReport report = runDiagnostics(config);
        printReport(config, report);
        System.exit(report.isPassed() ? 0 : 1);
    }

    private static DiagnosticReport runDiagnostics(GcsDiagnosticConfig config) {
        List<OperationResult> operationResults = new ArrayList<>();
        String putObjectKey = resolvePutObjectKey(config);

        try (S3Client client = buildGcsClient(config)) {
            operationResults.add(checkHeadBucket(client, config));
            operationResults.add(checkListObjects(client, config));
            if (config.isWriteCheckEnabled()) {
                operationResults.add(checkPutObject(client, config, putObjectKey));
            } else {
                operationResults.add(new OperationResult(
                    Operation.PUT_OBJECT,
                    null,
                    buildPutTarget(config, putObjectKey),
                    null,
                    null,
                    "Skipped because writeCheckEnabled=false."
                ));
            }
        } catch (Exception ex) {
            String error = "Failed to initialize GCS XML API client: " + conciseError(ex);
            operationResults.add(new OperationResult(
                Operation.HEAD_BUCKET,
                false,
                buildBucketTarget(config),
                null,
                null,
                error
            ));
            operationResults.add(new OperationResult(
                Operation.LIST_OBJECTS,
                false,
                buildListTarget(config),
                null,
                null,
                error
            ));
            operationResults.add(new OperationResult(
                Operation.PUT_OBJECT,
                config.isWriteCheckEnabled() ? false : null,
                buildPutTarget(config, putObjectKey),
                null,
                null,
                config.isWriteCheckEnabled() ? error : "Skipped because writeCheckEnabled=false."
            ));
        }

        return new DiagnosticReport(maskAccessKey(config.getAccessKeyId()), operationResults);
    }

    private static S3Client buildGcsClient(GcsDiagnosticConfig config) {
        ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
            .apiCallTimeout(Duration.ofSeconds(config.getTimeoutSeconds()))
            .apiCallAttemptTimeout(Duration.ofSeconds(config.getTimeoutSeconds()))
            .build();

        S3ClientBuilder builder = S3Client.builder()
            .region(Region.of(config.getRegion()))
            .endpointOverride(URI.create(config.getEndpoint()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAccessKeyId(), config.getSecretAccessKey())
                )
            )
            .overrideConfiguration(overrideConfiguration)
            .serviceConfiguration(
                S3Configuration.builder()
                    .pathStyleAccessEnabled(config.isPathStyle())
                    .build()
            );

        return builder.build();
    }

    private static OperationResult checkHeadBucket(S3Client client, GcsDiagnosticConfig config) {
        String target = buildBucketTarget(config);
        try {
            HeadBucketResponse response = client.headBucket(
                HeadBucketRequest.builder().bucket(config.getBucket()).build()
            );
            return new OperationResult(
                Operation.HEAD_BUCKET,
                true,
                target,
                response.sdkHttpResponse().statusCode(),
                null,
                "HeadBucket succeeded."
            );
        } catch (S3Exception ex) {
            return fromS3Exception(Operation.HEAD_BUCKET, target, ex);
        } catch (SdkException ex) {
            return new OperationResult(
                Operation.HEAD_BUCKET,
                false,
                target,
                null,
                null,
                conciseError(ex)
            );
        }
    }

    private static OperationResult checkListObjects(S3Client client, GcsDiagnosticConfig config) {
        String target = buildListTarget(config);
        try {
            ListObjectsV2Response response = client.listObjectsV2(
                ListObjectsV2Request.builder()
                    .bucket(config.getListBucket())
                    .prefix(config.getListPrefix())
                    .maxKeys(1)
                    .build()
            );
            Integer keyCount = response.keyCount();
            return new OperationResult(
                Operation.LIST_OBJECTS,
                true,
                target,
                response.sdkHttpResponse().statusCode(),
                null,
                "ListObjectsV2 succeeded. keyCount=" + (keyCount == null ? 0 : keyCount)
            );
        } catch (S3Exception ex) {
            return fromS3Exception(Operation.LIST_OBJECTS, target, ex);
        } catch (SdkException ex) {
            return new OperationResult(
                Operation.LIST_OBJECTS,
                false,
                target,
                null,
                null,
                conciseError(ex)
            );
        }
    }

    private static OperationResult checkPutObject(S3Client client, GcsDiagnosticConfig config, String key) {
        String target = buildPutTarget(config, key);
        try {
            PutObjectResponse response = client.putObject(
                PutObjectRequest.builder()
                    .bucket(config.getBucket())
                    .key(key)
                    .contentType("text/plain")
                    .build(),
                RequestBody.fromString("Pulse GCS connectivity probe at " + Instant.now().toString())
            );

            String cleanupMessage = "cleanup skipped";
            try {
                client.deleteObject(DeleteObjectRequest.builder().bucket(config.getBucket()).key(key).build());
                cleanupMessage = "cleanup deleted";
            } catch (Exception cleanupEx) {
                cleanupMessage = "cleanup failed: " + conciseError(cleanupEx);
            }

            return new OperationResult(
                Operation.PUT_OBJECT,
                true,
                target,
                response.sdkHttpResponse().statusCode(),
                null,
                "PutObject succeeded, " + cleanupMessage + "."
            );
        } catch (S3Exception ex) {
            return fromS3Exception(Operation.PUT_OBJECT, target, ex);
        } catch (SdkException ex) {
            return new OperationResult(
                Operation.PUT_OBJECT,
                false,
                target,
                null,
                null,
                conciseError(ex)
            );
        }
    }

    private static OperationResult fromS3Exception(Operation operation, String target, S3Exception ex) {
        String errorCode = ex.awsErrorDetails() != null ? ex.awsErrorDetails().errorCode() : null;
        Integer statusCode = ex.statusCode();
        return new OperationResult(operation, false, target, statusCode, errorCode, conciseError(ex));
    }

    private static String resolvePutObjectKey(GcsDiagnosticConfig config) {
        if (!isBlank(config.getPutObjectKey())) {
            return config.getPutObjectKey();
        }

        String prefix = config.getPrefix();
        String fileName = "pulse-gcs-connectivity-check-" + System.currentTimeMillis() + ".txt";
        if (isBlank(prefix)) {
            return fileName;
        }
        if (prefix.endsWith("/")) {
            return prefix + fileName;
        }
        return prefix + "/" + fileName;
    }

    private static String buildBucketTarget(GcsDiagnosticConfig config) {
        return "gs://" + config.getBucket();
    }

    private static String buildListTarget(GcsDiagnosticConfig config) {
        if (isBlank(config.getListPrefix())) {
            return "gs://" + config.getListBucket();
        }
        return "gs://" + config.getListBucket() + "/" + config.getListPrefix();
    }

    private static String buildPutTarget(GcsDiagnosticConfig config, String key) {
        return "gs://" + config.getBucket() + "/" + key;
    }

    private static void printBanner() {
        System.out.println("============================================================");
        System.out.println(" Pulse GCS Connectivity Diagnostic Tool");
        System.out.println("============================================================");
    }

    private static void printReport(GcsDiagnosticConfig config, DiagnosticReport report) {
        System.out.println();
        System.out.println("[Target]");
        System.out.println("  endpoint          : " + safe(config.getEndpoint()));
        System.out.println("  region            : " + safe(config.getRegion()));
        System.out.println("  bucket            : " + safe(config.getBucket()));
        System.out.println("  prefix            : " + safePrefix(config.getPrefix()));
        System.out.println("  lsPath            : " + buildListTarget(config));
        System.out.println("  pathStyle         : " + config.isPathStyle());
        System.out.println("  writeCheckEnabled : " + config.isWriteCheckEnabled());

        System.out.println();
        System.out.println("[Authentication]");
        System.out.println("  mode              : HMAC_STATIC");
        System.out.println("  accessKeyId       : " + safe(report.getMaskedAccessKey()));

        System.out.println();
        System.out.println("[Checks]");
        for (OperationResult result : report.getOperationResults()) {
            System.out.println("  * " + result.getOperation());
            System.out.println("    allowed     : " + formatAllowed(result.getAllowed()));
            System.out.println("    target      : " + safe(result.getTarget()));
            System.out.println("    statusCode  : " + safe(result.getStatusCode()));
            System.out.println("    errorCode   : " + safe(result.getErrorCode()));
            System.out.println("    detail      : " + safe(result.getMessage()));
        }

        System.out.println();
        if (report.isPassed()) {
            System.out.println("[Result] PASSED - GCS XML API connectivity is available with the supplied HMAC key.");
        } else {
            System.out.println("[Result] FAILED - one or more required checks did not pass.");
        }
    }

    private static String formatAllowed(Boolean allowed) {
        if (allowed == null) {
            return "SKIPPED";
        }
        return String.valueOf(allowed);
    }

    private static String safePrefix(String prefix) {
        return isBlank(prefix) ? "(root)" : prefix;
    }

    private static String safe(Object value) {
        return value == null ? "N/A" : String.valueOf(value);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String maskAccessKey(String accessKey) {
        if (isBlank(accessKey)) {
            return "N/A";
        }
        if (accessKey.length() <= 6) {
            return "***" + accessKey;
        }
        String prefix = accessKey.substring(0, 4);
        String suffix = accessKey.substring(accessKey.length() - 2);
        return prefix + "..." + suffix;
    }

    private static String conciseError(Throwable throwable) {
        String message = throwable.getMessage();
        String className = throwable.getClass().getSimpleName();
        if (isBlank(message)) {
            return className;
        }
        return className + ": " + message;
    }

    private enum Operation {
        HEAD_BUCKET,
        LIST_OBJECTS,
        PUT_OBJECT
    }

    private static class DiagnosticReport {
        private final String maskedAccessKey;
        private final List<OperationResult> operationResults;

        private DiagnosticReport(String maskedAccessKey, List<OperationResult> operationResults) {
            this.maskedAccessKey = maskedAccessKey;
            this.operationResults = operationResults;
        }

        private String getMaskedAccessKey() {
            return maskedAccessKey;
        }

        private List<OperationResult> getOperationResults() {
            return operationResults;
        }

        private boolean isPassed() {
            for (OperationResult result : operationResults) {
                if (Boolean.FALSE.equals(result.getAllowed())) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class OperationResult {
        private final Operation operation;
        private final Boolean allowed;
        private final String target;
        private final Integer statusCode;
        private final String errorCode;
        private final String message;

        private OperationResult(
            Operation operation,
            Boolean allowed,
            String target,
            Integer statusCode,
            String errorCode,
            String message
        ) {
            this.operation = operation;
            this.allowed = allowed;
            this.target = target;
            this.statusCode = statusCode;
            this.errorCode = errorCode;
            this.message = message;
        }

        private Operation getOperation() {
            return operation;
        }

        private Boolean getAllowed() {
            return allowed;
        }

        private String getTarget() {
            return target;
        }

        private Integer getStatusCode() {
            return statusCode;
        }

        private String getErrorCode() {
            return errorCode;
        }

        private String getMessage() {
            return message;
        }
    }
}
