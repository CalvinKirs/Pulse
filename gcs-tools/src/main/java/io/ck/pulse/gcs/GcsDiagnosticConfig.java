/*
 * Copyright (c) 2026 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */

package io.ck.pulse.gcs;

import java.util.Properties;

public class GcsDiagnosticConfig {
    private static final String DEFAULT_ENDPOINT = "https://storage.googleapis.com";
    private static final String DEFAULT_REGION = "auto";

    private final String endpoint;
    private final String region;
    private final String bucket;
    private final String prefix;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final boolean pathStyle;
    private final int timeoutSeconds;
    private final String listBucket;
    private final String listPrefix;
    private final String putObjectKey;
    private final boolean writeCheckEnabled;

    private GcsDiagnosticConfig(
        String endpoint,
        String region,
        String bucket,
        String prefix,
        String accessKeyId,
        String secretAccessKey,
        boolean pathStyle,
        int timeoutSeconds,
        String listBucket,
        String listPrefix,
        String putObjectKey,
        boolean writeCheckEnabled
    ) {
        this.endpoint = endpoint;
        this.region = region;
        this.bucket = bucket;
        this.prefix = prefix;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.pathStyle = pathStyle;
        this.timeoutSeconds = timeoutSeconds;
        this.listBucket = listBucket;
        this.listPrefix = listPrefix;
        this.putObjectKey = putObjectKey;
        this.writeCheckEnabled = writeCheckEnabled;
    }

    public static GcsDiagnosticConfig fromProperties(Properties properties) {
        String rawPath = trimToNull(properties.getProperty("gcsPath"));
        String bucket = trimToNull(properties.getProperty("bucket"));
        String prefix = trimToNull(properties.getProperty("prefix"));

        if (rawPath != null) {
            GcsTarget target = parseGcsPath(rawPath);
            if (bucket == null) {
                bucket = target.bucket;
            }
            if (prefix == null) {
                prefix = target.prefix;
            }
        }

        if (bucket == null) {
            throw new IllegalArgumentException(
                "Missing GCS target. Configure either gcsPath=gs://bucket/prefix or bucket=<bucket>."
            );
        }

        if (prefix == null) {
            prefix = "";
        } else {
            prefix = normalizePrefix(prefix);
        }

        String listBucket = bucket;
        String listPrefix = prefix;
        String listPath = trimToNull(properties.getProperty("lsPath"));
        if (listPath != null) {
            if (isGcsUri(listPath)) {
                GcsTarget listTarget = parseGcsPath(listPath);
                listBucket = listTarget.bucket;
                listPrefix = listTarget.prefix;
            } else {
                listPrefix = normalizePrefix(listPath);
            }
        }

        String accessKeyId = firstNonBlank(
            trimToNull(properties.getProperty("ak")),
            trimToNull(properties.getProperty("accessKeyId")),
            trimToNull(properties.getProperty("hmacAccessKey"))
        );
        String secretAccessKey = firstNonBlank(
            trimToNull(properties.getProperty("sk")),
            trimToNull(properties.getProperty("secretAccessKey")),
            trimToNull(properties.getProperty("hmacSecretKey"))
        );

        if (accessKeyId == null || secretAccessKey == null) {
            throw new IllegalArgumentException("Missing HMAC credentials. Configure ak/sk or accessKeyId/secretAccessKey.");
        }

        String endpoint = defaultIfNull(trimToNull(properties.getProperty("endpoint")), DEFAULT_ENDPOINT);
        String region = defaultIfNull(trimToNull(properties.getProperty("region")), DEFAULT_REGION);
        boolean pathStyle = Boolean.parseBoolean(defaultIfNull(trimToNull(properties.getProperty("pathStyle")), "true"));
        int timeoutSeconds = parsePositiveInt(properties.getProperty("timeoutSeconds"), 10);
        boolean writeCheckEnabled = Boolean.parseBoolean(
            defaultIfNull(trimToNull(properties.getProperty("writeCheckEnabled")), "false")
        );

        String putObjectKey = trimToNull(properties.getProperty("putObjectKey"));
        if (putObjectKey != null) {
            putObjectKey = trimLeadingSlash(putObjectKey);
        }

        return new GcsDiagnosticConfig(
            endpoint,
            region,
            bucket,
            prefix,
            accessKeyId,
            secretAccessKey,
            pathStyle,
            timeoutSeconds,
            listBucket,
            listPrefix,
            putObjectKey,
            writeCheckEnabled
        );
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getRegion() {
        return region;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public boolean isPathStyle() {
        return pathStyle;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public String getListBucket() {
        return listBucket;
    }

    public String getListPrefix() {
        return listPrefix;
    }

    public String getPutObjectKey() {
        return putObjectKey;
    }

    public boolean isWriteCheckEnabled() {
        return writeCheckEnabled;
    }

    private static GcsTarget parseGcsPath(String path) {
        String normalized = path.trim();
        String lowerCase = normalized.toLowerCase();
        if (lowerCase.startsWith("gs://")) {
            normalized = normalized.substring(5);
        } else if (lowerCase.startsWith("gcs://")) {
            normalized = normalized.substring(6);
        }

        if (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }

        int slashIndex = normalized.indexOf('/');
        if (slashIndex <= 0) {
            if (normalized.isEmpty()) {
                throw new IllegalArgumentException("Invalid gcsPath: " + path);
            }
            return new GcsTarget(normalized, "");
        }

        String bucket = normalized.substring(0, slashIndex).trim();
        String prefix = normalized.substring(slashIndex + 1).trim();
        if (bucket.isEmpty()) {
            throw new IllegalArgumentException("Invalid gcsPath (bucket is empty): " + path);
        }
        return new GcsTarget(bucket, normalizePrefix(prefix));
    }

    private static boolean isGcsUri(String value) {
        if (value == null) {
            return false;
        }
        String normalized = value.trim().toLowerCase();
        return normalized.startsWith("gs://") || normalized.startsWith("gcs://");
    }

    private static String normalizePrefix(String prefix) {
        String value = trimLeadingSlash(prefix);
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    private static String trimLeadingSlash(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        while (trimmed.startsWith("/")) {
            trimmed = trimmed.substring(1);
        }
        return trimmed;
    }

    private static int parsePositiveInt(String value, int defaultValue) {
        String trimmed = trimToNull(value);
        if (trimmed == null) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(trimmed);
            return parsed > 0 ? parsed : defaultValue;
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    private static String defaultIfNull(String value, String defaultValue) {
        return value == null ? defaultValue : value;
    }

    private static String firstNonBlank(String first, String second, String third) {
        if (first != null) {
            return first;
        }
        if (second != null) {
            return second;
        }
        if (third != null) {
            return third;
        }
        return null;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static class GcsTarget {
        private final String bucket;
        private final String prefix;

        private GcsTarget(String bucket, String prefix) {
            this.bucket = bucket;
            this.prefix = prefix;
        }
    }
}
