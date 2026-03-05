/*
 * Copyright (c) 2026 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */

package io.ck.pulse.s3;

import java.util.Properties;

public class S3DiagnosticConfig {
    private final S3AuthMode authMode;
    private final String region;
    private final String endpoint;
    private final String bucket;
    private final String prefix;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final String profileName;
    private final String webIdentityTokenFile;
    private final String roleArn;
    private final String roleSessionName;
    private final boolean pathStyle;
    private final int timeoutSeconds;
    private final String listBucket;
    private final String listPrefix;
    private final String putObjectKey;

    private S3DiagnosticConfig(
        S3AuthMode authMode,
        String region,
        String endpoint,
        String bucket,
        String prefix,
        String accessKeyId,
        String secretAccessKey,
        String sessionToken,
        String profileName,
        String webIdentityTokenFile,
        String roleArn,
        String roleSessionName,
        boolean pathStyle,
        int timeoutSeconds,
        String listBucket,
        String listPrefix,
        String putObjectKey
    ) {
        this.authMode = authMode;
        this.region = region;
        this.endpoint = endpoint;
        this.bucket = bucket;
        this.prefix = prefix;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
        this.profileName = profileName;
        this.webIdentityTokenFile = webIdentityTokenFile;
        this.roleArn = roleArn;
        this.roleSessionName = roleSessionName;
        this.pathStyle = pathStyle;
        this.timeoutSeconds = timeoutSeconds;
        this.listBucket = listBucket;
        this.listPrefix = listPrefix;
        this.putObjectKey = putObjectKey;
    }

    public static S3DiagnosticConfig fromProperties(Properties properties) {
        String rawPath = trimToNull(properties.getProperty("s3Path"));
        String bucket = trimToNull(properties.getProperty("bucket"));
        String prefix = trimToNull(properties.getProperty("prefix"));

        if (rawPath != null) {
            S3Target target = parseS3Path(rawPath);
            if (bucket == null) {
                bucket = target.bucket;
            }
            if (prefix == null) {
                prefix = target.prefix;
            }
        }

        if (bucket == null) {
            throw new IllegalArgumentException(
                "Missing S3 target. Configure either s3Path=s3://bucket/prefix or bucket=<bucket>.");
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
            if (isS3Uri(listPath)) {
                S3Target listTarget = parseS3Path(listPath);
                listBucket = listTarget.bucket;
                listPrefix = listTarget.prefix;
            } else {
                listPrefix = normalizePrefix(listPath);
            }
        }

        S3AuthMode authMode = S3AuthMode.fromString(properties.getProperty("authMode"));
        String region = defaultIfNull(trimToNull(properties.getProperty("region")), "us-east-1");
        String endpoint = trimToNull(properties.getProperty("endpoint"));

        String accessKeyId = null;
        String secretAccessKey = null;
        String sessionToken = null;
        String profileName = null;
        String webIdentityTokenFile = null;
        String roleArn = trimToNull(properties.getProperty("roleArn"));
        String roleSessionName = trimToNull(properties.getProperty("roleSessionName"));

        if (authMode == S3AuthMode.STATIC) {
            accessKeyId = trimToNull(properties.getProperty("accessKeyId"));
            secretAccessKey = trimToNull(properties.getProperty("secretAccessKey"));
            sessionToken = trimToNull(properties.getProperty("sessionToken"));
            if (accessKeyId == null || secretAccessKey == null) {
                throw new IllegalArgumentException("authMode=static requires accessKeyId and secretAccessKey.");
            }
        } else if (authMode == S3AuthMode.PROFILE) {
            profileName = trimToNull(properties.getProperty("profileName"));
        } else if (authMode == S3AuthMode.WEB_IDENTITY) {
            webIdentityTokenFile = trimToNull(properties.getProperty("webIdentityTokenFile"));
        }

        boolean pathStyle = Boolean.parseBoolean(defaultIfNull(trimToNull(properties.getProperty("pathStyle")), "false"));
        int timeoutSeconds = parsePositiveInt(properties.getProperty("timeoutSeconds"), 8);
        String putObjectKey = trimToNull(properties.getProperty("putObjectKey"));
        if (putObjectKey != null) {
            putObjectKey = trimLeadingSlash(putObjectKey);
        }

        return new S3DiagnosticConfig(
            authMode,
            region,
            endpoint,
            bucket,
            prefix,
            accessKeyId,
            secretAccessKey,
            sessionToken,
            profileName,
            webIdentityTokenFile,
            roleArn,
            roleSessionName,
            pathStyle,
            timeoutSeconds,
            listBucket,
            listPrefix,
            putObjectKey
        );
    }

    public S3AuthMode getAuthMode() {
        return authMode;
    }

    public String getRegion() {
        return region;
    }

    public String getEndpoint() {
        return endpoint;
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

    public String getSessionToken() {
        return sessionToken;
    }

    public String getProfileName() {
        return profileName;
    }

    public String getWebIdentityTokenFile() {
        return webIdentityTokenFile;
    }

    public String getRoleArn() {
        return roleArn;
    }

    public String getRoleSessionName() {
        return roleSessionName;
    }

    public boolean hasRoleArn() {
        return roleArn != null;
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

    public boolean hasStaticCredentials() {
        return accessKeyId != null && secretAccessKey != null;
    }

    private static S3Target parseS3Path(String path) {
        String normalized = path.trim();
        if (normalized.startsWith("s3://")) {
            normalized = normalized.substring(5);
        }

        if (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }

        int slashIndex = normalized.indexOf('/');
        if (slashIndex <= 0) {
            if (normalized.isEmpty()) {
                throw new IllegalArgumentException("Invalid s3Path: " + path);
            }
            return new S3Target(normalized, "");
        }

        String bucket = normalized.substring(0, slashIndex).trim();
        String prefix = normalized.substring(slashIndex + 1).trim();
        if (bucket.isEmpty()) {
            throw new IllegalArgumentException("Invalid s3Path (bucket is empty): " + path);
        }
        return new S3Target(bucket, normalizePrefix(prefix));
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

    private static boolean isS3Uri(String value) {
        if (value == null) {
            return false;
        }
        return value.trim().toLowerCase().startsWith("s3://");
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static class S3Target {
        private final String bucket;
        private final String prefix;

        private S3Target(String bucket, String prefix) {
            this.bucket = bucket;
            this.prefix = prefix;
        }
    }
}
