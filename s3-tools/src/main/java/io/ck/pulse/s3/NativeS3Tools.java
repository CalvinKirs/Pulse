/*
 * Copyright (c) 2026 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */

package io.ck.pulse.s3;

import io.ck.pulse.common.ConfigUtils;
import io.ck.pulse.s3.model.CredentialProbeResult;
import io.ck.pulse.s3.model.CredentialSource;
import io.ck.pulse.s3.model.PermissionCheckResult;
import io.ck.pulse.s3.model.PermissionOperation;
import io.ck.pulse.s3.model.S3ConnectivityReport;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
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
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

public class NativeS3Tools {

    public static void main(String[] args) {
        try {
            System.setOut(new java.io.PrintStream(System.out, true, StandardCharsets.UTF_8.toString()));
            System.setErr(new java.io.PrintStream(System.err, true, StandardCharsets.UTF_8.toString()));
        } catch (Exception ignored) {
            // Keep default stream settings when UTF-8 setup fails.
        }

        printBanner();

        S3DiagnosticConfig config;
        try {
            Properties properties = ConfigUtils.loadConfig();
            config = S3DiagnosticConfig.fromProperties(properties);
        } catch (Exception ex) {
            System.out.println("Configuration error: " + conciseError(ex));
            System.exit(1);
            return;
        }

        S3ConnectivityReport report;
        try {
            report = runDiagnostics(config);
        } catch (Exception ex) {
            System.out.println("Diagnostic error: " + conciseError(ex));
            System.exit(1);
            return;
        }

        printReport(report);

        boolean hasSelectedCredential = report.getSelectedCredentialSource() != null;
        boolean selectedHasDeniedPermission = false;
        for (PermissionCheckResult permissionCheckResult : report.getPermissionCheckResults()) {
            if (permissionCheckResult.getSource() == report.getSelectedCredentialSource()
                && Boolean.FALSE.equals(permissionCheckResult.getAllowed())) {
                selectedHasDeniedPermission = true;
                break;
            }
        }

        System.exit((hasSelectedCredential && !selectedHasDeniedPermission) ? 0 : 1);
    }

    private static S3ConnectivityReport runDiagnostics(S3DiagnosticConfig config) {
        List<ProviderCandidate> candidates = buildCandidates(config);
        List<ProbeState> probeStates = new ArrayList<>();

        for (int i = 0; i < candidates.size(); i++) {
            ProviderCandidate candidate = candidates.get(i);
            boolean detected = isSourceDetected(config, candidate.source);
            CredentialProbeResult probeResult = probeCredentials(config, candidate, detected, i + 1);
            probeStates.add(new ProbeState(candidate, probeResult));
        }

        ProbeState selectedState = findSelectedState(probeStates);
        List<CredentialProbeResult> credentialResults = markSelectedCredentialResult(probeStates, selectedState);

        List<PermissionCheckResult> permissionResults = new ArrayList<>();
        for (ProbeState state : probeStates) {
            if (state.probeResult.isUsable()) {
                permissionResults.addAll(runPermissionChecksForSource(config, state.candidate));
            } else {
                permissionResults.addAll(
                    buildSkippedPermissionChecks(
                        state.candidate.source,
                        config,
                        "Credential is not usable: " + state.probeResult.getMessage()
                    )
                );
            }
        }

        return new S3ConnectivityReport(
            config.getRegion(),
            config.getEndpoint(),
            config.getBucket(),
            config.getPrefix(),
            selectedState == null ? null : selectedState.probeResult.getSource(),
            selectedState == null ? null : selectedState.probeResult.getAccessKeyIdMasked(),
            selectedState == null ? null : selectedState.probeResult.getCallerArn(),
            credentialResults,
            permissionResults
        );
    }

    private static List<ProviderCandidate> buildCandidates(S3DiagnosticConfig config) {
        List<ProviderCandidate> candidates = new ArrayList<>();
        switch (config.getAuthMode()) {
            case AUTO:
                addCandidate(candidates, CredentialSource.SYSTEM_PROPERTIES, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return SystemPropertyCredentialsProvider.create();
                    }
                });
                addCandidate(candidates, CredentialSource.ENVIRONMENT, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return EnvironmentVariableCredentialsProvider.create();
                    }
                });
                addCandidate(candidates, CredentialSource.WEB_IDENTITY, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return WebIdentityTokenFileCredentialsProvider.create();
                    }
                });
                addCandidate(candidates, CredentialSource.PROFILE, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return ProfileCredentialsProvider.create();
                    }
                });
                addCandidate(candidates, CredentialSource.CONTAINER, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return ContainerCredentialsProvider.builder().build();
                    }
                });
                addCandidate(candidates, CredentialSource.INSTANCE_PROFILE, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return InstanceProfileCredentialsProvider.builder().build();
                    }
                });
                break;
            case STATIC:
                addCandidate(candidates, CredentialSource.STATIC, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return buildStaticProvider(config);
                    }
                });
                break;
            case SYSTEM_PROPERTIES:
                addCandidate(candidates, CredentialSource.SYSTEM_PROPERTIES, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return SystemPropertyCredentialsProvider.create();
                    }
                });
                break;
            case ENVIRONMENT:
                addCandidate(candidates, CredentialSource.ENVIRONMENT, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return EnvironmentVariableCredentialsProvider.create();
                    }
                });
                break;
            case WEB_IDENTITY:
                addCandidate(candidates, CredentialSource.WEB_IDENTITY, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return buildWebIdentityProvider(config);
                    }
                });
                break;
            case PROFILE:
                addCandidate(candidates, CredentialSource.PROFILE, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return buildProfileProvider(config);
                    }
                });
                break;
            case CONTAINER:
                addCandidate(candidates, CredentialSource.CONTAINER, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return ContainerCredentialsProvider.builder().build();
                    }
                });
                break;
            case INSTANCE_PROFILE:
                addCandidate(candidates, CredentialSource.INSTANCE_PROFILE, new Supplier<AwsCredentialsProvider>() {
                    @Override
                    public AwsCredentialsProvider get() {
                        return InstanceProfileCredentialsProvider.builder().build();
                    }
                });
                break;
            default:
                throw new IllegalStateException("Unsupported auth mode: " + config.getAuthMode());
        }
        return maybeWrapCandidatesWithAssumeRole(config, candidates);
    }

    private static List<ProviderCandidate> maybeWrapCandidatesWithAssumeRole(
        S3DiagnosticConfig config,
        List<ProviderCandidate> candidates
    ) {
        if (!shouldApplyAssumeRole(config)) {
            return candidates;
        }

        List<ProviderCandidate> baseCandidates = candidates;
        if (config.getAuthMode() == S3AuthMode.AUTO) {
            baseCandidates = filterExplicitAssumeRoleSources(candidates);
        }

        List<ProviderCandidate> wrappedCandidates = new ArrayList<>();
        for (ProviderCandidate candidate : baseCandidates) {
            if (!canUseAsAssumeRoleSource(candidate.source)) {
                wrappedCandidates.add(candidate);
                continue;
            }

            try {
                wrappedCandidates.add(new ProviderCandidate(
                    candidate.source,
                    buildAssumeRoleProvider(config, candidate.provider),
                    config.getRoleArn()
                ));
            } catch (Exception ex) {
                String message = "AssumeRole provider init failed for roleArn=" + config.getRoleArn()
                    + ": " + conciseError(ex);
                wrappedCandidates.add(new ProviderCandidate(candidate.source, failingProvider(message), config.getRoleArn()));
            }
        }
        return wrappedCandidates;
    }

    private static List<ProviderCandidate> filterExplicitAssumeRoleSources(List<ProviderCandidate> candidates) {
        List<ProviderCandidate> filtered = new ArrayList<>();
        for (ProviderCandidate candidate : candidates) {
            if (canUseAsAssumeRoleSource(candidate.source)) {
                filtered.add(candidate);
            }
        }
        return filtered;
    }

    private static boolean shouldApplyAssumeRole(S3DiagnosticConfig config) {
        return config.hasRoleArn() && config.getAuthMode() != S3AuthMode.WEB_IDENTITY;
    }

    private static boolean canUseAsAssumeRoleSource(CredentialSource source) {
        return source == CredentialSource.STATIC
            || source == CredentialSource.SYSTEM_PROPERTIES
            || source == CredentialSource.ENVIRONMENT
            || source == CredentialSource.PROFILE
            || source == CredentialSource.CONTAINER
            || source == CredentialSource.INSTANCE_PROFILE;
    }

    private static AwsCredentialsProvider buildAssumeRoleProvider(
        S3DiagnosticConfig config,
        AwsCredentialsProvider sourceProvider
    ) {
        ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
            .apiCallTimeout(Duration.ofSeconds(config.getTimeoutSeconds()))
            .apiCallAttemptTimeout(Duration.ofSeconds(config.getTimeoutSeconds()))
            .build();

        StsClient stsClient = StsClient.builder()
            .region(Region.of(config.getRegion()))
            .credentialsProvider(sourceProvider)
            .overrideConfiguration(overrideConfiguration)
            .build();

        AssumeRoleRequest request = AssumeRoleRequest.builder()
            .roleArn(config.getRoleArn())
            .roleSessionName(firstNonBlank(config.getRoleSessionName(), "pulse-s3-check", null))
            .build();

        return StsAssumeRoleCredentialsProvider.builder()
            .stsClient(stsClient)
            .refreshRequest(request)
            .build();
    }

    private static void addCandidate(
        List<ProviderCandidate> candidates,
        CredentialSource source,
        Supplier<AwsCredentialsProvider> providerSupplier
    ) {
        try {
            candidates.add(new ProviderCandidate(source, providerSupplier.get()));
        } catch (Exception ex) {
            String message = "Provider init failed: " + conciseError(ex);
            candidates.add(new ProviderCandidate(source, failingProvider(message)));
        }
    }

    private static AwsCredentialsProvider failingProvider(final String message) {
        return new AwsCredentialsProvider() {
            @Override
            public AwsCredentials resolveCredentials() {
                throw new IllegalStateException(message);
            }
        };
    }

    private static AwsCredentialsProvider buildStaticProvider(S3DiagnosticConfig config) {
        if (!config.hasStaticCredentials()) {
            throw new IllegalArgumentException("authMode=static requires both accessKeyId and secretAccessKey");
        }

        if (isBlank(config.getSessionToken())) {
            return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(config.getAccessKeyId(), config.getSecretAccessKey()));
        }

        return StaticCredentialsProvider.create(
            AwsSessionCredentials.create(
                config.getAccessKeyId(),
                config.getSecretAccessKey(),
                config.getSessionToken()
            )
        );
    }

    private static AwsCredentialsProvider buildWebIdentityProvider(S3DiagnosticConfig config) {
        WebIdentityTokenFileCredentialsProvider.Builder builder = WebIdentityTokenFileCredentialsProvider.builder();
        if (!isBlank(config.getRoleArn())) {
            builder.roleArn(config.getRoleArn());
        }
        if (!isBlank(config.getRoleSessionName())) {
            builder.roleSessionName(config.getRoleSessionName());
        }
        if (!isBlank(config.getWebIdentityTokenFile())) {
            builder.webIdentityTokenFile(Paths.get(config.getWebIdentityTokenFile()));
        }
        return builder.build();
    }

    private static AwsCredentialsProvider buildProfileProvider(S3DiagnosticConfig config) {
        ProfileCredentialsProvider.Builder builder = ProfileCredentialsProvider.builder();
        if (!isBlank(config.getProfileName())) {
            builder.profileName(config.getProfileName());
        }
        return builder.build();
    }

    private static boolean isSourceDetected(S3DiagnosticConfig config, CredentialSource source) {
        switch (source) {
            case STATIC:
                return config.hasStaticCredentials();
            case SYSTEM_PROPERTIES:
                return !isBlank(System.getProperty("aws.accessKeyId"))
                    && !isBlank(System.getProperty("aws.secretAccessKey"));
            case ENVIRONMENT:
                return !isBlank(System.getenv("AWS_ACCESS_KEY_ID"))
                    && !isBlank(System.getenv("AWS_SECRET_ACCESS_KEY"));
            case WEB_IDENTITY:
                String tokenFile = config.getAuthMode() == S3AuthMode.AUTO
                    ? firstNonBlank(
                        System.getProperty("aws.webIdentityTokenFile"),
                        System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE"),
                        null
                    )
                    : firstNonBlank(
                        config.getWebIdentityTokenFile(),
                        System.getProperty("aws.webIdentityTokenFile"),
                        System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
                    );
                String roleArn = config.getAuthMode() == S3AuthMode.AUTO
                    ? firstNonBlank(
                        System.getProperty("aws.roleArn"),
                        System.getenv("AWS_ROLE_ARN"),
                        null
                    )
                    : firstNonBlank(
                        config.getRoleArn(),
                        System.getProperty("aws.roleArn"),
                        System.getenv("AWS_ROLE_ARN")
                    );
                return !isBlank(tokenFile) && !isBlank(roleArn);
            case PROFILE:
                if (config.getAuthMode() != S3AuthMode.AUTO && !isBlank(config.getProfileName())) {
                    return true;
                }
                String userHome = System.getProperty("user.home");
                if (isBlank(userHome)) {
                    return false;
                }
                File credentialsFile = new File(userHome + "/.aws/credentials");
                File configFile = new File(userHome + "/.aws/config");
                return credentialsFile.exists() || configFile.exists();
            case CONTAINER:
                return !isBlank(System.getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"))
                    || !isBlank(System.getenv("AWS_CONTAINER_CREDENTIALS_FULL_URI"));
            case INSTANCE_PROFILE:
                return !"true".equalsIgnoreCase(System.getenv("AWS_EC2_METADATA_DISABLED"));
            default:
                return false;
        }
    }

    private static CredentialProbeResult probeCredentials(
        S3DiagnosticConfig config,
        ProviderCandidate candidate,
        boolean detected,
        int chainOrder
    ) {
        try {
            AwsCredentials credentials = candidate.provider.resolveCredentials();
            String maskedAccessKey = maskAccessKey(credentials.accessKeyId());
            CallerIdentity callerIdentity = fetchCallerIdentity(config, candidate.provider);

            String message = detected
                ? "Detected in environment and credentials resolved."
                : "No explicit env hint, but credentials resolved.";
            if (candidate.isAssumeRole()) {
                message = message + " AssumeRole target: " + candidate.assumeRoleArn
                    + ". Base source: " + candidate.source + ".";
            }
            if (callerIdentity.error != null) {
                message = message + " STS getCallerIdentity failed: " + callerIdentity.error;
            } else if (callerIdentity.arn != null) {
                message = message + " STS principal type: " + classifyStsPrincipal(callerIdentity.arn)
                    + ". Caller ARN: " + callerIdentity.arn;
            }

            return new CredentialProbeResult(
                candidate.source,
                chainOrder,
                false,
                detected,
                true,
                maskedAccessKey,
                callerIdentity.arn,
                callerIdentity.accountId,
                message
            );
        } catch (Exception ex) {
            String message = detected
                ? "Detected but resolve failed: " + conciseError(ex)
                : "Not detected or resolve failed: " + conciseError(ex);
            if (candidate.isAssumeRole()) {
                message = message + " AssumeRole target: " + candidate.assumeRoleArn
                    + ". Base source: " + candidate.source + ".";
            }
            return new CredentialProbeResult(
                candidate.source,
                chainOrder,
                false,
                detected,
                false,
                null,
                null,
                null,
                message
            );
        }
    }

    private static CallerIdentity fetchCallerIdentity(S3DiagnosticConfig config, AwsCredentialsProvider provider) {
        StsClient stsClient = null;
        try {
            ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofSeconds(config.getTimeoutSeconds()))
                .apiCallAttemptTimeout(Duration.ofSeconds(config.getTimeoutSeconds()))
                .build();

            stsClient = StsClient.builder()
                .region(Region.of(config.getRegion()))
                .credentialsProvider(provider)
                .overrideConfiguration(overrideConfiguration)
                .build();

            GetCallerIdentityResponse response = stsClient.getCallerIdentity();
            return new CallerIdentity(response.arn(), response.account(), null);
        } catch (Exception ex) {
            return new CallerIdentity(null, null, conciseError(ex));
        } finally {
            if (stsClient != null) {
                stsClient.close();
            }
        }
    }

    private static ProbeState findSelectedState(List<ProbeState> probeStates) {
        for (ProbeState state : probeStates) {
            if (state.probeResult.isUsable()) {
                return state;
            }
        }
        return null;
    }

    private static List<CredentialProbeResult> markSelectedCredentialResult(List<ProbeState> probeStates, ProbeState selectedState) {
        List<CredentialProbeResult> results = new ArrayList<>();
        for (ProbeState state : probeStates) {
            boolean selected = selectedState != null && selectedState.candidate.source == state.candidate.source;
            CredentialProbeResult original = state.probeResult;
            results.add(
                new CredentialProbeResult(
                    original.getSource(),
                    original.getChainOrder(),
                    selected,
                    original.isAvailable(),
                    original.isUsable(),
                    original.getAccessKeyIdMasked(),
                    original.getCallerArn(),
                    original.getCallerAccountId(),
                    original.getMessage()
                )
            );
        }
        return results;
    }

    private static List<PermissionCheckResult> runPermissionChecksForSource(S3DiagnosticConfig config, ProviderCandidate candidate) {
        List<PermissionCheckResult> results = new ArrayList<>();
        String putObjectKey = resolvePutObjectKey(config);
        try (S3Client s3Client = buildS3Client(config, candidate.provider)) {
            results.add(safePermissionCheck(
                new PermissionCheckSupplier() {
                    @Override
                    public PermissionCheckResult run() {
                        return checkHeadBucket(s3Client, config, candidate.source);
                    }
                },
                candidate.source,
                PermissionOperation.HEAD_BUCKET,
                buildBucketTarget(config)
            ));
            results.add(safePermissionCheck(
                new PermissionCheckSupplier() {
                    @Override
                    public PermissionCheckResult run() {
                        return checkListObjects(s3Client, config, candidate.source);
                    }
                },
                candidate.source,
                PermissionOperation.LIST_OBJECTS,
                buildListTarget(config)
            ));
            results.add(safePermissionCheck(
                new PermissionCheckSupplier() {
                    @Override
                    public PermissionCheckResult run() {
                        return checkPutObject(s3Client, config, candidate.source, putObjectKey);
                    }
                },
                candidate.source,
                PermissionOperation.PUT_OBJECT,
                buildPutTarget(config, putObjectKey)
            ));
            return results;
        } catch (Exception ex) {
            String error = "Failed to initialize S3 client: " + conciseError(ex);
            results.add(new PermissionCheckResult(
                candidate.source,
                PermissionOperation.HEAD_BUCKET,
                false,
                buildBucketTarget(config),
                null,
                null,
                error
            ));
            results.add(new PermissionCheckResult(
                candidate.source,
                PermissionOperation.LIST_OBJECTS,
                false,
                buildListTarget(config),
                null,
                null,
                error
            ));
            results.add(new PermissionCheckResult(
                candidate.source,
                PermissionOperation.PUT_OBJECT,
                false,
                buildPutTarget(config, putObjectKey),
                null,
                null,
                error
            ));
            return results;
        }
    }

    private static List<PermissionCheckResult> buildSkippedPermissionChecks(
        CredentialSource source,
        S3DiagnosticConfig config,
        String reason
    ) {
        List<PermissionCheckResult> results = new ArrayList<>();
        String putObjectKey = resolvePutObjectKey(config);
        results.add(new PermissionCheckResult(
            source,
            PermissionOperation.HEAD_BUCKET,
            null,
            buildBucketTarget(config),
            null,
            null,
            reason
        ));
        results.add(new PermissionCheckResult(
            source,
            PermissionOperation.LIST_OBJECTS,
            null,
            buildListTarget(config),
            null,
            null,
            reason
        ));
        results.add(new PermissionCheckResult(
            source,
            PermissionOperation.PUT_OBJECT,
            null,
            buildPutTarget(config, putObjectKey),
            null,
            null,
            reason
        ));
        return results;
    }

    private static S3Client buildS3Client(S3DiagnosticConfig config, AwsCredentialsProvider provider) {
        ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
            .apiCallTimeout(Duration.ofSeconds(config.getTimeoutSeconds()))
            .apiCallAttemptTimeout(Duration.ofSeconds(config.getTimeoutSeconds()))
            .build();

        S3ClientBuilder builder = S3Client.builder()
            .region(Region.of(config.getRegion()))
            .credentialsProvider(provider)
            .overrideConfiguration(overrideConfiguration)
            .serviceConfiguration(
                S3Configuration.builder()
                    .pathStyleAccessEnabled(config.isPathStyle())
                    .build()
            );

        if (!isBlank(config.getEndpoint())) {
            builder.endpointOverride(URI.create(config.getEndpoint()));
        }
        return builder.build();
    }

    private static PermissionCheckResult checkHeadBucket(
        S3Client client,
        S3DiagnosticConfig config,
        CredentialSource source
    ) {
        String target = buildBucketTarget(config);
        try {
            HeadBucketResponse response = client.headBucket(
                HeadBucketRequest.builder().bucket(config.getBucket()).build()
            );
            return new PermissionCheckResult(
                source,
                PermissionOperation.HEAD_BUCKET,
                true,
                target,
                response.sdkHttpResponse().statusCode(),
                null,
                "HeadBucket succeeded."
            );
        } catch (S3Exception ex) {
            return fromS3Exception(source, PermissionOperation.HEAD_BUCKET, target, ex);
        } catch (SdkException ex) {
            return new PermissionCheckResult(
                source,
                PermissionOperation.HEAD_BUCKET,
                false,
                target,
                null,
                null,
                conciseError(ex)
            );
        }
    }

    private static PermissionCheckResult checkListObjects(
        S3Client client,
        S3DiagnosticConfig config,
        CredentialSource source
    ) {
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
            String message = "ListObjectsV2 succeeded. keyCount=" + (keyCount == null ? 0 : keyCount);
            return new PermissionCheckResult(
                source,
                PermissionOperation.LIST_OBJECTS,
                true,
                target,
                response.sdkHttpResponse().statusCode(),
                null,
                message
            );
        } catch (S3Exception ex) {
            return fromS3Exception(source, PermissionOperation.LIST_OBJECTS, target, ex);
        } catch (SdkException ex) {
            return new PermissionCheckResult(
                source,
                PermissionOperation.LIST_OBJECTS,
                false,
                target,
                null,
                null,
                conciseError(ex)
            );
        }
    }

    private static PermissionCheckResult checkPutObject(
        S3Client client,
        S3DiagnosticConfig config,
        CredentialSource source,
        String key
    ) {
        String target = buildPutTarget(config, key);
        try {
            PutObjectResponse response = client.putObject(
                PutObjectRequest.builder()
                    .bucket(config.getBucket())
                    .key(key)
                    .contentType("text/plain")
                    .build(),
                RequestBody.fromString("Pulse S3 connectivity probe at " + Instant.now().toString())
            );

            String cleanupMessage = "cleanup skipped";
            try {
                client.deleteObject(DeleteObjectRequest.builder().bucket(config.getBucket()).key(key).build());
                cleanupMessage = "cleanup deleted";
            } catch (Exception cleanupEx) {
                cleanupMessage = "cleanup failed: " + conciseError(cleanupEx);
            }

            return new PermissionCheckResult(
                source,
                PermissionOperation.PUT_OBJECT,
                true,
                target,
                response.sdkHttpResponse().statusCode(),
                null,
                "PutObject succeeded, " + cleanupMessage + "."
            );
        } catch (S3Exception ex) {
            return fromS3Exception(source, PermissionOperation.PUT_OBJECT, target, ex);
        } catch (SdkException ex) {
            return new PermissionCheckResult(
                source,
                PermissionOperation.PUT_OBJECT,
                false,
                target,
                null,
                null,
                conciseError(ex)
            );
        }
    }

    private static PermissionCheckResult safePermissionCheck(
        PermissionCheckSupplier permissionCheckSupplier,
        CredentialSource source,
        PermissionOperation operation,
        String target
    ) {
        try {
            return permissionCheckSupplier.run();
        } catch (Exception ex) {
            return new PermissionCheckResult(
                source,
                operation,
                false,
                target,
                null,
                null,
                "Unexpected permission-check error: " + conciseError(ex)
            );
        }
    }

    private static PermissionCheckResult fromS3Exception(
        CredentialSource source,
        PermissionOperation operation,
        String target,
        S3Exception ex
    ) {
        String errorCode = ex.awsErrorDetails() != null ? ex.awsErrorDetails().errorCode() : null;
        Integer statusCode = ex.statusCode();
        String message = conciseError(ex);
        return new PermissionCheckResult(source, operation, false, target, statusCode, errorCode, message);
    }

    private static String resolvePutObjectKey(S3DiagnosticConfig config) {
        if (!isBlank(config.getPutObjectKey())) {
            return config.getPutObjectKey();
        }

        String prefix = config.getPrefix();
        String fileName = "pulse-connectivity-check-" + System.currentTimeMillis() + ".txt";
        if (isBlank(prefix)) {
            return fileName;
        }
        if (prefix.endsWith("/")) {
            return prefix + fileName;
        }
        return prefix + "/" + fileName;
    }

    private static String buildBucketTarget(S3DiagnosticConfig config) {
        return "s3://" + config.getBucket();
    }

    private static String buildListTarget(S3DiagnosticConfig config) {
        return "s3://" + config.getListBucket() + (isBlank(config.getListPrefix()) ? "" : "/" + config.getListPrefix());
    }

    private static String buildPutTarget(S3DiagnosticConfig config, String key) {
        return "s3://" + config.getBucket() + "/" + key;
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

    private static void printBanner() {
        System.out.println("============================================================");
        System.out.println(" Pulse S3 Connectivity Diagnostic Tool");
        System.out.println("============================================================");
    }

    private static void printReport(S3ConnectivityReport report) {
        System.out.println();
        System.out.println("[Target]");
        System.out.println("  region      : " + report.getRegion());
        System.out.println("  endpoint    : " + (isBlank(report.getEndpoint()) ? "(AWS default)" : report.getEndpoint()));
        System.out.println("  bucket      : " + report.getBucket());
        System.out.println("  prefix      : " + (isBlank(report.getPrefix()) ? "(root)" : report.getPrefix()));
        System.out.println("  lsPath      : " + safe(findAnyListTarget(report)));

        System.out.println();
        System.out.println("[DefaultChain Effective Credential]");
        System.out.println("  source      : " + safe(report.getSelectedCredentialSource()));
        System.out.println("  accessKeyId : " + safe(report.getSelectedAccessKeyMasked()));
        System.out.println("  callerArn   : " + safe(report.getSelectedCallerArn()));
        System.out.println("  stsType     : " + classifyStsPrincipal(report.getSelectedCallerArn()));

        System.out.println();
        System.out.println("[Credential By Credential Analysis]");
        for (CredentialProbeResult credentialProbeResult : report.getCredentialProbeResults()) {
            System.out.println(
                "  - #" + credentialProbeResult.getChainOrder() + " " + credentialProbeResult.getSource()
                    + (credentialProbeResult.isSelected() ? " [DEFAULT_CHAIN_EFFECTIVE]" : "")
            );
            System.out.println("    detected      : " + credentialProbeResult.isAvailable());
            System.out.println("    usable        : " + credentialProbeResult.isUsable());
            System.out.println("    accessKeyId   : " + safe(credentialProbeResult.getAccessKeyIdMasked()));
            System.out.println("    callerArn     : " + safe(credentialProbeResult.getCallerArn()));
            System.out.println("    stsType       : " + classifyStsPrincipal(credentialProbeResult.getCallerArn()));
            System.out.println("    accountId     : " + safe(credentialProbeResult.getCallerAccountId()));
            System.out.println("    detail        : " + safe(credentialProbeResult.getMessage()));

            List<PermissionCheckResult> sourcePermissionResults = permissionResultsForSource(
                report.getPermissionCheckResults(),
                credentialProbeResult.getSource()
            );
            for (PermissionCheckResult permissionCheckResult : sourcePermissionResults) {
                System.out.println("    * " + permissionCheckResult.getOperation());
                System.out.println("      allowed     : " + formatAllowed(permissionCheckResult.getAllowed()));
                System.out.println("      target      : " + safe(permissionCheckResult.getTarget()));
                System.out.println("      statusCode  : " + safe(permissionCheckResult.getStatusCode()));
                System.out.println("      errorCode   : " + safe(permissionCheckResult.getErrorCode()));
                System.out.println("      detail      : " + safe(permissionCheckResult.getMessage()));
            }
        }

        printIamRoleSummary(report);

        System.out.println();
        if (report.getSelectedCredentialSource() == null) {
            System.out.println("[Result] FAILED - no usable credentials detected in chain.");
            return;
        }

        boolean selectedHasDenied = false;
        for (PermissionCheckResult permissionCheckResult : report.getPermissionCheckResults()) {
            if (permissionCheckResult.getSource() == report.getSelectedCredentialSource()
                && Boolean.FALSE.equals(permissionCheckResult.getAllowed())) {
                selectedHasDenied = true;
                break;
            }
        }

        if (selectedHasDenied) {
            System.out.println("[Result] FAILED - default-chain effective credential has missing permissions.");
        } else {
            System.out.println("[Result] PASSED - default-chain effective credential passed HeadBucket/List/Put.");
        }
    }

    private static List<PermissionCheckResult> permissionResultsForSource(
        List<PermissionCheckResult> allResults,
        CredentialSource source
    ) {
        if (allResults == null || allResults.isEmpty()) {
            return Collections.emptyList();
        }
        List<PermissionCheckResult> filtered = new ArrayList<>();
        for (PermissionCheckResult result : allResults) {
            if (result.getSource() == source) {
                filtered.add(result);
            }
        }
        return filtered;
    }

    private static String formatAllowed(Boolean allowed) {
        if (allowed == null) {
            return "SKIPPED";
        }
        return String.valueOf(allowed);
    }

    private static void printIamRoleSummary(S3ConnectivityReport report) {
        List<CredentialSource> detectedSources = new ArrayList<>();
        List<CredentialSource> usableSources = new ArrayList<>();
        List<CredentialSource> usableRoleSources = new ArrayList<>();

        for (CredentialProbeResult probeResult : report.getCredentialProbeResults()) {
            if (probeResult.isAvailable()) {
                detectedSources.add(probeResult.getSource());
            }
            if (probeResult.isUsable()) {
                usableSources.add(probeResult.getSource());
                if (isRoleBasedIdentity(probeResult)) {
                    usableRoleSources.add(probeResult.getSource());
                }
            }
        }

        PermissionCheckResult selectedListPermission = findPermissionResult(
            report.getPermissionCheckResults(),
            report.getSelectedCredentialSource(),
            PermissionOperation.LIST_OBJECTS
        );

        System.out.println();
        System.out.println("[IAM Role / STS Summary]");
        System.out.println("  detectedSources      : " + formatSourceList(detectedSources));
        System.out.println("  usableSources        : " + formatSourceList(usableSources));
        System.out.println("  usableRoleSources    : " + formatSourceList(usableRoleSources));
        System.out.println("  selectedSource       : " + safe(report.getSelectedCredentialSource()));
        System.out.println("  selectedStsType      : " + classifyStsPrincipal(report.getSelectedCallerArn()));
        System.out.println("  selectedLsTarget     : " + safe(
            selectedListPermission == null ? findAnyListTarget(report) : selectedListPermission.getTarget()
        ));
        System.out.println("  selectedLsAllowed    : " + (
            selectedListPermission == null ? "N/A" : formatAllowed(selectedListPermission.getAllowed())
        ));
        System.out.println("  selectedLsDetail     : " + safe(
            selectedListPermission == null ? null : selectedListPermission.getMessage()
        ));
    }

    private static boolean isRoleBasedIdentity(CredentialProbeResult probeResult) {
        String principalType = classifyStsPrincipal(probeResult.getCallerArn());
        if ("ASSUMED_ROLE".equals(principalType) || "IAM_ROLE".equals(principalType)) {
            return true;
        }

        if (probeResult.getCallerArn() == null) {
            return probeResult.getSource() == CredentialSource.INSTANCE_PROFILE
                || probeResult.getSource() == CredentialSource.WEB_IDENTITY
                || probeResult.getSource() == CredentialSource.CONTAINER;
        }

        return false;
    }

    private static PermissionCheckResult findPermissionResult(
        List<PermissionCheckResult> allResults,
        CredentialSource source,
        PermissionOperation operation
    ) {
        if (allResults == null || source == null || operation == null) {
            return null;
        }
        for (PermissionCheckResult result : allResults) {
            if (result.getSource() == source && result.getOperation() == operation) {
                return result;
            }
        }
        return null;
    }

    private static String findAnyListTarget(S3ConnectivityReport report) {
        if (report == null || report.getPermissionCheckResults() == null) {
            return null;
        }
        for (PermissionCheckResult result : report.getPermissionCheckResults()) {
            if (result.getOperation() == PermissionOperation.LIST_OBJECTS) {
                return result.getTarget();
            }
        }
        return null;
    }

    private static String formatSourceList(List<CredentialSource> sources) {
        if (sources == null || sources.isEmpty()) {
            return "(none)";
        }

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < sources.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(sources.get(i));
        }
        return builder.toString();
    }

    private static String safe(Object value) {
        return value == null ? "N/A" : String.valueOf(value);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String firstNonBlank(String first, String second, String third) {
        if (!isBlank(first)) {
            return first;
        }
        if (!isBlank(second)) {
            return second;
        }
        if (!isBlank(third)) {
            return third;
        }
        return null;
    }

    private static String conciseError(Throwable throwable) {
        String message = throwable.getMessage();
        String className = throwable.getClass().getSimpleName();
        if (isBlank(message)) {
            return className;
        }
        return className + ": " + message;
    }

    private static String classifyStsPrincipal(String callerArn) {
        if (isBlank(callerArn)) {
            return "UNKNOWN";
        }

        String normalized = callerArn.toLowerCase();
        if (normalized.contains(":assumed-role/")) {
            return "ASSUMED_ROLE";
        }
        if (normalized.contains(":role/")) {
            return "IAM_ROLE";
        }
        if (normalized.contains(":user/")) {
            return "IAM_USER";
        }
        if (normalized.contains(":federated-user/")) {
            return "FEDERATED_USER";
        }
        if (normalized.endsWith(":root")) {
            return "ROOT";
        }
        return "UNKNOWN";
    }

    private static class ProviderCandidate {
        private final CredentialSource source;
        private final AwsCredentialsProvider provider;
        private final String assumeRoleArn;

        private ProviderCandidate(CredentialSource source, AwsCredentialsProvider provider) {
            this(source, provider, null);
        }

        private ProviderCandidate(CredentialSource source, AwsCredentialsProvider provider, String assumeRoleArn) {
            this.source = source;
            this.provider = provider;
            this.assumeRoleArn = assumeRoleArn;
        }

        private boolean isAssumeRole() {
            return !isBlank(assumeRoleArn);
        }
    }

    private interface PermissionCheckSupplier {
        PermissionCheckResult run();
    }

    private static class ProbeState {
        private final ProviderCandidate candidate;
        private final CredentialProbeResult probeResult;

        private ProbeState(ProviderCandidate candidate, CredentialProbeResult probeResult) {
            this.candidate = candidate;
            this.probeResult = probeResult;
        }
    }

    private static class CallerIdentity {
        private final String arn;
        private final String accountId;
        private final String error;

        private CallerIdentity(String arn, String accountId, String error) {
            this.arn = arn;
            this.accountId = accountId;
            this.error = error;
        }
    }
}
