# S3 Tools

> Part of the [Pulse](../README.md) connectivity testing toolkit

**Native S3 Connectivity Diagnostic Tool**

This module validates S3 connectivity with explicit credential-source probing.

## What It Checks

- Detects which credential source is actually used
- Probes multiple sources such as `web_identity`, `instance_profile`, `profile`, and `env`
- Prints masked access key id and STS caller identity for each probe
- Classifies STS principal type (`ASSUMED_ROLE` / `IAM_USER` / etc.) to highlight IAM role sessions
- In `authMode=auto`, evaluates pure default-chain order (no credential config required) and marks which one is effective
- Prints IAM-role/STS summary with detected sources, usable sources, and role-based usable sources
- Verifies path-level permissions for each credential source (one-by-one):
  - `HeadBucket`
  - `ListObjectsV2` on `lsPath` (if configured) or target prefix
  - `PutObject` on target prefix
- Continues probing all credentials even when one credential/provider check fails

## Usage

```bash
# Build
mvn -pl s3-tools -am clean package

# Run from a working directory containing config.properties
java -jar native-s3-tools-jar-with-dependencies.jar
```

## Configuration

Create `config.properties` in the same directory as the JAR:

```properties
s3Path=s3://your-bucket/your/prefix
region=us-east-1
authMode=auto
# Optional: override ListObjectsV2 check target only
# lsPath=s3://your-bucket/another/prefix
```

For default-chain analysis, keep `authMode=auto`. In this mode, static/profile/web-identity override fields are ignored.
Exception: if `roleArn` is configured, the tool switches to explicit STS base-source probing and tries each source separately (`system`, `env`, `profile`, `container`, `instance_profile`) before `STS AssumeRole`.

## authMode Values

| Value | Meaning |
|-------|---------|
| `auto` | Default chain: system, env, web identity, profile, container, instance profile (if `roleArn` is set, auto switches to explicit STS source-by-source probing: system/env/profile/container/instance_profile, each then `AssumeRole`) |
| `static` | Use `accessKeyId` + `secretAccessKey` (+ optional `sessionToken`) |
| `web_identity` | Use OIDC token file + role |
| `instance_profile` | Use EC2 instance metadata credentials |
| `profile` | Use `~/.aws/credentials` profile |
| `env` | Use environment variables |
| `system_properties` | Use JVM system properties |
| `container` | Use ECS container credentials endpoint |

## Output Highlights

The tool prints:

- effective default-chain credential (`[DEFAULT_CHAIN_EFFECTIVE]`)
- each probe status (`detected=true/false`, `usable=true/false`)
- STS principal type for selected and each probe source
- caller identity (`arn`, `accountId`) when available
- AssumeRole target in probe detail when `roleArn` is configured
- IAM role / STS summary block (`[IAM Role / STS Summary]`)
- permission result per operation for every credential source (`allowed=true/false/SKIPPED`)

## Model Classes

Core models are in:

- `s3-tools/src/main/java/io/ck/pulse/s3/model/CredentialProbeResult.java`
- `s3-tools/src/main/java/io/ck/pulse/s3/model/PermissionCheckResult.java`
- `s3-tools/src/main/java/io/ck/pulse/s3/model/S3ConnectivityReport.java`
