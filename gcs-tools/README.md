# GCS Tools

> Part of the [Pulse](../README.md) connectivity testing toolkit

**Native GCS Connectivity Diagnostic Tool**

This module validates Google Cloud Storage connectivity through the XML API using HMAC credentials (`ak` / `sk`).

## What It Checks

- Builds an S3-compatible client against the GCS XML API endpoint
- Verifies bucket reachability with `HeadBucket`
- Verifies prefix visibility with `ListObjectsV2`
- Optionally verifies write permission with `PutObject` followed by cleanup delete
- Prints masked HMAC access key, effective target path, HTTP status code, and service error code

## Usage

```bash
# Build
mvn -pl gcs-tools -am clean package

# Run from a working directory containing config.properties
java -jar native-gcs-tools-jar-with-dependencies.jar
```

## Configuration

Create `config.properties` in the same directory as the JAR:

```properties
gcsPath=gs://your-bucket/your-prefix
ak=your-hmac-access-key
sk=your-hmac-secret-key

# Optional
# endpoint=https://storage.googleapis.com
# region=auto
# lsPath=gs://your-bucket/another-prefix
# pathStyle=true
# timeoutSeconds=10
# writeCheckEnabled=true
# putObjectKey=your-prefix/pulse-gcs-connectivity-check.txt
```

## Configuration Reference

| Property | Required | Description |
|----------|----------|-------------|
| `gcsPath` | Yes* | Target path in `gs://bucket/prefix` form |
| `bucket` | Yes* | Bucket name, used when `gcsPath` is not set |
| `prefix` | No | Prefix under the bucket, used when `gcsPath` is not set |
| `ak` | Yes** | HMAC access key id |
| `sk` | Yes** | HMAC secret key |
| `accessKeyId` | Yes** | Alias of `ak` |
| `secretAccessKey` | Yes** | Alias of `sk` |
| `endpoint` | No | XML API endpoint, default `https://storage.googleapis.com` |
| `region` | No | Signing region, default `auto` |
| `lsPath` | No | Override target used by the list check |
| `pathStyle` | No | Path-style access, default `true` |
| `timeoutSeconds` | No | API call timeout in seconds, default `10` |
| `writeCheckEnabled` | No | Whether to perform upload/delete validation, default `false` |
| `putObjectKey` | No | Custom object key used by the write check |

\* Configure either `gcsPath`, or `bucket` with optional `prefix`.

\** Configure either `ak/sk`, or `accessKeyId/secretAccessKey`.

## Sample Output

```text
[Target]
  endpoint          : https://storage.googleapis.com
  region            : auto
  bucket            : your-bucket
  prefix            : your-prefix
  lsPath            : gs://your-bucket/your-prefix
  pathStyle         : true
  writeCheckEnabled : false

[Authentication]
  mode              : HMAC_STATIC
  accessKeyId       : GOOG...AB

[Checks]
  * HEAD_BUCKET
    allowed     : true
    target      : gs://your-bucket
    statusCode  : 200
    errorCode   : N/A
    detail      : HeadBucket succeeded.
```

## Notes

- This tool is for GCS HMAC interoperability credentials, not JSON service-account keys.
- The default endpoint is GCS XML API: `https://storage.googleapis.com`.
- `pathStyle=true` is enabled by default to avoid hostname-style bucket compatibility issues.

## License

MIT License - see [LICENSE](../LICENSE) for details.
