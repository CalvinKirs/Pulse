# Pulse

**Lightweight Connectivity Testing Toolkit**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Java](https://img.shields.io/badge/Java-8%2B-orange.svg)](https://www.oracle.com/java/)
[![C++](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://isocpp.org/)

Pulse is a repo of small connectivity diagnostics for infrastructure dependencies such as Kerberos, metastore, and object storage.
It currently contains two implementation tracks:

- **Java tools**: Maven-managed diagnostic JARs for Kerberos, HMS, S3, and GCS
- **C++ tool**: a standalone Azure Blob client built with CMake/vcpkg

This distinction should be explicit in the README. The build, packaging, and runtime model are different enough that users should not have to infer it from the directory names.

## Design Goals

- **Lightweight**: runnable artifacts with minimal setup
- **Portable**: easy to copy into target environments for diagnosis
- **Actionable**: output focuses on concrete failure points and follow-up fixes
- **Focused**: each tool checks one dependency family well instead of becoming a general platform

## Tool Matrix

| Tool | Target | Language / Runtime | Build Path | Artifact | Main Checks |
|------|--------|--------------------|------------|----------|-------------|
| [kerberos-tools](./kerberos-tools) | Kerberos / KDC | Java 8+ | Maven reactor | fat JAR | `krb5.conf`, KDC reachability, keytab inspection, login test |
| [hms-tools](./hms-tools) | Hive Metastore | Java 8+ | Maven reactor | fat JAR | `get_table`, `list_partitions`, `get_all_databases`, latency distribution |
| [s3-tools](./s3-tools) | S3-compatible storage | Java 8+ | Maven reactor | fat JAR | credential-source probing, STS identity, bucket/list/put checks |
| [gcs-tools](./gcs-tools) | GCS XML API | Java 8+ | Maven reactor | fat JAR | HMAC auth, bucket/list, optional write/delete checks |
| [azure-blob-cpp](./azure-blob-cpp) | Azure Blob Storage | C++17 | standalone CMake project | native binary | container reachability, optional upload/read/delete validation |

## Repository Layout

```text
Pulse/
├── pom.xml                    # Maven reactor for Java modules only
├── common/                    # Shared Java utilities
├── kerberos-tools/            # Java Kerberos diagnostics
├── hms-tools/                 # Java Hive Metastore latency diagnostics
├── s3-tools/                  # Java S3 diagnostics
├── gcs-tools/                 # Java GCS XML API diagnostics
├── azure-blob-cpp/            # Standalone C++ Azure Blob client
└── LICENSE
```

## Java vs. C++

### Java Track

The Java tools are the main Pulse toolkit. They are built from the root `pom.xml` and produce standalone JARs under each module's `target/` directory.

```bash
mvn clean package
java -jar gcs-tools/target/native-gcs-tools-jar-with-dependencies.jar
```

### C++ Track

`azure-blob-cpp` is intentionally separate from the Maven reactor. It uses the Azure SDK for C++, CMake, and `vcpkg`, and produces a native executable rather than a JAR.

```bash
cd azure-blob-cpp
./build_linux.sh
./build/azure_blob_connectivity --mode basic ...
```

If this split is not documented, users will reasonably assume every module is a Java JAR, which is no longer true.

## Quick Start

### Run a Java Tool

1. Build from the repo root with `mvn clean package`
2. Go to the target module's `target/` directory
3. Follow the target module's README for configuration style
4. Run the module-specific JAR

Example:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar --help
```

### Run the Azure Blob Tool

See [`azure-blob-cpp/README.md`](./azure-blob-cpp/README.md) for platform-specific build instructions and runtime arguments.

## Adding New Tools

For a new Java-based diagnostic:

1. Create a new module directory
2. Add the module to the root [`pom.xml`](./pom.xml)
3. Reuse `common` for shared logic where appropriate
4. Package the tool as a standalone JAR

For a new native tool:

1. Keep it as a clearly separate project directory
2. Document its runtime/build chain in its own README
3. Add it to the root matrix so users can see it is not part of the Java reactor

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
