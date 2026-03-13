# HMS Tools

> Part of the [Pulse](../README.md) connectivity testing toolkit

**Hive Metastore Latency Probe**

A standalone diagnostic utility for measuring Hive Metastore RPC latency from the target environment. It is useful for Doris external catalog troubleshooting, Hive planning delays, and general metastore reachability and authentication checks.

## Features

- `get_table` latency probe
- `list_partitions` latency probe
- `get_all_databases` latency probe
- latency summary for connect, RPC, and end-to-end time
- simple auth and Kerberos support
- support for `hive-site.xml`, `core-site.xml`, `hdfs-site.xml`
- repeatable `--conf` and `--resource` overrides

## Build

```bash
mvn -pl hms-tools -am -DskipTests package
```

Artifact:

```bash
hms-tools/target/native-hms-tools-jar-with-dependencies.jar
```

## Usage

Show help:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar --help
```

Simple authentication:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar \
  --uris thrift://10.20.30.4:30616 \
  --simple-user hadoop \
  --db tpch \
  --table lineitem \
  --iterations 50 \
  --concurrency 8
```

Kerberos with Hadoop config directory:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar \
  --uris thrift://hms-host:9083 \
  --conf-dir /etc/hadoop/conf \
  --kerberos-principal hive/_HOST@EXAMPLE.COM \
  --keytab /etc/security/keytabs/hive.service.keytab \
  --db tpch \
  --table lineitem
```

Only test a lightweight RPC:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar \
  --uris thrift://10.20.30.4:30616 \
  --operation get_all_databases \
  --iterations 20
```

## Notes

- This tool does not depend on Doris at runtime.
- Use `--conf-dir` to load `core-site.xml`, `hdfs-site.xml`, and `hive-site.xml`.
- Use repeated `--conf key=value` arguments for extra Hadoop or Hive overrides.
- Exit code is `0` when all measured requests succeed, otherwise `2`.
