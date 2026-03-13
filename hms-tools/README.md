# HMS Tools

> Part of the [Pulse](../README.md) connectivity testing toolkit

**Hive Metastore Diagnostics**

`hms-tools` is a standalone CLI for checking Hive Metastore reachability, authentication, metadata RPC health, object visibility, and latency.

It is designed for troubleshooting first, performance measurement second.

## Commands

- `check`: end-to-end HMS health check
- `ping`: repeated per-URI connectivity and lightweight RPC probe
- `object`: inspect a database, table, or partition
- `bench`: measure latency distribution for selected HMS RPCs
- `config`: print effective runtime configuration
- `version`: print tool and dependency versions

## Build

```bash
mvn -pl hms-tools -am -DskipTests package
```

Artifact:

```bash
hms-tools/target/native-hms-tools-jar-with-dependencies.jar
```

## Quick Start

Show global help:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar --help
```

Show command help:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar help check
```

Basic health check:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar \
  check \
  --uris thrift://hms1:9083,thrift://hms2:9083
```

Quick probe with JSON output:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar \
  ping \
  --uris thrift://hms1:9083,thrift://hms2:9083 \
  --count 3 \
  --format json
```

Simple authentication:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar \
  check \
  --uris thrift://10.20.30.4:30616 \
  --auth simple \
  --simple-user hadoop
```

Kerberos with Hadoop config directory:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar \
  check \
  --uris thrift://hms-host:9083 \
  --conf-dir /etc/hadoop/conf \
  --auth kerberos \
  --principal hive/hms-host@EXAMPLE.COM \
  --keytab /etc/security/keytabs/hive.service.keytab
```

Inspect an object:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar \
  object \
  --uris thrift://hms-host:9083 \
  --db tpch \
  --table lineitem \
  --check schema
```

Measure latency:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar \
  bench \
  --uris thrift://hms-host:9083 \
  --rpc get_table \
  --db tpch \
  --table lineitem \
  --iterations 50 \
  --concurrency 8
```

Dump effective config:

```bash
java -jar hms-tools/target/native-hms-tools-jar-with-dependencies.jar \
  config \
  --uris thrift://hms-host:9083 \
  --conf-dir /etc/hadoop/conf
```

## Global Options

- `--uris <thrift://host:port[,..]>`: HMS thrift URIs
- `--conf-dir <dir>`: load `core-site.xml`, `hdfs-site.xml`, and `hive-site.xml`
- `--resource <file>`: add extra Hadoop or Hive XML resource, repeatable
- `--conf <k=v>`: add runtime config overrides, repeatable
- `--auth <auto|simple|kerberos>`: authentication mode, default `auto`
- `--simple-user <user>`: simple-auth identity
- `--principal <principal>`: Kerberos principal
- `--keytab <path>`: Kerberos keytab
- `--timeout <sec>`: connect and RPC timeout, default `10`
- `--no-retry`: disable metastore client retries
- `--format <text|json>`: output format, default `text`
- `--verbose`: print stage details

## Command Notes

- `check` defaults to a lightweight `get_all_databases` RPC and can optionally validate a specific `--db` or `--table`.
- `ping` runs repeated probes and reports each attempt per URI.
- `object` supports `exists`, `schema`, `partitions`, and `stats`.
- `bench` supports `get_table`, `list_partitions`, `get_all_databases`, and `get_database`.
- `bench --limit` fails fast when the value exceeds the Hive metastore API limit instead of silently truncating it.

## Output

- `text`: human-readable operator output
- `json`: structured output for scripts and automation

Current text output is stage-oriented and shows:

- command status
- auth result
- config summary
- per-URI connect / RPC stages
- final summary and exit code

## Exit Codes

- `0`: all checks passed
- `10`: invalid arguments
- `12`: authentication failure
- `13`: partial URI failure
- `14`: all URIs unreachable
- `15`: basic RPC failure
- `16`: object not found
- `17`: permission denied
- `20`: benchmark threshold exceeded

## Notes

- This tool does not depend on Doris at runtime.
- Use `config` before `check` when you need to confirm which files and overrides were actually loaded.
- Use `--no-retry` when you want first-failure behavior instead of client-side failover masking the issue.
