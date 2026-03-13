/*
 * Copyright (c) 2026 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */

package io.ck.pulse.hms;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class HmsLatencyProbe {

    private static final HiveMetaHookLoader DUMMY_HOOK_LOADER = table -> null;
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.000");

    public static void main(String[] args) throws Exception {
        Options options = buildOptions();
        if (containsHelp(args)) {
            printUsage(options, System.out);
            return;
        }

        try {
            CommandLine commandLine = parseCommandLine(options, args);
            ProbeConfig config = ProbeConfig.from(commandLine);
            HiveConf hiveConf = buildHiveConf(config);
            UserGroupInformation.setConfiguration(hiveConf);
            UserGroupInformation ugi = buildUserGroupInformation(config);

            printBanner(config, hiveConf);
            runWarmup(config, hiveConf, ugi);
            ProbeSummary summary = runMeasured(config, hiveConf, ugi);
            summary.print();

            if (summary.failures > 0) {
                System.exit(2);
            }
        } catch (Exception exception) {
            System.err.println("Error: " + Objects.toString(exception.getMessage(), exception.getClass().getSimpleName()));
            System.exit(1);
        }
    }

    private static boolean containsHelp(String[] args) {
        for (String arg : args) {
            if ("--help".equals(arg) || "-h".equals(arg)) {
                return true;
            }
        }
        return false;
    }

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("help").desc("Print help").build());
        options.addOption(Option.builder().longOpt("uris").hasArg().argName("thrift://host:port[,..]")
            .required().desc("Hive metastore thrift URIs").build());
        options.addOption(Option.builder().longOpt("operation").hasArg().argName("name")
            .desc("Probe operation: get_table, list_partitions, get_all_databases. Default: get_table").build());
        options.addOption(Option.builder().longOpt("db").hasArg().argName("db")
            .desc("Database name for get_table/list_partitions").build());
        options.addOption(Option.builder().longOpt("table").hasArg().argName("table")
            .desc("Table name for get_table/list_partitions").build());
        options.addOption(Option.builder().longOpt("partitions-limit").hasArg().argName("N")
            .desc("Limit for list_partitions. Default: 1000").build());
        options.addOption(Option.builder().longOpt("warmup").hasArg().argName("N")
            .desc("Warmup requests before measurement. Default: 3").build());
        options.addOption(Option.builder().longOpt("iterations").hasArg().argName("N")
            .desc("Measured requests. Default: 20").build());
        options.addOption(Option.builder().longOpt("concurrency").hasArg().argName("N")
            .desc("Concurrent measured requests. Default: 1").build());
        options.addOption(Option.builder().longOpt("conf-dir").hasArg().argName("dir")
            .desc("Directory containing core-site.xml, hdfs-site.xml, hive-site.xml").build());
        options.addOption(Option.builder().longOpt("resource").hasArg().argName("file")
            .desc("Extra Hadoop/Hive XML resource. Repeatable").build());
        options.addOption(Option.builder().longOpt("conf").hasArg().argName("k=v")
            .desc("Additional Hive or Hadoop conf. Repeatable").build());
        options.addOption(Option.builder().longOpt("simple-user").hasArg().argName("user")
            .desc("Run RPCs as the specified simple-auth user").build());
        options.addOption(Option.builder().longOpt("kerberos-principal").hasArg().argName("principal")
            .desc("Kerberos principal. If omitted, use the current ticket cache").build());
        options.addOption(Option.builder().longOpt("keytab").hasArg().argName("path")
            .desc("Keytab for Kerberos login").build());
        options.addOption(Option.builder().longOpt("connect-timeout").hasArg().argName("seconds")
            .desc("Override hive.metastore.client.socket.timeout").build());
        options.addOption(Option.builder().longOpt("verbose")
            .desc("Print per-request latency").build());
        return options;
    }

    private static CommandLine parseCommandLine(Options options, String[] args) throws ParseException {
        try {
            return new DefaultParser().parse(options, args);
        } catch (ParseException parseException) {
            printUsage(options, System.err);
            throw parseException;
        }
    }

    private static void printUsage(Options options, java.io.PrintStream out) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter writer = new PrintWriter(out, true);
        formatter.printHelp(writer, 120,
            "java -jar native-hms-tools-jar-with-dependencies.jar "
                + "--uris thrift://127.0.0.1:9083 --db db1 --table tbl1 [options]",
            null, options, 2, 4,
            "Example: java -jar native-hms-tools-jar-with-dependencies.jar "
                + "--uris thrift://10.20.30.4:30616 --db tpch --table lineitem "
                + "--iterations 50 --concurrency 8",
            true);
        writer.flush();
    }

    private static HiveConf buildHiveConf(ProbeConfig config) {
        HiveConf hiveConf = new HiveConf();
        addConfDirResources(hiveConf, config.confDir);
        for (String resource : config.resources) {
            hiveConf.addResource(new Path(resource));
        }
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, config.metastoreUris);
        if (config.simpleUser != null) {
            hiveConf.set("hadoop.user.name", config.simpleUser);
        }
        if (config.connectTimeoutSeconds != null) {
            hiveConf.set(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname,
                String.valueOf(config.connectTimeoutSeconds));
        }
        for (Map.Entry<String, String> entry : config.overrides.entrySet()) {
            hiveConf.set(entry.getKey(), entry.getValue());
        }
        return hiveConf;
    }

    private static void addConfDirResources(HiveConf hiveConf, String confDir) {
        if (confDir == null) {
            return;
        }
        List<String> names = Arrays.asList("core-site.xml", "hdfs-site.xml", "hive-site.xml");
        for (String name : names) {
            File file = new File(confDir, name);
            if (file.exists()) {
                hiveConf.addResource(new Path(file.getAbsolutePath()));
            }
        }
    }

    private static UserGroupInformation buildUserGroupInformation(ProbeConfig config) throws Exception {
        if (config.kerberosPrincipal != null || config.keytab != null) {
            if (config.kerberosPrincipal == null || config.keytab == null) {
                throw new IllegalArgumentException("Both --kerberos-principal and --keytab are required together");
            }
            UserGroupInformation.loginUserFromKeytab(config.kerberosPrincipal, config.keytab);
            return UserGroupInformation.getLoginUser();
        }
        if (config.simpleUser != null) {
            return UserGroupInformation.createRemoteUser(config.simpleUser);
        }
        return UserGroupInformation.getCurrentUser();
    }

    private static void printBanner(ProbeConfig config, HiveConf hiveConf) {
        System.out.println("Pulse HMS Latency Probe");
        System.out.println(
            "target=" + config.metastoreUris
                + " operation=" + config.operation.cliName
                + " warmup=" + config.warmupIterations
                + " iterations=" + config.measureIterations
                + " concurrency=" + config.concurrency
                + " timeout=" + hiveConf.get(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname)
        );
        System.out.println();
    }

    private static void runWarmup(ProbeConfig config, HiveConf hiveConf, UserGroupInformation ugi) throws Exception {
        for (int i = 0; i < config.warmupIterations; i++) {
            ProbeResult result = doAs(ugi, new Callable<ProbeResult>() {
                @Override
                public ProbeResult call() throws Exception {
                    return runSingleProbe(config, hiveConf);
                }
            });
            if (config.verbose) {
                printPerRequest("warmup", i + 1, result);
            }
        }
    }

    private static ProbeSummary runMeasured(ProbeConfig config, HiveConf hiveConf, UserGroupInformation ugi)
        throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(config.concurrency);
        List<Future<ProbeResult>> futures = new ArrayList<Future<ProbeResult>>(config.measureIterations);
        try {
            for (int i = 0; i < config.measureIterations; i++) {
                futures.add(executorService.submit(new ProbeTask(config, hiveConf, ugi)));
            }
        } finally {
            executorService.shutdown();
        }
        executorService.awaitTermination(7, TimeUnit.DAYS);

        ProbeSummary summary = new ProbeSummary(config);
        int index = 0;
        for (Future<ProbeResult> future : futures) {
            index++;
            try {
                ProbeResult result = future.get();
                summary.recordSuccess(result);
                if (config.verbose) {
                    printPerRequest("measure", index, result);
                }
            } catch (ExecutionException executionException) {
                Throwable cause = executionException.getCause() == null ? executionException : executionException.getCause();
                summary.recordFailure(cause);
            }
        }
        return summary;
    }

    private static void printPerRequest(String phase, int index, ProbeResult result) {
        System.out.println("[" + phase + "-" + pad(index) + "] connect=" + formatMillis(result.connectMs)
            + " rpc=" + formatMillis(result.rpcMs)
            + " total=" + formatMillis(result.totalMs)
            + " payload=" + result.payloadSummary);
    }

    private static String pad(int index) {
        if (index >= 100) {
            return String.valueOf(index);
        }
        if (index >= 10) {
            return "0" + index;
        }
        return "00" + index;
    }

    private static ProbeResult runSingleProbe(ProbeConfig config, HiveConf hiveConf) throws Exception {
        long totalStart = System.nanoTime();
        HiveConf localHiveConf = new HiveConf(hiveConf);

        long connectStart = totalStart;
        IMetaStoreClient client = RetryingMetaStoreClient.getProxy(localHiveConf, DUMMY_HOOK_LOADER,
            HiveMetaStoreClient.class.getName());
        long connectEnd = System.nanoTime();

        long rpcStart = connectEnd;
        String payloadSummary;
        try {
            payloadSummary = config.operation.execute(client, config);
        } finally {
            client.close();
        }
        long rpcEnd = System.nanoTime();

        return new ProbeResult(
            nanosToMillis(connectEnd - connectStart),
            nanosToMillis(rpcEnd - rpcStart),
            nanosToMillis(rpcEnd - totalStart),
            payloadSummary
        );
    }

    private static <T> T doAs(UserGroupInformation ugi, final Callable<T> callable) throws Exception {
        return ugi.doAs(new PrivilegedExceptionAction<T>() {
            @Override
            public T run() throws Exception {
                return callable.call();
            }
        });
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0d;
    }

    private static String formatMillis(double value) {
        return DECIMAL_FORMAT.format(value) + "ms";
    }

    private enum Operation {
        GET_TABLE("get_table") {
            @Override
            String execute(IMetaStoreClient client, ProbeConfig config) throws Exception {
                Table table = client.getTable(config.dbName, config.tableName);
                return table.getDbName() + "." + table.getTableName();
            }

            @Override
            void validate(ProbeConfig config) {
                config.requireDatabaseAndTable();
            }
        },
        LIST_PARTITIONS("list_partitions") {
            @Override
            String execute(IMetaStoreClient client, ProbeConfig config) throws Exception {
                short maxPartitions = (short) Math.min(config.partitionsLimit, Short.MAX_VALUE);
                List<String> partitionNames = client.listPartitionNames(config.dbName, config.tableName, maxPartitions);
                return "partitions=" + partitionNames.size();
            }

            @Override
            void validate(ProbeConfig config) {
                config.requireDatabaseAndTable();
            }
        },
        GET_ALL_DATABASES("get_all_databases") {
            @Override
            String execute(IMetaStoreClient client, ProbeConfig config) throws Exception {
                List<String> databases = client.getAllDatabases();
                return "databases=" + databases.size();
            }
        };

        private final String cliName;

        Operation(String cliName) {
            this.cliName = cliName;
        }

        abstract String execute(IMetaStoreClient client, ProbeConfig config) throws Exception;

        void validate(ProbeConfig config) {
        }

        static Operation from(String cliName) {
            for (Operation operation : values()) {
                if (operation.cliName.equalsIgnoreCase(cliName)) {
                    return operation;
                }
            }
            throw new IllegalArgumentException("Unsupported operation: " + cliName);
        }
    }

    private static class ProbeConfig {
        private final String metastoreUris;
        private final Operation operation;
        private final String dbName;
        private final String tableName;
        private final int partitionsLimit;
        private final int warmupIterations;
        private final int measureIterations;
        private final int concurrency;
        private final String confDir;
        private final List<String> resources;
        private final Map<String, String> overrides;
        private final String simpleUser;
        private final String kerberosPrincipal;
        private final String keytab;
        private final Integer connectTimeoutSeconds;
        private final boolean verbose;

        private ProbeConfig(
            String metastoreUris,
            Operation operation,
            String dbName,
            String tableName,
            int partitionsLimit,
            int warmupIterations,
            int measureIterations,
            int concurrency,
            String confDir,
            List<String> resources,
            Map<String, String> overrides,
            String simpleUser,
            String kerberosPrincipal,
            String keytab,
            Integer connectTimeoutSeconds,
            boolean verbose
        ) {
            this.metastoreUris = metastoreUris;
            this.operation = operation;
            this.dbName = dbName;
            this.tableName = tableName;
            this.partitionsLimit = partitionsLimit;
            this.warmupIterations = warmupIterations;
            this.measureIterations = measureIterations;
            this.concurrency = concurrency;
            this.confDir = confDir;
            this.resources = resources;
            this.overrides = overrides;
            this.simpleUser = simpleUser;
            this.kerberosPrincipal = kerberosPrincipal;
            this.keytab = keytab;
            this.connectTimeoutSeconds = connectTimeoutSeconds;
            this.verbose = verbose;
        }

        static ProbeConfig from(CommandLine commandLine) {
            String operationText = commandLine.getOptionValue("operation", "get_table");
            ProbeConfig config = new ProbeConfig(
                requireText(commandLine.getOptionValue("uris"), "--uris"),
                Operation.from(operationText),
                commandLine.getOptionValue("db"),
                commandLine.getOptionValue("table"),
                parsePositiveInt(commandLine.getOptionValue("partitions-limit", "1000"), "--partitions-limit"),
                parseNonNegativeInt(commandLine.getOptionValue("warmup", "3"), "--warmup"),
                parsePositiveInt(commandLine.getOptionValue("iterations", "20"), "--iterations"),
                parsePositiveInt(commandLine.getOptionValue("concurrency", "1"), "--concurrency"),
                commandLine.getOptionValue("conf-dir"),
                optionValues(commandLine, "resource"),
                parseOverrides(optionValues(commandLine, "conf")),
                commandLine.getOptionValue("simple-user"),
                commandLine.getOptionValue("kerberos-principal"),
                commandLine.getOptionValue("keytab"),
                commandLine.hasOption("connect-timeout")
                    ? Integer.valueOf(parsePositiveInt(commandLine.getOptionValue("connect-timeout"), "--connect-timeout"))
                    : null,
                commandLine.hasOption("verbose")
            );
            config.operation.validate(config);
            return config;
        }

        private static List<String> optionValues(CommandLine commandLine, String option) {
            String[] values = commandLine.getOptionValues(option);
            if (values == null || values.length == 0) {
                return Collections.emptyList();
            }
            return Arrays.asList(values);
        }

        private static Map<String, String> parseOverrides(List<String> values) {
            Map<String, String> overrides = new LinkedHashMap<String, String>();
            for (String value : values) {
                int split = value.indexOf('=');
                if (split <= 0 || split == value.length() - 1) {
                    throw new IllegalArgumentException("Invalid --conf entry: " + value + ". Expected key=value");
                }
                overrides.put(value.substring(0, split), value.substring(split + 1));
            }
            return overrides;
        }

        private static String requireText(String value, String optionName) {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException(optionName + " is required");
            }
            return value.trim();
        }

        private static int parsePositiveInt(String value, String optionName) {
            int parsed = parseInt(value, optionName);
            if (parsed <= 0) {
                throw new IllegalArgumentException(optionName + " must be > 0");
            }
            return parsed;
        }

        private static int parseNonNegativeInt(String value, String optionName) {
            int parsed = parseInt(value, optionName);
            if (parsed < 0) {
                throw new IllegalArgumentException(optionName + " must be >= 0");
            }
            return parsed;
        }

        private static int parseInt(String value, String optionName) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException numberFormatException) {
                throw new IllegalArgumentException(optionName + " must be an integer: " + value, numberFormatException);
            }
        }

        private void requireDatabaseAndTable() {
            if (dbName == null || dbName.trim().isEmpty()) {
                throw new IllegalArgumentException("--db is required for " + operation.cliName);
            }
            if (tableName == null || tableName.trim().isEmpty()) {
                throw new IllegalArgumentException("--table is required for " + operation.cliName);
            }
        }
    }

    private static class ProbeTask implements Callable<ProbeResult> {
        private final ProbeConfig config;
        private final HiveConf hiveConf;
        private final UserGroupInformation ugi;

        private ProbeTask(ProbeConfig config, HiveConf hiveConf, UserGroupInformation ugi) {
            this.config = config;
            this.hiveConf = hiveConf;
            this.ugi = ugi;
        }

        @Override
        public ProbeResult call() throws Exception {
            return doAs(ugi, new Callable<ProbeResult>() {
                @Override
                public ProbeResult call() throws Exception {
                    return runSingleProbe(config, hiveConf);
                }
            });
        }
    }

    private static class ProbeResult {
        private final double connectMs;
        private final double rpcMs;
        private final double totalMs;
        private final String payloadSummary;

        private ProbeResult(double connectMs, double rpcMs, double totalMs, String payloadSummary) {
            this.connectMs = connectMs;
            this.rpcMs = rpcMs;
            this.totalMs = totalMs;
            this.payloadSummary = payloadSummary;
        }
    }

    private static class ProbeSummary {
        private final ProbeConfig config;
        private final List<Double> connectLatencies = new ArrayList<Double>();
        private final List<Double> rpcLatencies = new ArrayList<Double>();
        private final List<Double> totalLatencies = new ArrayList<Double>();
        private final List<String> failureMessages = new ArrayList<String>();
        private int failures;

        private ProbeSummary(ProbeConfig config) {
            this.config = config;
        }

        private void recordSuccess(ProbeResult result) {
            connectLatencies.add(result.connectMs);
            rpcLatencies.add(result.rpcMs);
            totalLatencies.add(result.totalMs);
        }

        private void recordFailure(Throwable throwable) {
            failures++;
            failureMessages.add(throwable.getClass().getSimpleName() + ": " + Objects.toString(throwable.getMessage(), ""));
        }

        private void print() {
            System.out.println("Summary");
            System.out.println("success=" + connectLatencies.size() + " failures=" + failures + " operation=" + config.operation.cliName);
            if (!connectLatencies.isEmpty()) {
                printMetric("connect", connectLatencies);
                printMetric("rpc", rpcLatencies);
                printMetric("total", totalLatencies);
            }
            if (!failureMessages.isEmpty()) {
                System.out.println();
                System.out.println("Failures");
                for (String failureMessage : failureMessages) {
                    System.out.println("- " + failureMessage);
                }
            }
        }

        private void printMetric(String name, List<Double> latencies) {
            Collections.sort(latencies);
            System.out.println(
                name + " min=" + formatMillis(percentile(latencies, 0.0d))
                    + " p50=" + formatMillis(percentile(latencies, 0.50d))
                    + " p95=" + formatMillis(percentile(latencies, 0.95d))
                    + " p99=" + formatMillis(percentile(latencies, 0.99d))
                    + " max=" + formatMillis(percentile(latencies, 1.0d))
                    + " avg=" + formatMillis(average(latencies))
            );
        }

        private double percentile(List<Double> values, double fraction) {
            if (values.isEmpty()) {
                return 0.0d;
            }
            if (fraction <= 0.0d) {
                return values.get(0);
            }
            if (fraction >= 1.0d) {
                return values.get(values.size() - 1);
            }
            int index = (int) Math.ceil(fraction * values.size()) - 1;
            if (index < 0) {
                index = 0;
            }
            if (index >= values.size()) {
                index = values.size() - 1;
            }
            return values.get(index);
        }

        private double average(List<Double> values) {
            double sum = 0.0d;
            for (Double value : values) {
                sum += value;
            }
            return sum / values.size();
        }
    }
}
