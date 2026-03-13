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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.core.config.Configurator;

import java.io.File;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
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
    private static final String TOOL_NAME = "hms-tools";
    private static final String TOOL_VERSION = "2.0.0-dev";
    private static final String HADOOP_VERSION = "3.3.6";
    private static final String HIVE_VERSION = "3.1.3";

    static {
        initializeLogging();
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printGlobalHelp(System.out);
            return;
        }

        try {
            CommandSelection selection = selectCommand(args);
            if (selection.helpCommand != null) {
                printCommandHelp(selection.helpCommand, System.out);
                return;
            }
            if (selection.commandLine.hasOption("help")) {
                printCommandHelp(selection.command, System.out);
                return;
            }

            CommandOutput output = run(selection.command, selection.commandLine);
            output.print();
            if (output.exitCode != 0) {
                System.exit(output.exitCode);
            }
        } catch (ParseException parseException) {
            System.err.println("Error: " + parseException.getMessage());
            System.exit(10);
        } catch (IllegalArgumentException illegalArgumentException) {
            System.err.println("Error: " + illegalArgumentException.getMessage());
            System.exit(10);
        } catch (Exception exception) {
            System.err.println("Error: " + Objects.toString(exception.getMessage(), exception.getClass().getSimpleName()));
            System.exit(1);
        }
    }

    private static CommandOutput run(CommandKind command, CommandLine commandLine) throws Exception {
        switch (command) {
            case CHECK:
                return runCheck(commandLine);
            case PING:
                return runPing(commandLine);
            case OBJECT:
                return runObject(commandLine);
            case BENCH:
                return runBench(commandLine);
            case CONFIG:
                return runConfig(commandLine);
            case VERSION:
                return runVersion(commandLine);
            default:
                throw new IllegalArgumentException("Unsupported command: " + command.cliName);
        }
    }

    private static void initializeLogging() {
        try {
            if (HmsLatencyProbe.class.getClassLoader().getResource("log4j2.xml") != null) {
                Configurator.initialize(null, HmsLatencyProbe.class.getClassLoader(), "log4j2.xml");
            }
        } catch (Throwable ignored) {
            // Logging setup should not prevent the CLI from starting.
        }
    }

    private static CommandSelection selectCommand(String[] args) throws ParseException {
        if ("help".equalsIgnoreCase(args[0])) {
            CommandKind helpTarget = args.length > 1 ? CommandKind.from(args[1]) : CommandKind.GLOBAL;
            return new CommandSelection(CommandKind.GLOBAL, helpTarget, null);
        }
        if ("--help".equals(args[0]) || "-h".equals(args[0])) {
            return new CommandSelection(CommandKind.GLOBAL, CommandKind.GLOBAL, null);
        }

        CommandKind command = CommandKind.CHECK;
        String[] commandArgs = args;
        if (CommandKind.isCommand(args[0])) {
            command = CommandKind.from(args[0]);
            commandArgs = Arrays.copyOfRange(args, 1, args.length);
        }

        CommandLine commandLine = parseCommandLine(buildOptions(command), commandArgs);
        return new CommandSelection(command, null, commandLine);
    }

    private static Options buildOptions(CommandKind command) {
        Options options = buildGlobalOptions();
        switch (command) {
            case CHECK:
                addCheckOptions(options);
                break;
            case PING:
                addPingOptions(options);
                break;
            case OBJECT:
                addObjectOptions(options);
                break;
            case BENCH:
                addBenchOptions(options);
                break;
            case CONFIG:
            case VERSION:
            case GLOBAL:
                break;
            default:
                throw new IllegalArgumentException("Unsupported command: " + command.cliName);
        }
        return options;
    }

    private static Options buildGlobalOptions() {
        Options options = new Options();
        options.addOption(Option.builder("h").longOpt("help").desc("Print help").build());
        options.addOption(Option.builder().longOpt("uris").hasArg().argName("thrift://host:port[,..]")
            .desc("Hive metastore thrift URIs").build());
        options.addOption(Option.builder().longOpt("conf-dir").hasArg().argName("dir")
            .desc("Directory containing core-site.xml, hdfs-site.xml, hive-site.xml").build());
        options.addOption(Option.builder().longOpt("resource").hasArg().argName("file")
            .desc("Extra Hadoop or Hive XML resource. Repeatable").build());
        options.addOption(Option.builder().longOpt("conf").hasArg().argName("k=v")
            .desc("Additional Hadoop or Hive conf. Repeatable").build());
        options.addOption(Option.builder().longOpt("auth").hasArg().argName("auto|simple|kerberos")
            .desc("Authentication mode. Default: auto").build());
        options.addOption(Option.builder().longOpt("simple-user").hasArg().argName("user")
            .desc("Run RPCs as the specified simple-auth user").build());
        options.addOption(Option.builder().longOpt("principal").hasArg().argName("principal")
            .desc("Kerberos principal for login").build());
        options.addOption(Option.builder().longOpt("keytab").hasArg().argName("path")
            .desc("Keytab for Kerberos login").build());
        options.addOption(Option.builder().longOpt("timeout").hasArg().argName("seconds")
            .desc("Socket timeout for connect and RPC. Default: 10").build());
        options.addOption(Option.builder().longOpt("no-retry")
            .desc("Disable metastore client retries").build());
        options.addOption(Option.builder().longOpt("format").hasArg().argName("text|json")
            .desc("Output format. Default: text").build());
        options.addOption(Option.builder().longOpt("verbose")
            .desc("Print detailed stage output").build());
        options.addOption(Option.builder().longOpt("show-secrets")
            .desc("Show sensitive config values in config output").build());
        return options;
    }

    private static void addCheckOptions(Options options) {
        options.addOption(Option.builder().longOpt("deep")
            .desc("Enable deeper metadata checks").build());
        options.addOption(Option.builder().longOpt("db").hasArg().argName("db")
            .desc("Database name for optional object validation").build());
        options.addOption(Option.builder().longOpt("table").hasArg().argName("table")
            .desc("Table name for optional object validation").build());
        options.addOption(Option.builder().longOpt("rpc").hasArg().argName("name")
            .desc("Basic RPC: get_all_databases, get_database, get_tables. Default: get_all_databases").build());
        options.addOption(Option.builder().longOpt("fail-fast")
            .desc("Stop after the first fatal failure").build());
    }

    private static void addPingOptions(Options options) {
        options.addOption(Option.builder().longOpt("rpc").hasArg().argName("name")
            .desc("Lightweight RPC: get_all_databases, get_database, get_tables. Default: get_all_databases").build());
        options.addOption(Option.builder().longOpt("db").hasArg().argName("db")
            .desc("Database name for get_database").build());
        options.addOption(Option.builder().longOpt("count").hasArg().argName("N")
            .desc("Number of probes per URI. Default: 1").build());
        options.addOption(Option.builder().longOpt("interval-ms").hasArg().argName("N")
            .desc("Delay between probes in milliseconds. Default: 0").build());
        options.addOption(Option.builder().longOpt("parallel")
            .desc("Probe URIs in parallel").build());
    }

    private static void addObjectOptions(Options options) {
        options.addOption(Option.builder().longOpt("db").hasArg().argName("db")
            .desc("Database name").build());
        options.addOption(Option.builder().longOpt("table").hasArg().argName("table")
            .desc("Table name").build());
        options.addOption(Option.builder().longOpt("partition").hasArg().argName("k=v/..")
            .desc("Partition spec or partition name").build());
        options.addOption(Option.builder().longOpt("check").hasArg().argName("exists|schema|partitions|stats")
            .desc("Object check type").build());
        options.addOption(Option.builder().longOpt("limit").hasArg().argName("N")
            .desc("Max partition count for partitions check").build());
    }

    private static void addBenchOptions(Options options) {
        options.addOption(Option.builder().longOpt("rpc").hasArg().argName("name")
            .desc("Benchmark RPC: get_table, list_partitions, get_all_databases, get_database").build());
        options.addOption(Option.builder().longOpt("db").hasArg().argName("db")
            .desc("Database name for get_table, list_partitions, get_database").build());
        options.addOption(Option.builder().longOpt("table").hasArg().argName("table")
            .desc("Table name for get_table and list_partitions").build());
        options.addOption(Option.builder().longOpt("limit").hasArg().argName("N")
            .desc("Limit for list_partitions. Must be <= 32767").build());
        options.addOption(Option.builder().longOpt("warmup").hasArg().argName("N")
            .desc("Warmup requests before measurement. Default: 3").build());
        options.addOption(Option.builder().longOpt("iterations").hasArg().argName("N")
            .desc("Measured requests. Default: 20").build());
        options.addOption(Option.builder().longOpt("concurrency").hasArg().argName("N")
            .desc("Concurrent measured requests. Default: 1").build());
        options.addOption(Option.builder().longOpt("per-uri")
            .desc("Benchmark each URI independently").build());
        options.addOption(Option.builder().longOpt("histogram")
            .desc("Print latency percentiles and aggregates").build());
        options.addOption(Option.builder().longOpt("success-threshold").hasArg().argName("pct")
            .desc("Minimum success ratio required for a passing result").build());
        options.addOption(Option.builder().longOpt("latency-slo-ms").hasArg().argName("N")
            .desc("Fail if p95 total latency exceeds the threshold").build());
    }

    private static CommandLine parseCommandLine(Options options, String[] args) throws ParseException {
        try {
            return new DefaultParser().parse(options, args);
        } catch (ParseException parseException) {
            throw parseException;
        }
    }

    private static CommandOutput runCheck(CommandLine commandLine) throws Exception {
        GlobalConfig global = parseGlobalConfig(commandLine, true);
        CheckOptions check = CheckOptions.from(commandLine);
        HiveConfBundle bundle = buildHiveConf(global, joinUris(global.uris));
        UserGroupInformation.setConfiguration(bundle.hiveConf);

        AuthContext authContext = buildAuthContext(global);
        List<UriReport> uriReports = new ArrayList<UriReport>();
        for (String uri : global.uris) {
            UriReport uriReport = new UriReport(uri);
            uriReport.add(connectStage(uri, global.timeoutSeconds));
            if (uriReport.lastStatus() == Status.OK) {
                uriReport.add(runBasicRpc(uri, bundle.hiveConf, global, authContext, check.rpc, check.dbName));
                if (uriReport.lastStatus() == Status.OK && check.dbName != null) {
                    uriReport.add(runCheckObjectStage(uri, bundle.hiveConf, global, authContext, check));
                    if (check.deep && check.tableName != null && uriReport.lastStatus() == Status.OK) {
                        uriReport.add(runSchemaStage(uri, bundle.hiveConf, global, authContext, check.dbName, check.tableName));
                    }
                }
            }
            uriReports.add(uriReport);
            if (check.failFast && uriReport.status == Status.FAIL) {
                break;
            }
        }

        int exitCode = decideDiagnosticExitCode(authContext, uriReports);
        Status overall = decideDiagnosticStatus(authContext, uriReports);
        Map<String, Object> json = buildDiagnosticJson("check", overall, exitCode, global, bundle, authContext, uriReports);
        String text = renderDiagnosticText("check", overall, exitCode, global, bundle, authContext, uriReports);
        return new CommandOutput(global.format, exitCode, text, json);
    }

    private static CommandOutput runPing(CommandLine commandLine) throws Exception {
        GlobalConfig global = parseGlobalConfig(commandLine, true);
        PingOptions ping = PingOptions.from(commandLine);
        HiveConfBundle bundle = buildHiveConf(global, joinUris(global.uris));
        UserGroupInformation.setConfiguration(bundle.hiveConf);

        AuthContext authContext = buildAuthContext(global);
        List<UriReport> uriReports;
        if (ping.parallel && global.uris.size() > 1) {
            uriReports = runPingParallel(global, bundle, authContext, ping);
        } else {
            uriReports = runPingSequential(global, bundle, authContext, ping);
        }

        int exitCode = decideDiagnosticExitCode(authContext, uriReports);
        Status overall = decideDiagnosticStatus(authContext, uriReports);
        Map<String, Object> json = buildDiagnosticJson("ping", overall, exitCode, global, bundle, authContext, uriReports);
        String text = renderDiagnosticText("ping", overall, exitCode, global, bundle, authContext, uriReports);
        return new CommandOutput(global.format, exitCode, text, json);
    }

    private static List<UriReport> runPingSequential(
        GlobalConfig global,
        HiveConfBundle bundle,
        AuthContext authContext,
        PingOptions ping
    ) throws Exception {
        List<UriReport> reports = new ArrayList<UriReport>();
        for (String uri : global.uris) {
            reports.add(runPingForUri(uri, global, bundle.hiveConf, authContext, ping));
        }
        return reports;
    }

    private static List<UriReport> runPingParallel(
        GlobalConfig global,
        HiveConfBundle bundle,
        AuthContext authContext,
        PingOptions ping
    ) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(global.uris.size());
        List<Future<UriReport>> futures = new ArrayList<Future<UriReport> >();
        try {
            for (final String uri : global.uris) {
                futures.add(executorService.submit(new Callable<UriReport>() {
                    @Override
                    public UriReport call() throws Exception {
                        return runPingForUri(uri, global, bundle.hiveConf, authContext, ping);
                    }
                }));
            }
        } finally {
            executorService.shutdown();
        }
        executorService.awaitTermination(1, TimeUnit.DAYS);

        List<UriReport> reports = new ArrayList<UriReport>();
        for (Future<UriReport> future : futures) {
            try {
                reports.add(future.get());
            } catch (ExecutionException executionException) {
                Throwable cause = executionException.getCause() == null ? executionException : executionException.getCause();
                UriReport report = new UriReport("unknown");
                report.add(StageResult.failure("ping", classifyErrorCode(cause), cause));
                reports.add(report);
            }
        }
        return reports;
    }

    private static UriReport runPingForUri(
        String uri,
        GlobalConfig global,
        HiveConf hiveConf,
        AuthContext authContext,
        PingOptions ping
    ) throws Exception {
        UriReport uriReport = new UriReport(uri);
        for (int i = 0; i < ping.count; i++) {
            StageResult connect = connectStage(uri, global.timeoutSeconds);
            String stageName = "attempt-" + pad(i + 1);
            if (connect.status != Status.OK) {
                uriReport.add(renameStage(connect, stageName + "-connect"));
            } else {
                StageResult rpc = runBasicRpc(uri, hiveConf, global, authContext, ping.rpc, ping.dbName);
                String message = "connect=" + formatMillis(valueOrZero(connect.latencyMs))
                    + " rpc=" + formatMillis(valueOrZero(rpc.latencyMs))
                    + " result=" + Objects.toString(rpc.message, "");
                if (rpc.status == Status.OK) {
                    uriReport.add(new StageResult(stageName, Status.OK, connect.latencyMs + rpc.latencyMs, message, null, rpc.details));
                } else {
                    uriReport.add(new StageResult(stageName, Status.FAIL, connect.latencyMs + rpc.latencyMs, message,
                        rpc.errorCode, rpc.details));
                }
            }
            if (ping.intervalMs > 0 && i + 1 < ping.count) {
                Thread.sleep(ping.intervalMs);
            }
        }
        return uriReport;
    }

    private static CommandOutput runObject(CommandLine commandLine) throws Exception {
        GlobalConfig global = parseGlobalConfig(commandLine, true);
        ObjectOptions object = ObjectOptions.from(commandLine);
        HiveConfBundle bundle = buildHiveConf(global, joinUris(global.uris));
        UserGroupInformation.setConfiguration(bundle.hiveConf);

        AuthContext authContext = buildAuthContext(global);
        List<UriReport> uriReports = new ArrayList<UriReport>();
        for (String uri : global.uris) {
            UriReport uriReport = new UriReport(uri);
            uriReport.add(connectStage(uri, global.timeoutSeconds));
            if (uriReport.lastStatus() == Status.OK) {
                uriReport.add(runObjectStage(uri, bundle.hiveConf, global, authContext, object));
            }
            uriReports.add(uriReport);
        }

        int exitCode = decideObjectExitCode(authContext, uriReports);
        Status overall = decideDiagnosticStatus(authContext, uriReports);
        Map<String, Object> json = buildDiagnosticJson("object", overall, exitCode, global, bundle, authContext, uriReports);
        String text = renderDiagnosticText("object", overall, exitCode, global, bundle, authContext, uriReports);
        return new CommandOutput(global.format, exitCode, text, json);
    }

    private static CommandOutput runBench(CommandLine commandLine) throws Exception {
        GlobalConfig global = parseGlobalConfig(commandLine, true);
        BenchOptions bench = BenchOptions.from(commandLine);
        HiveConfBundle bundle = buildHiveConf(global, joinUris(global.uris));
        UserGroupInformation.setConfiguration(bundle.hiveConf);
        AuthContext authContext = buildAuthContext(global);

        List<BenchTargetReport> targetReports = new ArrayList<BenchTargetReport>();
        if (bench.perUri && global.uris.size() > 1) {
            for (String uri : global.uris) {
                ProbeSummary summary = runBenchmark(uri, bundle.hiveConf, global, authContext, bench);
                targetReports.add(new BenchTargetReport(uri, summary));
            }
        } else {
            ProbeSummary summary = runBenchmark(joinUris(global.uris), bundle.hiveConf, global, authContext, bench);
            targetReports.add(new BenchTargetReport(joinUris(global.uris), summary));
        }

        int exitCode = decideBenchExitCode(authContext, targetReports, bench);
        Status overall = decideBenchStatus(authContext, targetReports, exitCode);
        Map<String, Object> json = buildBenchJson(overall, exitCode, global, bundle, authContext, bench, targetReports);
        String text = renderBenchText(overall, exitCode, global, bundle, authContext, bench, targetReports);
        return new CommandOutput(global.format, exitCode, text, json);
    }

    private static CommandOutput runConfig(CommandLine commandLine) throws Exception {
        GlobalConfig global = parseGlobalConfig(commandLine, true);
        HiveConfBundle bundle = buildHiveConf(global, joinUris(global.uris));
        UserGroupInformation.setConfiguration(bundle.hiveConf);
        AuthContext authContext = buildAuthContext(global);

        Status overall = authContext.ok ? Status.OK : Status.FAIL;
        int exitCode = authContext.ok ? 0 : 12;

        Map<String, Object> json = new LinkedHashMap<String, Object>();
        json.put("command", "config");
        json.put("status", overall.name());
        json.put("exit_code", Integer.valueOf(exitCode));
        json.put("tool", toolMetadata());
        json.put("config", buildConfigMap(global, bundle));
        json.put("auth", authContext.toMap());

        StringBuilder text = new StringBuilder();
        text.append("COMMAND: config\n");
        text.append("STATUS: ").append(overall.name()).append('\n');
        text.append("AUTH: ").append(authContext.render()).append("\n");
        text.append("CONFIG\n");
        text.append("  uris=").append(joinUris(global.uris)).append('\n');
        text.append("  auth_mode=").append(global.authMode.name().toLowerCase(Locale.ROOT)).append('\n');
        text.append("  timeout=").append(global.timeoutSeconds).append("s\n");
        text.append("  retry=").append(global.noRetry ? "off" : "on").append('\n');
        text.append("  loaded_resources=").append(bundle.loadedResources.size()).append('\n');
        if (!bundle.loadedResources.isEmpty()) {
            for (String resource : bundle.loadedResources) {
                text.append("    ").append(resource).append('\n');
            }
        }
        if (!bundle.missingDefaultResources.isEmpty()) {
            text.append("  missing_default_resources=").append(bundle.missingDefaultResources.size()).append('\n');
            for (String resource : bundle.missingDefaultResources) {
                text.append("    ").append(resource).append('\n');
            }
        }
        text.append("  effective_conf").append('\n');
        for (Map.Entry<String, String> entry : buildConfigSnapshot(global, bundle).entrySet()) {
            text.append("    ").append(entry.getKey()).append('=').append(entry.getValue()).append('\n');
        }
        text.append("EXIT_CODE: ").append(exitCode).append('\n');
        return new CommandOutput(global.format, exitCode, text.toString(), json);
    }

    private static CommandOutput runVersion(CommandLine commandLine) {
        OutputFormat format = OutputFormat.from(commandLine.getOptionValue("format", "text"));
        Map<String, Object> json = new LinkedHashMap<String, Object>();
        json.put("command", "version");
        json.put("status", "OK");
        json.put("exit_code", Integer.valueOf(0));
        json.put("tool", toolMetadata());
        Map<String, Object> runtime = new LinkedHashMap<String, Object>();
        runtime.put("java_version", System.getProperty("java.version"));
        runtime.put("java_vendor", System.getProperty("java.vendor"));
        json.put("runtime", runtime);

        StringBuilder text = new StringBuilder();
        text.append("COMMAND: version\n");
        text.append("STATUS: OK\n");
        text.append("tool=").append(TOOL_NAME).append('\n');
        text.append("version=").append(TOOL_VERSION).append('\n');
        text.append("hadoop_version=").append(HADOOP_VERSION).append('\n');
        text.append("hive_version=").append(HIVE_VERSION).append('\n');
        text.append("java_version=").append(System.getProperty("java.version")).append('\n');
        text.append("java_vendor=").append(System.getProperty("java.vendor")).append('\n');
        return new CommandOutput(format, 0, text.toString(), json);
    }

    private static ProbeSummary runBenchmark(
        String targetUris,
        HiveConf baseHiveConf,
        GlobalConfig global,
        AuthContext authContext,
        BenchOptions bench
    ) throws Exception {
        HiveConf hiveConf = new HiveConf(baseHiveConf);
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, targetUris);
        runWarmup(bench, hiveConf, authContext.ugi, global.noRetry);
        return runMeasured(bench, hiveConf, authContext.ugi, global.noRetry);
    }

    private static void runWarmup(BenchOptions bench, HiveConf hiveConf, UserGroupInformation ugi, boolean noRetry) throws Exception {
        for (int i = 0; i < bench.warmupIterations; i++) {
            ProbeResult result = doAs(ugi, new Callable<ProbeResult>() {
                @Override
                public ProbeResult call() throws Exception {
                    return runSingleProbe(bench, hiveConf, noRetry);
                }
            });
            if (bench.verbose) {
                System.out.println("[warmup-" + pad(i + 1) + "] connect=" + formatMillis(result.connectMs)
                    + " rpc=" + formatMillis(result.rpcMs)
                    + " total=" + formatMillis(result.totalMs)
                    + " payload=" + result.payloadSummary);
            }
        }
    }

    private static ProbeSummary runMeasured(BenchOptions bench, HiveConf hiveConf, UserGroupInformation ugi, boolean noRetry)
        throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(bench.concurrency);
        List<Future<ProbeResult>> futures = new ArrayList<Future<ProbeResult> >(bench.measureIterations);
        try {
            for (int i = 0; i < bench.measureIterations; i++) {
                futures.add(executorService.submit(new ProbeTask(bench, hiveConf, ugi, noRetry)));
            }
        } finally {
            executorService.shutdown();
        }
        executorService.awaitTermination(7, TimeUnit.DAYS);

        ProbeSummary summary = new ProbeSummary(bench);
        for (Future<ProbeResult> future : futures) {
            try {
                summary.recordSuccess(future.get());
            } catch (ExecutionException executionException) {
                Throwable cause = executionException.getCause() == null ? executionException : executionException.getCause();
                summary.recordFailure(cause);
            }
        }
        return summary;
    }

    private static ProbeResult runSingleProbe(BenchOptions bench, HiveConf hiveConf, boolean noRetry) throws Exception {
        long totalStart = System.nanoTime();
        HiveConf localHiveConf = new HiveConf(hiveConf);
        long connectStart = totalStart;
        IMetaStoreClient client = createClient(localHiveConf, noRetry);
        long connectEnd = System.nanoTime();

        long rpcStart = connectEnd;
        String payloadSummary;
        try {
            payloadSummary = bench.rpc.execute(client, bench);
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

    private static IMetaStoreClient createClient(HiveConf hiveConf, boolean noRetry) throws Exception {
        if (noRetry) {
            return new HiveMetaStoreClient(hiveConf, DUMMY_HOOK_LOADER);
        }
        return RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER, HiveMetaStoreClient.class.getName());
    }

    private static StageResult runBasicRpc(
        final String uri,
        final HiveConf baseHiveConf,
        final GlobalConfig global,
        final AuthContext authContext,
        final BasicRpc basicRpc,
        final String dbName
    ) {
        long start = System.nanoTime();
        try {
            RpcPayload payload = withClient(uri, baseHiveConf, global, authContext, new ClientCall<RpcPayload>() {
                @Override
                public RpcPayload call(IMetaStoreClient client) throws Exception {
                    return basicRpc.execute(client, dbName);
                }
            });
            return StageResult.success("rpc " + basicRpc.cliName, nanosToMillis(System.nanoTime() - start), payload.summary, payload.details);
        } catch (Exception exception) {
            return StageResult.failure("rpc " + basicRpc.cliName, nanosToMillis(System.nanoTime() - start),
                classifyErrorCode(exception), exception);
        }
    }

    private static StageResult runCheckObjectStage(
        final String uri,
        final HiveConf baseHiveConf,
        final GlobalConfig global,
        final AuthContext authContext,
        final CheckOptions check
    ) {
        long start = System.nanoTime();
        try {
            RpcPayload payload = withClient(uri, baseHiveConf, global, authContext, new ClientCall<RpcPayload>() {
                @Override
                public RpcPayload call(IMetaStoreClient client) throws Exception {
                    if (check.tableName != null) {
                        Table table = client.getTable(check.dbName, check.tableName);
                        Map<String, Object> details = new LinkedHashMap<String, Object>();
                        details.put("db", table.getDbName());
                        details.put("table", table.getTableName());
                        details.put("partitioned", Boolean.valueOf(table.getPartitionKeysSize() > 0));
                        return new RpcPayload(table.getDbName() + "." + table.getTableName(), details);
                    }
                    Database database = client.getDatabase(check.dbName);
                    Map<String, Object> details = new LinkedHashMap<String, Object>();
                    details.put("db", database.getName());
                    details.put("location_uri", database.getLocationUri());
                    return new RpcPayload(database.getName(), details);
                }
            });
            return StageResult.success("object", nanosToMillis(System.nanoTime() - start), payload.summary, payload.details);
        } catch (Exception exception) {
            return StageResult.failure("object", nanosToMillis(System.nanoTime() - start),
                classifyErrorCode(exception), exception);
        }
    }

    private static StageResult runSchemaStage(
        final String uri,
        final HiveConf baseHiveConf,
        final GlobalConfig global,
        final AuthContext authContext,
        final String dbName,
        final String tableName
    ) {
        long start = System.nanoTime();
        try {
            RpcPayload payload = withClient(uri, baseHiveConf, global, authContext, new ClientCall<RpcPayload>() {
                @Override
                public RpcPayload call(IMetaStoreClient client) throws Exception {
                    List<FieldSchema> schema = client.getSchema(dbName, tableName);
                    Map<String, Object> details = new LinkedHashMap<String, Object>();
                    details.put("field_count", Integer.valueOf(schema.size()));
                    List<String> names = new ArrayList<String>();
                    for (FieldSchema field : schema) {
                        names.add(field.getName() + ":" + field.getType());
                    }
                    details.put("fields", names);
                    return new RpcPayload("fields=" + schema.size(), details);
                }
            });
            return StageResult.success("schema", nanosToMillis(System.nanoTime() - start), payload.summary, payload.details);
        } catch (Exception exception) {
            return StageResult.failure("schema", nanosToMillis(System.nanoTime() - start),
                classifyErrorCode(exception), exception);
        }
    }

    private static StageResult runObjectStage(
        final String uri,
        final HiveConf baseHiveConf,
        final GlobalConfig global,
        final AuthContext authContext,
        final ObjectOptions object
    ) {
        long start = System.nanoTime();
        try {
            RpcPayload payload = withClient(uri, baseHiveConf, global, authContext, new ClientCall<RpcPayload>() {
                @Override
                public RpcPayload call(IMetaStoreClient client) throws Exception {
                    return object.check.execute(client, object);
                }
            });
            return StageResult.success("object " + object.check.cliName, nanosToMillis(System.nanoTime() - start),
                payload.summary, payload.details);
        } catch (Exception exception) {
            return StageResult.failure("object " + object.check.cliName, nanosToMillis(System.nanoTime() - start),
                classifyErrorCode(exception), exception);
        }
    }

    private static <T> T withClient(
        final String uri,
        final HiveConf baseHiveConf,
        final GlobalConfig global,
        final AuthContext authContext,
        final ClientCall<T> clientCall
    ) throws Exception {
        return doAs(authContext.ugi, new Callable<T>() {
            @Override
            public T call() throws Exception {
                HiveConf localHiveConf = new HiveConf(baseHiveConf);
                localHiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, uri);
                IMetaStoreClient client = createClient(localHiveConf, global.noRetry);
                try {
                    return clientCall.call(client);
                } finally {
                    client.close();
                }
            }
        });
    }

    private static StageResult connectStage(String uriText, int timeoutSeconds) {
        long start = System.nanoTime();
        try {
            ParsedUri parsedUri = ParsedUri.from(uriText);
            Socket socket = new Socket();
            try {
                socket.connect(new InetSocketAddress(parsedUri.host, parsedUri.port), timeoutSeconds * 1000);
            } finally {
                socket.close();
            }
            Map<String, Object> details = new LinkedHashMap<String, Object>();
            details.put("host", parsedUri.host);
            details.put("port", Integer.valueOf(parsedUri.port));
            return StageResult.success("connect", nanosToMillis(System.nanoTime() - start),
                parsedUri.host + ":" + parsedUri.port, details);
        } catch (Exception exception) {
            return StageResult.failure("connect", nanosToMillis(System.nanoTime() - start),
                classifyErrorCode(exception), exception);
        }
    }

    private static GlobalConfig parseGlobalConfig(CommandLine commandLine, boolean requireUris) {
        List<String> uris = requireUris
            ? parseUris(requireText(commandLine.getOptionValue("uris"), "--uris"))
            : Collections.<String>emptyList();
        AuthMode authMode = AuthMode.from(commandLine.getOptionValue("auth", "auto"));
        String simpleUser = commandLine.getOptionValue("simple-user");
        String principal = commandLine.getOptionValue("principal");
        String keytab = commandLine.getOptionValue("keytab");

        if (authMode == AuthMode.AUTO) {
            if (principal != null || keytab != null) {
                authMode = AuthMode.KERBEROS;
            } else if (simpleUser != null) {
                authMode = AuthMode.SIMPLE;
            }
        }
        if (authMode == AuthMode.SIMPLE && (principal != null || keytab != null)) {
            throw new IllegalArgumentException("--principal/--keytab cannot be used with --auth simple");
        }
        if (authMode == AuthMode.KERBEROS && (principal == null || keytab == null)) {
            throw new IllegalArgumentException("--principal and --keytab are required for --auth kerberos");
        }

        return new GlobalConfig(
            uris,
            commandLine.getOptionValue("conf-dir"),
            optionValues(commandLine, "resource"),
            parseOverrides(optionValues(commandLine, "conf")),
            authMode,
            simpleUser,
            principal,
            keytab,
            parsePositiveInt(commandLine.getOptionValue("timeout", "10"), "--timeout"),
            commandLine.hasOption("no-retry"),
            OutputFormat.from(commandLine.getOptionValue("format", "text")),
            commandLine.hasOption("verbose"),
            commandLine.hasOption("show-secrets")
        );
    }

    private static HiveConfBundle buildHiveConf(GlobalConfig config, String metastoreUris) {
        HiveConf hiveConf = new HiveConf();
        List<String> loadedResources = new ArrayList<String>();
        List<String> missingDefaultResources = new ArrayList<String>();

        if (config.confDir != null) {
            File confDir = new File(config.confDir);
            if (!confDir.isDirectory()) {
                throw new IllegalArgumentException("--conf-dir does not exist or is not a directory: " + config.confDir);
            }
            for (String name : Arrays.asList("core-site.xml", "hdfs-site.xml", "hive-site.xml")) {
                File file = new File(confDir, name);
                if (file.isFile()) {
                    hiveConf.addResource(new Path(file.getAbsolutePath()));
                    loadedResources.add(file.getAbsolutePath());
                } else {
                    missingDefaultResources.add(file.getAbsolutePath());
                }
            }
        }

        for (String resource : config.resources) {
            File file = new File(resource);
            if (!file.isFile()) {
                throw new IllegalArgumentException("Resource file does not exist: " + resource);
            }
            hiveConf.addResource(new Path(file.getAbsolutePath()));
            loadedResources.add(file.getAbsolutePath());
        }

        if (metastoreUris != null) {
            hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, metastoreUris);
        }
        hiveConf.set(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname, String.valueOf(config.timeoutSeconds));
        if (config.simpleUser != null) {
            hiveConf.set("hadoop.user.name", config.simpleUser);
        }
        if (config.noRetry) {
            hiveConf.set(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES.varname, "0");
            hiveConf.set(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES.varname, "0");
            hiveConf.set(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY.varname, "0");
        }
        for (Map.Entry<String, String> entry : config.overrides.entrySet()) {
            hiveConf.set(entry.getKey(), entry.getValue());
        }
        return new HiveConfBundle(hiveConf, loadedResources, missingDefaultResources);
    }

    private static AuthContext buildAuthContext(GlobalConfig config) {
        try {
            switch (config.authMode) {
                case SIMPLE:
                    String simpleUser = config.simpleUser == null ? System.getProperty("user.name") : config.simpleUser;
                    return AuthContext.success(config.authMode, UserGroupInformation.createRemoteUser(simpleUser), simpleUser);
                case KERBEROS:
                    String expandedPrincipal = expandHost(config.principal);
                    UserGroupInformation.loginUserFromKeytab(expandedPrincipal, config.keytab);
                    return AuthContext.success(config.authMode, UserGroupInformation.getLoginUser(), expandedPrincipal);
                case AUTO:
                default:
                    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
                    return AuthContext.success(config.authMode, currentUser, currentUser.getUserName());
            }
        } catch (Exception exception) {
            return AuthContext.failure(config.authMode, exception);
        }
    }

    private static int decideDiagnosticExitCode(AuthContext authContext, List<UriReport> uriReports) {
        if (!authContext.ok) {
            return 12;
        }
        int healthy = 0;
        int connectSuccess = 0;
        int rpcSuccess = 0;
        boolean objectNotFoundOnly = true;
        boolean permissionDeniedOnly = true;

        for (UriReport uriReport : uriReports) {
            if (uriReport.status == Status.OK) {
                healthy++;
            }
            for (StageResult stage : uriReport.stages) {
                if ("connect".equals(stage.name) && stage.status == Status.OK) {
                    connectSuccess++;
                }
                if (stage.name.startsWith("rpc") && stage.status == Status.OK) {
                    rpcSuccess++;
                }
                if (stage.status == Status.FAIL) {
                    objectNotFoundOnly = objectNotFoundOnly && "OBJECT_NOT_FOUND".equals(stage.errorCode);
                    permissionDeniedOnly = permissionDeniedOnly && "PERMISSION_DENIED".equals(stage.errorCode);
                }
            }
        }

        if (healthy == uriReports.size()) {
            return 0;
        }
        if (objectNotFoundOnly && !uriReports.isEmpty()) {
            return 16;
        }
        if (permissionDeniedOnly && !uriReports.isEmpty()) {
            return 17;
        }
        if (connectSuccess == 0) {
            return 14;
        }
        if (rpcSuccess == 0) {
            return 15;
        }
        return 13;
    }

    private static int decideObjectExitCode(AuthContext authContext, List<UriReport> uriReports) {
        int base = decideDiagnosticExitCode(authContext, uriReports);
        if (base == 15) {
            return 16;
        }
        return base;
    }

    private static Status decideDiagnosticStatus(AuthContext authContext, List<UriReport> uriReports) {
        if (!authContext.ok) {
            return Status.FAIL;
        }
        int healthy = 0;
        for (UriReport report : uriReports) {
            if (report.status == Status.OK) {
                healthy++;
            }
        }
        if (healthy == uriReports.size()) {
            return Status.OK;
        }
        if (healthy == 0) {
            return Status.FAIL;
        }
        return Status.DEGRADED;
    }

    private static int decideBenchExitCode(AuthContext authContext, List<BenchTargetReport> targetReports, BenchOptions bench) {
        if (!authContext.ok) {
            return 12;
        }
        boolean anyFailure = false;
        boolean anySuccess = false;
        for (BenchTargetReport report : targetReports) {
            if (report.summary.successCount() > 0) {
                anySuccess = true;
            }
            if (report.summary.failures > 0) {
                anyFailure = true;
            }
            if (bench.successThresholdPct != null && report.summary.successRatio() * 100.0d < bench.successThresholdPct.doubleValue()) {
                return 20;
            }
            if (bench.latencySloMs != null && report.summary.totalP95() > bench.latencySloMs.doubleValue()) {
                return 20;
            }
        }
        if (!anyFailure) {
            return 0;
        }
        if (anySuccess) {
            return 13;
        }
        return 15;
    }

    private static Status decideBenchStatus(AuthContext authContext, List<BenchTargetReport> targetReports, int exitCode) {
        if (!authContext.ok) {
            return Status.FAIL;
        }
        if (exitCode == 0) {
            return Status.OK;
        }
        if (exitCode == 20) {
            return Status.DEGRADED;
        }
        for (BenchTargetReport report : targetReports) {
            if (report.summary.successCount() > 0) {
                return Status.DEGRADED;
            }
        }
        return Status.FAIL;
    }

    private static Map<String, Object> buildDiagnosticJson(
        String commandName,
        Status overall,
        int exitCode,
        GlobalConfig global,
        HiveConfBundle bundle,
        AuthContext authContext,
        List<UriReport> uriReports
    ) {
        Map<String, Object> json = new LinkedHashMap<String, Object>();
        json.put("command", commandName);
        json.put("status", overall.name());
        json.put("exit_code", Integer.valueOf(exitCode));
        json.put("tool", toolMetadata());
        json.put("config", buildConfigMap(global, bundle));
        json.put("auth", authContext.toMap());
        List<Object> uris = new ArrayList<Object>();
        int healthy = 0;
        for (UriReport uriReport : uriReports) {
            uris.add(uriReport.toMap());
            if (uriReport.status == Status.OK) {
                healthy++;
            }
        }
        json.put("uris", uris);
        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        summary.put("healthy_uris", Integer.valueOf(healthy));
        summary.put("unhealthy_uris", Integer.valueOf(uriReports.size() - healthy));
        json.put("summary", summary);
        return json;
    }

    private static Map<String, Object> buildBenchJson(
        Status overall,
        int exitCode,
        GlobalConfig global,
        HiveConfBundle bundle,
        AuthContext authContext,
        BenchOptions bench,
        List<BenchTargetReport> targetReports
    ) {
        Map<String, Object> json = new LinkedHashMap<String, Object>();
        json.put("command", "bench");
        json.put("status", overall.name());
        json.put("exit_code", Integer.valueOf(exitCode));
        json.put("tool", toolMetadata());
        json.put("config", buildConfigMap(global, bundle));
        json.put("auth", authContext.toMap());

        Map<String, Object> benchmark = new LinkedHashMap<String, Object>();
        benchmark.put("rpc", bench.rpc.cliName);
        benchmark.put("warmup", Integer.valueOf(bench.warmupIterations));
        benchmark.put("iterations", Integer.valueOf(bench.measureIterations));
        benchmark.put("concurrency", Integer.valueOf(bench.concurrency));
        benchmark.put("per_uri", Boolean.valueOf(bench.perUri));
        if (bench.dbName != null) {
            benchmark.put("db", bench.dbName);
        }
        if (bench.tableName != null) {
            benchmark.put("table", bench.tableName);
        }
        if (bench.limit != null) {
            benchmark.put("limit", bench.limit);
        }
        List<Object> targets = new ArrayList<Object>();
        for (BenchTargetReport targetReport : targetReports) {
            targets.add(targetReport.toMap());
        }
        benchmark.put("targets", targets);
        json.put("benchmark", benchmark);
        return json;
    }

    private static String renderDiagnosticText(
        String commandName,
        Status overall,
        int exitCode,
        GlobalConfig global,
        HiveConfBundle bundle,
        AuthContext authContext,
        List<UriReport> uriReports
    ) {
        StringBuilder text = new StringBuilder();
        text.append("COMMAND: ").append(commandName).append('\n');
        text.append("STATUS: ").append(overall.name()).append('\n');
        text.append("AUTH: ").append(authContext.render()).append('\n');
        text.append("CONFIG: resources=").append(bundle.loadedResources.size())
            .append(" retry=").append(global.noRetry ? "off" : "on")
            .append(" timeout=").append(global.timeoutSeconds).append("s").append('\n');
        text.append('\n');

        for (UriReport uriReport : uriReports) {
            text.append("URI ").append(uriReport.uri).append('\n');
            for (StageResult stage : uriReport.stages) {
                text.append("  ").append(stage.render(global.verbose)).append('\n');
            }
            text.append('\n');
        }

        int healthy = 0;
        for (UriReport report : uriReports) {
            if (report.status == Status.OK) {
                healthy++;
            }
        }
        text.append("SUMMARY").append('\n');
        text.append("  healthy_uris=").append(healthy).append('\n');
        text.append("  unhealthy_uris=").append(uriReports.size() - healthy).append('\n');
        text.append("  overall_status=").append(overall.name()).append('\n');
        text.append("  exit_code=").append(exitCode).append('\n');
        return text.toString();
    }

    private static String renderBenchText(
        Status overall,
        int exitCode,
        GlobalConfig global,
        HiveConfBundle bundle,
        AuthContext authContext,
        BenchOptions bench,
        List<BenchTargetReport> targetReports
    ) {
        StringBuilder text = new StringBuilder();
        text.append("COMMAND: bench\n");
        text.append("STATUS: ").append(overall.name()).append('\n');
        text.append("AUTH: ").append(authContext.render()).append('\n');
        text.append("CONFIG: resources=").append(bundle.loadedResources.size())
            .append(" retry=").append(global.noRetry ? "off" : "on")
            .append(" timeout=").append(global.timeoutSeconds).append("s").append('\n');
        text.append("RPC: ").append(bench.rpc.cliName).append('\n');
        text.append("WARMUP: ").append(bench.warmupIterations)
            .append(" ITERATIONS: ").append(bench.measureIterations)
            .append(" CONCURRENCY: ").append(bench.concurrency).append('\n');
        if (bench.dbName != null) {
            text.append("SCOPE: db=").append(bench.dbName);
            if (bench.tableName != null) {
                text.append(" table=").append(bench.tableName);
            }
            if (bench.limit != null) {
                text.append(" limit=").append(bench.limit);
            }
            text.append('\n');
        }
        text.append('\n');

        for (BenchTargetReport targetReport : targetReports) {
            text.append("TARGET ").append(targetReport.target).append('\n');
            text.append("  success=").append(targetReport.summary.successCount())
                .append(" failures=").append(targetReport.summary.failures).append('\n');
            text.append("  connect ").append(targetReport.summary.metricText(targetReport.summary.connectLatencies)).append('\n');
            text.append("  rpc ").append(targetReport.summary.metricText(targetReport.summary.rpcLatencies)).append('\n');
            text.append("  total ").append(targetReport.summary.metricText(targetReport.summary.totalLatencies)).append('\n');
            if (!targetReport.summary.failureMessages.isEmpty()) {
                text.append("  failures").append('\n');
                for (String failureMessage : targetReport.summary.failureMessages) {
                    text.append("    ").append(failureMessage).append('\n');
                }
            }
            text.append('\n');
        }

        text.append("SUMMARY").append('\n');
        text.append("  overall_status=").append(overall.name()).append('\n');
        text.append("  exit_code=").append(exitCode).append('\n');
        return text.toString();
    }

    private static Map<String, Object> buildConfigMap(GlobalConfig global, HiveConfBundle bundle) {
        Map<String, Object> config = new LinkedHashMap<String, Object>();
        config.put("uris", new ArrayList<String>(global.uris));
        config.put("auth_mode", global.authMode.name().toLowerCase(Locale.ROOT));
        config.put("retry_enabled", Boolean.valueOf(!global.noRetry));
        config.put("timeout_sec", Integer.valueOf(global.timeoutSeconds));
        config.put("loaded_resources", new ArrayList<String>(bundle.loadedResources));
        config.put("missing_default_resources", new ArrayList<String>(bundle.missingDefaultResources));
        config.put("effective_conf", buildConfigSnapshot(global, bundle));
        return config;
    }

    private static Map<String, String> buildConfigSnapshot(GlobalConfig global, HiveConfBundle bundle) {
        Map<String, String> snapshot = new LinkedHashMap<String, String>();
        snapshot.put("hive.metastore.uris", bundle.hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname));
        snapshot.put("hive.metastore.client.socket.timeout",
            bundle.hiveConf.get(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname));
        snapshot.put("hive.metastore.connect.retries",
            bundle.hiveConf.get(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES.varname));
        snapshot.put("hive.metastore.failure.retries",
            bundle.hiveConf.get(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES.varname));
        for (Map.Entry<String, String> entry : global.overrides.entrySet()) {
            snapshot.put(entry.getKey(), maybeRedact(entry.getKey(), entry.getValue(), global.showSecrets));
        }
        return snapshot;
    }

    private static Map<String, Object> toolMetadata() {
        Map<String, Object> tool = new LinkedHashMap<String, Object>();
        tool.put("name", TOOL_NAME);
        tool.put("version", TOOL_VERSION);
        tool.put("hadoop_version", HADOOP_VERSION);
        tool.put("hive_version", HIVE_VERSION);
        return tool;
    }

    private static String maybeRedact(String key, String value, boolean showSecrets) {
        if (showSecrets) {
            return value;
        }
        String lower = key.toLowerCase(Locale.ROOT);
        if (lower.contains("password") || lower.contains("secret") || lower.contains("keytab")) {
            return "***";
        }
        return value;
    }

    private static String joinUris(List<String> uris) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < uris.size(); i++) {
            if (i > 0) {
                builder.append(',');
            }
            builder.append(uris.get(i));
        }
        return builder.toString();
    }

    private static String expandHost(String principal) {
        if (principal == null || principal.indexOf("_HOST") < 0) {
            return principal;
        }
        try {
            String hostname = InetAddress.getLocalHost().getCanonicalHostName();
            return principal.replace("_HOST", hostname);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Failed to expand _HOST in principal: " + principal, exception);
        }
    }

    private static List<String> parseUris(String value) {
        List<String> uris = new ArrayList<String>();
        for (String part : value.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                ParsedUri.from(trimmed);
                uris.add(trimmed);
            }
        }
        if (uris.isEmpty()) {
            throw new IllegalArgumentException("--uris is required");
        }
        return uris;
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

    private static double parsePercent(String value, String optionName) {
        try {
            double parsed = Double.parseDouble(value);
            if (parsed < 0.0d || parsed > 100.0d) {
                throw new IllegalArgumentException(optionName + " must be between 0 and 100");
            }
            return parsed;
        } catch (NumberFormatException numberFormatException) {
            throw new IllegalArgumentException(optionName + " must be a number: " + value, numberFormatException);
        }
    }

    private static int parseInt(String value, String optionName) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException numberFormatException) {
            throw new IllegalArgumentException(optionName + " must be an integer: " + value, numberFormatException);
        }
    }

    private static double nanosToMillis(long nanos) {
        return nanos / 1_000_000.0d;
    }

    private static double valueOrZero(Double value) {
        return value == null ? 0.0d : value.doubleValue();
    }

    private static String formatMillis(double value) {
        return DECIMAL_FORMAT.format(value) + "ms";
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

    private static StageResult renameStage(StageResult stageResult, String name) {
        return new StageResult(name, stageResult.status, stageResult.latencyMs, stageResult.message,
            stageResult.errorCode, stageResult.details);
    }

    private static String classifyErrorCode(Throwable throwable) {
        Throwable root = rootCause(throwable);
        if (root instanceof UnknownHostException || root instanceof ConnectException) {
            return "NETWORK_UNREACHABLE";
        }
        if (root instanceof SocketTimeoutException) {
            return "NETWORK_TIMEOUT";
        }
        if (root instanceof NoSuchObjectException) {
            return "OBJECT_NOT_FOUND";
        }
        String className = root.getClass().getSimpleName();
        if ("UnknownDBException".equals(className) || "UnknownTableException".equals(className)) {
            return "OBJECT_NOT_FOUND";
        }
        String message = Objects.toString(root.getMessage(), "").toLowerCase(Locale.ROOT);
        if (message.contains("permission") || message.contains("access denied") || message.contains("not authorized")) {
            return "PERMISSION_DENIED";
        }
        if (message.contains("login") || message.contains("kerberos") || message.contains("sasl")) {
            return "AUTH_FAILURE";
        }
        return "RPC_FAILURE";
    }

    private static Throwable rootCause(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private static <T> T doAs(UserGroupInformation ugi, final Callable<T> callable) throws Exception {
        return ugi.doAs(new PrivilegedExceptionAction<T>() {
            @Override
            public T run() throws Exception {
                return callable.call();
            }
        });
    }

    private static void printGlobalHelp(java.io.PrintStream out) {
        PrintWriter writer = new PrintWriter(out, true);
        writer.println("NAME");
        writer.println("    hms-tools - Hive Metastore standalone diagnostics and troubleshooting tool");
        writer.println();
        writer.println("SYNOPSIS");
        writer.println("    hms-tools <command> [options]");
        writer.println("    hms-tools [check-options] --uris <uri[,uri...]>");
        writer.println();
        writer.println("COMMANDS");
        writer.println("    check      Run end-to-end HMS health checks");
        writer.println("    ping       Probe each HMS URI with quick connectivity and lightweight RPC checks");
        writer.println("    object     Inspect a specific database, table, or partition");
        writer.println("    bench      Measure latency distribution for selected HMS RPCs");
        writer.println("    config     Print effective runtime configuration");
        writer.println("    version    Print build and dependency information");
        writer.println("    help       Show help for a command");
        writer.println();
        writer.println("EXAMPLES");
        writer.println("    hms-tools check --uris thrift://hms1:9083,thrift://hms2:9083");
        writer.println("    hms-tools ping --uris thrift://hms1:9083 --count 3 --format json");
        writer.println("    hms-tools object --uris thrift://hms1:9083 --db tpch --table lineitem --check schema");
        writer.println("    hms-tools bench --uris thrift://hms1:9083 --rpc get_table --db tpch --table lineitem");
        writer.println();
        writer.println("Use `hms-tools help <command>` for command-specific help.");
        writer.flush();
    }

    private static void printCommandHelp(CommandKind command, java.io.PrintStream out) {
        if (command == CommandKind.GLOBAL) {
            printGlobalHelp(out);
            return;
        }

        PrintWriter writer = new PrintWriter(out, true);
        HelpFormatter formatter = new HelpFormatter();
        switch (command) {
            case CHECK:
                formatter.printHelp(writer, 120,
                    "hms-tools check --uris thrift://hms1:9083[,thrift://hms2:9083] [options]",
                    "Run a troubleshooting-oriented HMS health check.",
                    buildOptions(command), 2, 4, null, true);
                break;
            case PING:
                formatter.printHelp(writer, 120,
                    "hms-tools ping --uris thrift://hms1:9083[,thrift://hms2:9083] [options]",
                    "Run repeated per-URI connect and lightweight RPC probes.",
                    buildOptions(command), 2, 4, null, true);
                break;
            case OBJECT:
                formatter.printHelp(writer, 120,
                    "hms-tools object --uris thrift://hms1:9083 --db <db> --check <type> [options]",
                    "Inspect object-level metadata and visibility.",
                    buildOptions(command), 2, 4, null, true);
                break;
            case BENCH:
                formatter.printHelp(writer, 120,
                    "hms-tools bench --uris thrift://hms1:9083 --rpc <name> [options]",
                    "Measure latency distribution for selected HMS RPCs.",
                    buildOptions(command), 2, 4, null, true);
                break;
            case CONFIG:
                formatter.printHelp(writer, 120,
                    "hms-tools config --uris thrift://hms1:9083[,thrift://hms2:9083] [options]",
                    "Print effective runtime configuration and auth context.",
                    buildOptions(command), 2, 4, null, true);
                break;
            case VERSION:
                formatter.printHelp(writer, 120,
                    "hms-tools version [--format text|json]",
                    "Print tool version and runtime metadata.",
                    buildOptions(command), 2, 4, null, true);
                break;
            default:
                throw new IllegalArgumentException("Unsupported help command: " + command.cliName);
        }
        writer.flush();
    }

    private interface ClientCall<T> {
        T call(IMetaStoreClient client) throws Exception;
    }

    private enum CommandKind {
        CHECK("check"),
        PING("ping"),
        OBJECT("object"),
        BENCH("bench"),
        CONFIG("config"),
        VERSION("version"),
        GLOBAL("global");

        private final String cliName;

        CommandKind(String cliName) {
            this.cliName = cliName;
        }

        static boolean isCommand(String value) {
            for (CommandKind command : values()) {
                if (command != GLOBAL && command.cliName.equalsIgnoreCase(value)) {
                    return true;
                }
            }
            return false;
        }

        static CommandKind from(String value) {
            for (CommandKind command : values()) {
                if (command.cliName.equalsIgnoreCase(value)) {
                    return command;
                }
            }
            throw new IllegalArgumentException("Unsupported command: " + value);
        }
    }

    private enum OutputFormat {
        TEXT,
        JSON;

        static OutputFormat from(String value) {
            if ("json".equalsIgnoreCase(value)) {
                return JSON;
            }
            if ("text".equalsIgnoreCase(value)) {
                return TEXT;
            }
            throw new IllegalArgumentException("Unsupported format: " + value);
        }
    }

    private enum AuthMode {
        AUTO,
        SIMPLE,
        KERBEROS;

        static AuthMode from(String value) {
            String normalized = value.toLowerCase(Locale.ROOT);
            if ("auto".equals(normalized)) {
                return AUTO;
            }
            if ("simple".equals(normalized)) {
                return SIMPLE;
            }
            if ("kerberos".equals(normalized)) {
                return KERBEROS;
            }
            throw new IllegalArgumentException("Unsupported auth mode: " + value);
        }
    }

    private enum Status {
        OK,
        DEGRADED,
        FAIL
    }

    private enum BasicRpc {
        GET_ALL_DATABASES("get_all_databases") {
            @Override
            RpcPayload execute(IMetaStoreClient client, String dbName) throws Exception {
                List<String> databases = client.getAllDatabases();
                Map<String, Object> details = new LinkedHashMap<String, Object>();
                details.put("database_count", Integer.valueOf(databases.size()));
                details.put("databases", databases);
                return new RpcPayload("databases=" + databases.size(), details);
            }
        },
        GET_DATABASE("get_database") {
            @Override
            RpcPayload execute(IMetaStoreClient client, String dbName) throws Exception {
                if (dbName == null || dbName.trim().isEmpty()) {
                    throw new IllegalArgumentException("--db is required for rpc get_database");
                }
                Database database = client.getDatabase(dbName);
                Map<String, Object> details = new LinkedHashMap<String, Object>();
                details.put("db", database.getName());
                details.put("location_uri", database.getLocationUri());
                return new RpcPayload(database.getName(), details);
            }
        },
        GET_TABLES("get_tables") {
            @Override
            RpcPayload execute(IMetaStoreClient client, String dbName) throws Exception {
                if (dbName == null || dbName.trim().isEmpty()) {
                    throw new IllegalArgumentException("--db is required for rpc get_tables");
                }
                List<String> tables = client.getAllTables(dbName);
                Map<String, Object> details = new LinkedHashMap<String, Object>();
                details.put("db", dbName);
                details.put("table_count", Integer.valueOf(tables.size()));
                details.put("tables", tables);
                return new RpcPayload("tables=" + tables.size(), details);
            }
        };

        private final String cliName;

        BasicRpc(String cliName) {
            this.cliName = cliName;
        }

        abstract RpcPayload execute(IMetaStoreClient client, String dbName) throws Exception;

        static BasicRpc from(String value) {
            for (BasicRpc rpc : values()) {
                if (rpc.cliName.equalsIgnoreCase(value)) {
                    return rpc;
                }
            }
            throw new IllegalArgumentException("Unsupported rpc: " + value);
        }
    }

    private enum ObjectCheck {
        EXISTS("exists") {
            @Override
            RpcPayload execute(IMetaStoreClient client, ObjectOptions options) throws Exception {
                if (options.tableName == null) {
                    Database database = client.getDatabase(options.dbName);
                    Map<String, Object> details = new LinkedHashMap<String, Object>();
                    details.put("db", database.getName());
                    details.put("location_uri", database.getLocationUri());
                    return new RpcPayload("database=" + database.getName(), details);
                }
                if (options.partitionName != null) {
                    Partition partition = client.getPartition(options.dbName, options.tableName, options.partitionName);
                    Map<String, Object> details = new LinkedHashMap<String, Object>();
                    details.put("partition", options.partitionName);
                    details.put("values", partition.getValues());
                    return new RpcPayload("partition=" + options.partitionName, details);
                }
                boolean exists = client.tableExists(options.dbName, options.tableName);
                if (!exists) {
                    throw new NoSuchObjectException("Table does not exist: " + options.dbName + "." + options.tableName);
                }
                Map<String, Object> details = new LinkedHashMap<String, Object>();
                details.put("db", options.dbName);
                details.put("table", options.tableName);
                return new RpcPayload(options.dbName + "." + options.tableName, details);
            }
        },
        SCHEMA("schema") {
            @Override
            RpcPayload execute(IMetaStoreClient client, ObjectOptions options) throws Exception {
                List<FieldSchema> schema = client.getSchema(options.dbName, options.tableName);
                Map<String, Object> details = new LinkedHashMap<String, Object>();
                List<String> fields = new ArrayList<String>();
                for (FieldSchema field : schema) {
                    fields.add(field.getName() + ":" + field.getType());
                }
                details.put("field_count", Integer.valueOf(schema.size()));
                details.put("fields", fields);
                return new RpcPayload("fields=" + schema.size(), details);
            }
        },
        PARTITIONS("partitions") {
            @Override
            RpcPayload execute(IMetaStoreClient client, ObjectOptions options) throws Exception {
                short limit = validatePartitionLimit(options.limit == null ? Integer.valueOf(1000) : options.limit);
                List<String> partitions = client.listPartitionNames(options.dbName, options.tableName, limit);
                Map<String, Object> details = new LinkedHashMap<String, Object>();
                details.put("count", Integer.valueOf(partitions.size()));
                details.put("limit", Integer.valueOf(limit));
                details.put("partitions", partitions);
                return new RpcPayload("partitions=" + partitions.size(), details);
            }
        },
        STATS("stats") {
            @Override
            RpcPayload execute(IMetaStoreClient client, ObjectOptions options) throws Exception {
                if (options.partitionName != null) {
                    Partition partition = client.getPartition(options.dbName, options.tableName, options.partitionName);
                    Map<String, Object> details = new LinkedHashMap<String, Object>();
                    details.put("partition", options.partitionName);
                    details.put("parameters", partition.getParameters());
                    return new RpcPayload("partition_stats=" + partition.getParameters().size(), details);
                }
                Table table = client.getTable(options.dbName, options.tableName);
                Map<String, Object> details = new LinkedHashMap<String, Object>();
                details.put("parameters", table.getParameters());
                return new RpcPayload("table_stats=" + table.getParameters().size(), details);
            }
        };

        private final String cliName;

        ObjectCheck(String cliName) {
            this.cliName = cliName;
        }

        abstract RpcPayload execute(IMetaStoreClient client, ObjectOptions options) throws Exception;

        static ObjectCheck from(String value) {
            for (ObjectCheck check : values()) {
                if (check.cliName.equalsIgnoreCase(value)) {
                    return check;
                }
            }
            throw new IllegalArgumentException("Unsupported object check: " + value);
        }
    }

    private enum BenchRpc {
        GET_TABLE("get_table") {
            @Override
            String execute(IMetaStoreClient client, BenchOptions bench) throws Exception {
                Table table = client.getTable(bench.dbName, bench.tableName);
                return table.getDbName() + "." + table.getTableName();
            }

            @Override
            void validate(BenchOptions bench) {
                requireDbAndTable(bench);
            }
        },
        LIST_PARTITIONS("list_partitions") {
            @Override
            String execute(IMetaStoreClient client, BenchOptions bench) throws Exception {
                short limit = validatePartitionLimit(bench.limit == null ? Integer.valueOf(1000) : bench.limit);
                List<String> partitions = client.listPartitionNames(bench.dbName, bench.tableName, limit);
                return "partitions=" + partitions.size();
            }

            @Override
            void validate(BenchOptions bench) {
                requireDbAndTable(bench);
            }
        },
        GET_ALL_DATABASES("get_all_databases") {
            @Override
            String execute(IMetaStoreClient client, BenchOptions bench) throws Exception {
                List<String> databases = client.getAllDatabases();
                return "databases=" + databases.size();
            }
        },
        GET_DATABASE("get_database") {
            @Override
            String execute(IMetaStoreClient client, BenchOptions bench) throws Exception {
                Database database = client.getDatabase(bench.dbName);
                return database.getName();
            }

            @Override
            void validate(BenchOptions bench) {
                if (bench.dbName == null || bench.dbName.trim().isEmpty()) {
                    throw new IllegalArgumentException("--db is required for rpc get_database");
                }
            }
        };

        private final String cliName;

        BenchRpc(String cliName) {
            this.cliName = cliName;
        }

        abstract String execute(IMetaStoreClient client, BenchOptions bench) throws Exception;

        void validate(BenchOptions bench) {
        }

        static BenchRpc from(String value) {
            for (BenchRpc rpc : values()) {
                if (rpc.cliName.equalsIgnoreCase(value)) {
                    return rpc;
                }
            }
            throw new IllegalArgumentException("Unsupported bench rpc: " + value);
        }

        private static void requireDbAndTable(BenchOptions bench) {
            if (bench.dbName == null || bench.dbName.trim().isEmpty()) {
                throw new IllegalArgumentException("--db is required for rpc " + bench.rpc.cliName);
            }
            if (bench.tableName == null || bench.tableName.trim().isEmpty()) {
                throw new IllegalArgumentException("--table is required for rpc " + bench.rpc.cliName);
            }
        }
    }

    private static short validatePartitionLimit(Integer limit) {
        int value = limit == null ? 1000 : limit.intValue();
        if (value <= 0) {
            throw new IllegalArgumentException("--limit must be > 0");
        }
        if (value > Short.MAX_VALUE) {
            throw new IllegalArgumentException("--limit must be <= " + Short.MAX_VALUE + " for the Hive metastore API");
        }
        return (short) value;
    }

    private static class CommandSelection {
        private final CommandKind command;
        private final CommandKind helpCommand;
        private final CommandLine commandLine;

        private CommandSelection(CommandKind command, CommandKind helpCommand, CommandLine commandLine) {
            this.command = command;
            this.helpCommand = helpCommand;
            this.commandLine = commandLine;
        }
    }

    private static class CommandOutput {
        private final OutputFormat format;
        private final int exitCode;
        private final String text;
        private final Map<String, Object> json;

        private CommandOutput(OutputFormat format, int exitCode, String text, Map<String, Object> json) {
            this.format = format;
            this.exitCode = exitCode;
            this.text = text;
            this.json = json;
        }

        private void print() {
            if (format == OutputFormat.JSON) {
                System.out.println(toJson(json));
            } else {
                System.out.print(text);
                if (!text.endsWith("\n")) {
                    System.out.println();
                }
            }
        }
    }

    private static class GlobalConfig {
        private final List<String> uris;
        private final String confDir;
        private final List<String> resources;
        private final Map<String, String> overrides;
        private final AuthMode authMode;
        private final String simpleUser;
        private final String principal;
        private final String keytab;
        private final int timeoutSeconds;
        private final boolean noRetry;
        private final OutputFormat format;
        private final boolean verbose;
        private final boolean showSecrets;

        private GlobalConfig(
            List<String> uris,
            String confDir,
            List<String> resources,
            Map<String, String> overrides,
            AuthMode authMode,
            String simpleUser,
            String principal,
            String keytab,
            int timeoutSeconds,
            boolean noRetry,
            OutputFormat format,
            boolean verbose,
            boolean showSecrets
        ) {
            this.uris = uris;
            this.confDir = confDir;
            this.resources = resources;
            this.overrides = overrides;
            this.authMode = authMode;
            this.simpleUser = simpleUser;
            this.principal = principal;
            this.keytab = keytab;
            this.timeoutSeconds = timeoutSeconds;
            this.noRetry = noRetry;
            this.format = format;
            this.verbose = verbose;
            this.showSecrets = showSecrets;
        }
    }

    private static class HiveConfBundle {
        private final HiveConf hiveConf;
        private final List<String> loadedResources;
        private final List<String> missingDefaultResources;

        private HiveConfBundle(HiveConf hiveConf, List<String> loadedResources, List<String> missingDefaultResources) {
            this.hiveConf = hiveConf;
            this.loadedResources = loadedResources;
            this.missingDefaultResources = missingDefaultResources;
        }
    }

    private static class AuthContext {
        private final AuthMode mode;
        private final boolean ok;
        private final UserGroupInformation ugi;
        private final String identity;
        private final String errorCode;
        private final String errorMessage;

        private AuthContext(
            AuthMode mode,
            boolean ok,
            UserGroupInformation ugi,
            String identity,
            String errorCode,
            String errorMessage
        ) {
            this.mode = mode;
            this.ok = ok;
            this.ugi = ugi;
            this.identity = identity;
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
        }

        static AuthContext success(AuthMode mode, UserGroupInformation ugi, String identity) {
            return new AuthContext(mode, true, ugi, identity, null, null);
        }

        static AuthContext failure(AuthMode mode, Exception exception) {
            return new AuthContext(mode, false, null, null, classifyErrorCode(exception), exception.getMessage());
        }

        String render() {
            if (ok) {
                return mode.name() + " OK identity=" + identity;
            }
            return mode.name() + " FAIL code=" + errorCode + " message=" + Objects.toString(errorMessage, "");
        }

        Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<String, Object>();
            map.put("mode", mode.name().toLowerCase(Locale.ROOT));
            map.put("ok", Boolean.valueOf(ok));
            if (identity != null) {
                map.put("identity", identity);
            }
            if (errorCode != null) {
                Map<String, Object> error = new LinkedHashMap<String, Object>();
                error.put("code", errorCode);
                error.put("message", errorMessage);
                map.put("error", error);
            }
            return map;
        }
    }

    private static class ParsedUri {
        private final String host;
        private final int port;

        private ParsedUri(String host, int port) {
            this.host = host;
            this.port = port;
        }

        static ParsedUri from(String uriText) {
            try {
                URI uri = new URI(uriText);
                if (!"thrift".equalsIgnoreCase(uri.getScheme())) {
                    throw new IllegalArgumentException("Unsupported URI scheme: " + uriText);
                }
                if (uri.getHost() == null || uri.getPort() <= 0) {
                    throw new IllegalArgumentException("Invalid metastore URI: " + uriText);
                }
                return new ParsedUri(uri.getHost(), uri.getPort());
            } catch (Exception exception) {
                if (exception instanceof IllegalArgumentException) {
                    throw (IllegalArgumentException) exception;
                }
                throw new IllegalArgumentException("Invalid metastore URI: " + uriText, exception);
            }
        }
    }

    private static class StageResult {
        private final String name;
        private final Status status;
        private final Double latencyMs;
        private final String message;
        private final String errorCode;
        private final Map<String, Object> details;

        private StageResult(
            String name,
            Status status,
            Double latencyMs,
            String message,
            String errorCode,
            Map<String, Object> details
        ) {
            this.name = name;
            this.status = status;
            this.latencyMs = latencyMs;
            this.message = message;
            this.errorCode = errorCode;
            this.details = details == null ? Collections.<String, Object>emptyMap() : details;
        }

        static StageResult success(String name, Double latencyMs, String message, Map<String, Object> details) {
            return new StageResult(name, Status.OK, latencyMs, message, null, details);
        }

        static StageResult failure(String name, Double latencyMs, String errorCode, Throwable throwable) {
            Map<String, Object> details = new LinkedHashMap<String, Object>();
            details.put("class", throwable.getClass().getSimpleName());
            details.put("message", Objects.toString(rootCause(throwable).getMessage(), ""));
            return new StageResult(name, Status.FAIL, latencyMs, Objects.toString(throwable.getMessage(), ""), errorCode, details);
        }

        static StageResult failure(String name, String errorCode, Throwable throwable) {
            return failure(name, null, errorCode, throwable);
        }

        String render(boolean verbose) {
            StringBuilder builder = new StringBuilder();
            builder.append(name.toUpperCase(Locale.ROOT)).append(": ").append(status.name());
            if (latencyMs != null) {
                builder.append(' ').append(formatMillis(latencyMs.doubleValue()));
            }
            if (message != null && !message.isEmpty()) {
                builder.append(' ').append(message);
            }
            if (errorCode != null) {
                builder.append(" code=").append(errorCode);
            }
            if (verbose && !details.isEmpty()) {
                builder.append(" details=").append(details);
            }
            return builder.toString();
        }

        Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<String, Object>();
            map.put("name", name);
            map.put("status", status.name());
            if (latencyMs != null) {
                map.put("latency_ms", latencyMs);
            }
            if (message != null) {
                map.put("message", message);
            }
            if (errorCode != null) {
                Map<String, Object> error = new LinkedHashMap<String, Object>();
                error.put("code", errorCode);
                error.put("details", details);
                map.put("error", error);
            } else if (!details.isEmpty()) {
                map.put("details", details);
            }
            return map;
        }
    }

    private static class UriReport {
        private final String uri;
        private final List<StageResult> stages = new ArrayList<StageResult>();
        private Status status = Status.OK;

        private UriReport(String uri) {
            this.uri = uri;
        }

        private void add(StageResult stageResult) {
            stages.add(stageResult);
            if (stageResult.status != Status.OK) {
                status = Status.FAIL;
            }
        }

        private Status lastStatus() {
            if (stages.isEmpty()) {
                return Status.OK;
            }
            return stages.get(stages.size() - 1).status;
        }

        private Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<String, Object>();
            map.put("uri", uri);
            map.put("status", status.name());
            List<Object> stageMaps = new ArrayList<Object>();
            for (StageResult stage : stages) {
                stageMaps.add(stage.toMap());
            }
            map.put("stages", stageMaps);
            return map;
        }
    }

    private static class RpcPayload {
        private final String summary;
        private final Map<String, Object> details;

        private RpcPayload(String summary, Map<String, Object> details) {
            this.summary = summary;
            this.details = details;
        }
    }

    private static class CheckOptions {
        private final BasicRpc rpc;
        private final String dbName;
        private final String tableName;
        private final boolean deep;
        private final boolean failFast;

        private CheckOptions(BasicRpc rpc, String dbName, String tableName, boolean deep, boolean failFast) {
            this.rpc = rpc;
            this.dbName = dbName;
            this.tableName = tableName;
            this.deep = deep;
            this.failFast = failFast;
        }

        static CheckOptions from(CommandLine commandLine) {
            BasicRpc rpc = BasicRpc.from(commandLine.getOptionValue("rpc", "get_all_databases"));
            String dbName = commandLine.getOptionValue("db");
            String tableName = commandLine.getOptionValue("table");
            if (tableName != null && (dbName == null || dbName.trim().isEmpty())) {
                throw new IllegalArgumentException("--db is required when --table is provided");
            }
            if ((rpc == BasicRpc.GET_DATABASE || rpc == BasicRpc.GET_TABLES)
                && (dbName == null || dbName.trim().isEmpty())) {
                throw new IllegalArgumentException("--db is required for rpc " + rpc.cliName);
            }
            return new CheckOptions(rpc, dbName, tableName, commandLine.hasOption("deep"), commandLine.hasOption("fail-fast"));
        }
    }

    private static class PingOptions {
        private final BasicRpc rpc;
        private final String dbName;
        private final int count;
        private final int intervalMs;
        private final boolean parallel;

        private PingOptions(BasicRpc rpc, String dbName, int count, int intervalMs, boolean parallel) {
            this.rpc = rpc;
            this.dbName = dbName;
            this.count = count;
            this.intervalMs = intervalMs;
            this.parallel = parallel;
        }

        static PingOptions from(CommandLine commandLine) {
            BasicRpc rpc = BasicRpc.from(commandLine.getOptionValue("rpc", "get_all_databases"));
            String dbName = commandLine.getOptionValue("db");
            if ((rpc == BasicRpc.GET_DATABASE || rpc == BasicRpc.GET_TABLES)
                && (dbName == null || dbName.trim().isEmpty())) {
                throw new IllegalArgumentException("--db is required for rpc " + rpc.cliName);
            }
            return new PingOptions(
                rpc,
                dbName,
                parsePositiveInt(commandLine.getOptionValue("count", "1"), "--count"),
                parseNonNegativeInt(commandLine.getOptionValue("interval-ms", "0"), "--interval-ms"),
                commandLine.hasOption("parallel")
            );
        }
    }

    private static class ObjectOptions {
        private final String dbName;
        private final String tableName;
        private final String partitionName;
        private final ObjectCheck check;
        private final Integer limit;

        private ObjectOptions(String dbName, String tableName, String partitionName, ObjectCheck check, Integer limit) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.partitionName = partitionName;
            this.check = check;
            this.limit = limit;
        }

        static ObjectOptions from(CommandLine commandLine) {
            String dbName = requireText(commandLine.getOptionValue("db"), "--db");
            String tableName = commandLine.getOptionValue("table");
            String partitionName = commandLine.getOptionValue("partition");
            ObjectCheck check = ObjectCheck.from(requireText(commandLine.getOptionValue("check"), "--check"));
            Integer limit = commandLine.hasOption("limit")
                ? Integer.valueOf(parsePositiveInt(commandLine.getOptionValue("limit"), "--limit"))
                : null;

            if ((check == ObjectCheck.SCHEMA || check == ObjectCheck.PARTITIONS || check == ObjectCheck.STATS)
                && (tableName == null || tableName.trim().isEmpty())) {
                throw new IllegalArgumentException("--table is required for check " + check.cliName);
            }
            if (partitionName != null && (tableName == null || tableName.trim().isEmpty())) {
                throw new IllegalArgumentException("--table is required when --partition is provided");
            }
            return new ObjectOptions(dbName, tableName, partitionName, check, limit);
        }
    }

    private static class BenchOptions {
        private final BenchRpc rpc;
        private final String dbName;
        private final String tableName;
        private final Integer limit;
        private final int warmupIterations;
        private final int measureIterations;
        private final int concurrency;
        private final boolean perUri;
        private final boolean histogram;
        private final Double successThresholdPct;
        private final Double latencySloMs;
        private final boolean verbose;

        private BenchOptions(
            BenchRpc rpc,
            String dbName,
            String tableName,
            Integer limit,
            int warmupIterations,
            int measureIterations,
            int concurrency,
            boolean perUri,
            boolean histogram,
            Double successThresholdPct,
            Double latencySloMs,
            boolean verbose
        ) {
            this.rpc = rpc;
            this.dbName = dbName;
            this.tableName = tableName;
            this.limit = limit;
            this.warmupIterations = warmupIterations;
            this.measureIterations = measureIterations;
            this.concurrency = concurrency;
            this.perUri = perUri;
            this.histogram = histogram;
            this.successThresholdPct = successThresholdPct;
            this.latencySloMs = latencySloMs;
            this.verbose = verbose;
        }

        static BenchOptions from(CommandLine commandLine) {
            BenchRpc rpc = BenchRpc.from(requireText(commandLine.getOptionValue("rpc"), "--rpc"));
            BenchOptions bench = new BenchOptions(
                rpc,
                commandLine.getOptionValue("db"),
                commandLine.getOptionValue("table"),
                commandLine.hasOption("limit")
                    ? Integer.valueOf(parsePositiveInt(commandLine.getOptionValue("limit"), "--limit"))
                    : null,
                parseNonNegativeInt(commandLine.getOptionValue("warmup", "3"), "--warmup"),
                parsePositiveInt(commandLine.getOptionValue("iterations", "20"), "--iterations"),
                parsePositiveInt(commandLine.getOptionValue("concurrency", "1"), "--concurrency"),
                commandLine.hasOption("per-uri"),
                commandLine.hasOption("histogram"),
                commandLine.hasOption("success-threshold")
                    ? Double.valueOf(parsePercent(commandLine.getOptionValue("success-threshold"), "--success-threshold"))
                    : null,
                commandLine.hasOption("latency-slo-ms")
                    ? Double.valueOf(parsePositiveInt(commandLine.getOptionValue("latency-slo-ms"), "--latency-slo-ms"))
                    : null,
                commandLine.hasOption("verbose")
            );
            rpc.validate(bench);
            if (bench.limit != null) {
                validatePartitionLimit(bench.limit);
            }
            return bench;
        }
    }

    private static class ProbeTask implements Callable<ProbeResult> {
        private final BenchOptions bench;
        private final HiveConf hiveConf;
        private final UserGroupInformation ugi;
        private final boolean noRetry;

        private ProbeTask(BenchOptions bench, HiveConf hiveConf, UserGroupInformation ugi, boolean noRetry) {
            this.bench = bench;
            this.hiveConf = hiveConf;
            this.ugi = ugi;
            this.noRetry = noRetry;
        }

        @Override
        public ProbeResult call() throws Exception {
            return doAs(ugi, new Callable<ProbeResult>() {
                @Override
                public ProbeResult call() throws Exception {
                    return runSingleProbe(bench, hiveConf, noRetry);
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
        private final BenchOptions bench;
        private final List<Double> connectLatencies = new ArrayList<Double>();
        private final List<Double> rpcLatencies = new ArrayList<Double>();
        private final List<Double> totalLatencies = new ArrayList<Double>();
        private final List<String> failureMessages = new ArrayList<String>();
        private int failures;

        private ProbeSummary(BenchOptions bench) {
            this.bench = bench;
        }

        private void recordSuccess(ProbeResult result) {
            connectLatencies.add(result.connectMs);
            rpcLatencies.add(result.rpcMs);
            totalLatencies.add(result.totalMs);
        }

        private void recordFailure(Throwable throwable) {
            failures++;
            failureMessages.add(classifyErrorCode(throwable) + ": " + throwable.getClass().getSimpleName()
                + ": " + Objects.toString(rootCause(throwable).getMessage(), ""));
        }

        private int successCount() {
            return totalLatencies.size();
        }

        private double successRatio() {
            int total = successCount() + failures;
            if (total == 0) {
                return 0.0d;
            }
            return successCount() / (double) total;
        }

        private double totalP95() {
            List<Double> copy = new ArrayList<Double>(totalLatencies);
            Collections.sort(copy);
            return percentile(copy, 0.95d);
        }

        private String metricText(List<Double> latencies) {
            if (latencies.isEmpty()) {
                return "no successful samples";
            }
            List<Double> copy = new ArrayList<Double>(latencies);
            Collections.sort(copy);
            return "min=" + formatMillis(percentile(copy, 0.0d))
                + " p50=" + formatMillis(percentile(copy, 0.50d))
                + " p95=" + formatMillis(percentile(copy, 0.95d))
                + " p99=" + formatMillis(percentile(copy, 0.99d))
                + " max=" + formatMillis(percentile(copy, 1.0d))
                + " avg=" + formatMillis(average(copy));
        }

        private Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<String, Object>();
            map.put("success", Integer.valueOf(successCount()));
            map.put("failures", Integer.valueOf(failures));
            map.put("success_ratio", Double.valueOf(successRatio()));
            map.put("connect_ms", metricMap(connectLatencies));
            map.put("rpc_ms", metricMap(rpcLatencies));
            map.put("total_ms", metricMap(totalLatencies));
            map.put("failure_messages", new ArrayList<String>(failureMessages));
            return map;
        }

        private Map<String, Object> metricMap(List<Double> latencies) {
            Map<String, Object> map = new LinkedHashMap<String, Object>();
            if (latencies.isEmpty()) {
                return map;
            }
            List<Double> copy = new ArrayList<Double>(latencies);
            Collections.sort(copy);
            map.put("min", Double.valueOf(percentile(copy, 0.0d)));
            map.put("p50", Double.valueOf(percentile(copy, 0.50d)));
            map.put("p95", Double.valueOf(percentile(copy, 0.95d)));
            map.put("p99", Double.valueOf(percentile(copy, 0.99d)));
            map.put("max", Double.valueOf(percentile(copy, 1.0d)));
            map.put("avg", Double.valueOf(average(copy)));
            return map;
        }

        private double percentile(List<Double> values, double fraction) {
            if (values.isEmpty()) {
                return 0.0d;
            }
            if (fraction <= 0.0d) {
                return values.get(0).doubleValue();
            }
            if (fraction >= 1.0d) {
                return values.get(values.size() - 1).doubleValue();
            }
            int index = (int) Math.ceil(fraction * values.size()) - 1;
            if (index < 0) {
                index = 0;
            }
            if (index >= values.size()) {
                index = values.size() - 1;
            }
            return values.get(index).doubleValue();
        }

        private double average(List<Double> values) {
            if (values.isEmpty()) {
                return 0.0d;
            }
            double sum = 0.0d;
            for (Double value : values) {
                sum += value.doubleValue();
            }
            return sum / values.size();
        }
    }

    private static class BenchTargetReport {
        private final String target;
        private final ProbeSummary summary;

        private BenchTargetReport(String target, ProbeSummary summary) {
            this.target = target;
            this.summary = summary;
        }

        private Map<String, Object> toMap() {
            Map<String, Object> map = new LinkedHashMap<String, Object>();
            map.put("target", target);
            map.put("summary", summary.toMap());
            return map;
        }
    }

    private static String toJson(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String) {
            return "\"" + escapeJson((String) value) + "\"";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }
        if (value instanceof Map) {
            StringBuilder builder = new StringBuilder();
            builder.append('{');
            boolean first = true;
            for (Object entryObject : ((Map<?, ?>) value).entrySet()) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) entryObject;
                if (!first) {
                    builder.append(',');
                }
                first = false;
                builder.append(toJson(String.valueOf(entry.getKey())));
                builder.append(':');
                builder.append(toJson(entry.getValue()));
            }
            builder.append('}');
            return builder.toString();
        }
        if (value instanceof Iterable) {
            StringBuilder builder = new StringBuilder();
            builder.append('[');
            boolean first = true;
            for (Object item : (Iterable<?>) value) {
                if (!first) {
                    builder.append(',');
                }
                first = false;
                builder.append(toJson(item));
            }
            builder.append(']');
            return builder.toString();
        }
        return toJson(String.valueOf(value));
    }

    private static String escapeJson(String value) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\':
                    builder.append("\\\\");
                    break;
                case '"':
                    builder.append("\\\"");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    if (ch < 0x20) {
                        builder.append(String.format("\\u%04x", Integer.valueOf(ch)));
                    } else {
                        builder.append(ch);
                    }
            }
        }
        return builder.toString();
    }
}
