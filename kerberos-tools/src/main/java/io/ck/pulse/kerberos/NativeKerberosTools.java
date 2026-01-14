/*
 * Copyright (c) 2025 Calvin Kirs
 *
 * Licensed under the MIT License.
 * See the LICENSE file in the project root for license information.
 */
package io.ck.pulse.kerberos;

import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.keytab.KeytabEntry;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
import io.ck.pulse.common.ConfigUtils;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kerberos Connectivity Diagnostic Tool for Apache Doris HMS Catalog
 * 
 * This tool performs comprehensive Kerberos authentication diagnostics:
 * 1. Validates krb5.conf structure and settings
 * 2. Checks KDC network reachability
 * 3. Validates keytab file and principal
 * 4. Performs actual Kerberos authentication test
 * 5. Generates detailed diagnostic report
 */
public class NativeKerberosTools {

    // ANSI color codes for terminal output
    private static final String RESET = "\u001B[0m";
    private static final String GREEN = "\u001B[32m";
    private static final String RED = "\u001B[31m";
    private static final String YELLOW = "\u001B[33m";
    private static final String CYAN = "\u001B[36m";
    private static final String BOLD = "\u001B[1m";

    // Diagnostic result storage
    private static final List<DiagnosticResult> diagnosticResults = new ArrayList<>();

    public static void main(String[] args) {
        try {
            System.setOut(new PrintStream(System.out, true, StandardCharsets.UTF_8.toString()));
            System.setErr(new PrintStream(System.err, true, StandardCharsets.UTF_8.toString()));
        } catch (Exception e) {
            // Fallback to default encoding
        }

        printBanner();

        // Load configuration
        Properties properties = ConfigUtils.loadConfig();
        String keytabPath = properties.getProperty("keytabPath");
        String principal = properties.getProperty("principal");
        String krb5ConfPath = properties.getProperty("krb5ConfPath");
        // Validate required parameters
        if (!validateRequiredParams(keytabPath, principal, krb5ConfPath)) {
            printFinalReport();
            System.exit(1);
        }

        // Run all diagnostic checks
        runDiagnostics(krb5ConfPath, keytabPath, principal);

        // Print final report
        printFinalReport();

        // Exit with appropriate code
        boolean hasErrors = diagnosticResults.stream().anyMatch(r -> r.status == Status.FAILED);
        System.exit(hasErrors ? 1 : 0);
    }
    
    private static void printBanner() {
        System.out.println(CYAN + BOLD);
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║      Doris Kerberos Connectivity Diagnostic Tool             ║");
        System.out.println("║                        v1.0.0                                ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println(RESET);
    }

    private static boolean validateRequiredParams(String keytabPath, String principal, String krb5ConfPath) {
        printSection("Configuration Validation");
        boolean valid = true;

        if (keytabPath == null || keytabPath.isEmpty()) {
            addResult("keytabPath", Status.FAILED, "keytabPath is not configured in config.properties");
            valid = false;
        } else {
            addResult("keytabPath", Status.PASSED, "Configured: " + keytabPath);
        }

        if (principal == null || principal.isEmpty()) {
            addResult("principal", Status.FAILED, "principal is not configured in config.properties");
            valid = false;
        } else {
            addResult("principal", Status.PASSED, "Configured: " + principal);
        }

        if (krb5ConfPath == null || krb5ConfPath.isEmpty()) {
            addResult("krb5ConfPath", Status.FAILED, "krb5ConfPath is not configured in config.properties");
            valid = false;
        } else {
            addResult("krb5ConfPath", Status.PASSED, "Configured: " + krb5ConfPath);
        }

        return valid;
    }

    private static void runDiagnostics(String krb5ConfPath, String keytabPath, String principal) {
        // Step 1: Validate krb5.conf
        Krb5Config krb5Config = validateKrb5Conf(krb5ConfPath);

        // Step 2: Check KDC reachability
        if (krb5Config != null) {
            checkKdcReachability(krb5Config);
        }

        // Step 3: Validate keytab file
        boolean keytabValid = validateKeytab(keytabPath, principal);

        // Step 4: Perform Kerberos authentication
        if (keytabValid && krb5Config != null) {
            performKerberosAuth(krb5ConfPath, keytabPath, principal);
        }
    }

    // ==================== Step 1: krb5.conf Validation ====================
    
    private static Krb5Config validateKrb5Conf(String krb5ConfPath) {
        printSection("Step 1: krb5.conf Validation");

        File krb5File = new File(krb5ConfPath);
        if (!krb5File.exists()) {
            addResult("krb5.conf exists", Status.FAILED, 
                "File not found: " + krb5ConfPath + "\n" +
                "   Suggestion: Verify the file path and ensure the file exists");
            return null;
        }
        addResult("krb5.conf exists", Status.PASSED, "File found: " + krb5ConfPath);

        if (!krb5File.canRead()) {
            addResult("krb5.conf readable", Status.FAILED, 
                "Cannot read file: " + krb5ConfPath + "\n" +
                "   Suggestion: Check file permissions (chmod 644)");
            return null;
        }
        addResult("krb5.conf readable", Status.PASSED, "File is readable");

        // Parse krb5.conf manually for better error reporting
        Krb5Config config = parseKrb5Conf(krb5ConfPath);
        if (config == null) {
            return null;
        }

        // Validate default_realm
        if (config.defaultRealm == null || config.defaultRealm.isEmpty()) {
            addResult("default_realm", Status.FAILED, 
                "default_realm not found in [libdefaults] section\n" +
                "   Suggestion: Add 'default_realm = YOUR.REALM' in [libdefaults]");
        } else {
            addResult("default_realm", Status.PASSED, "Value: " + config.defaultRealm);
        }

        // Validate KDC configuration
        if (config.kdcAddresses.isEmpty()) {
            addResult("KDC configuration", Status.FAILED, 
                "No KDC servers found in [realms] section\n" +
                "   Suggestion: Add KDC configuration under [realms] -> " + config.defaultRealm);
        } else {
            addResult("KDC configuration", Status.PASSED, 
                "Found " + config.kdcAddresses.size() + " KDC server(s): " + config.kdcAddresses);
        }

        // Check UDP preference limit (important for Doris)
        if (config.udpPreferenceLimit > 1) {
            addResult("udp_preference_limit", Status.WARNING, 
                "Current value: " + config.udpPreferenceLimit + "\n" +
                "   Recommendation: For Doris, set 'udp_preference_limit = 1' in [libdefaults]\n" +
                "   to force TCP connections to KDC for better reliability");
        } else if (config.udpPreferenceLimit == 1) {
            addResult("udp_preference_limit", Status.PASSED, "Correctly set to 1 (TCP forced)");
        } else {
            addResult("udp_preference_limit", Status.INFO, "Using default value (UDP preferred)");
        }

        // Check renewable and forwardable
        if (!config.renewable) {
            addResult("renewable", Status.INFO, 
                "Ticket renewal not enabled. Consider adding 'renewable = true'");
        }

        return config;
    }

    private static Krb5Config parseKrb5Conf(String path) {
        Krb5Config config = new Krb5Config();
        config.udpPreferenceLimit = 9999; // Default

        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            String currentSection = "";
            String currentRealm = "";
            
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                
                // Skip comments and empty lines
                if (line.isEmpty() || line.startsWith("#") || line.startsWith(";")) {
                    continue;
                }

                // Section detection
                if (line.startsWith("[") && line.endsWith("]")) {
                    currentSection = line.substring(1, line.length() - 1).toLowerCase();
                    continue;
                }

                // Parse key-value pairs
                if (currentSection.equals("libdefaults")) {
                    if (line.contains("default_realm")) {
                        config.defaultRealm = extractValue(line);
                    } else if (line.contains("udp_preference_limit")) {
                        try {
                            config.udpPreferenceLimit = Integer.parseInt(extractValue(line));
                        } catch (NumberFormatException e) {
                            // Keep default
                        }
                    } else if (line.contains("renewable") && line.contains("true")) {
                        config.renewable = true;
                    } else if (line.contains("forwardable") && line.contains("true")) {
                        config.forwardable = true;
                    }
                } else if (currentSection.equals("realms")) {
                    // Detect realm block
                    if (line.contains("=") && line.contains("{")) {
                        currentRealm = line.split("=")[0].trim();
                    } else if (line.toLowerCase().contains("kdc")) {
                        String kdcValue = extractValue(line);
                        if (kdcValue != null && !kdcValue.isEmpty()) {
                            config.kdcAddresses.add(kdcValue);
                        }
                    } else if (line.toLowerCase().contains("admin_server")) {
                        config.adminServer = extractValue(line);
                    }
                }
            }

            addResult("krb5.conf parsing", Status.PASSED, "File parsed successfully");
            return config;

        } catch (IOException e) {
            addResult("krb5.conf parsing", Status.FAILED, 
                "Failed to parse file: " + e.getMessage() + "\n" +
                "   Suggestion: Check if the file is valid and properly formatted");
            return null;
        }
    }

    private static String extractValue(String line) {
        int equalsIndex = line.indexOf('=');
        if (equalsIndex > 0 && equalsIndex < line.length() - 1) {
            String value = line.substring(equalsIndex + 1).trim();
            // Remove quotes and braces
            value = value.replaceAll("[\"'{}]", "").trim();
            return value;
        }
        return "";
    }

    // ==================== Step 2: KDC Reachability ====================

    private static void checkKdcReachability(Krb5Config config) {
        printSection("Step 2: KDC Network Reachability");

        if (config.kdcAddresses.isEmpty()) {
            addResult("KDC reachability", Status.SKIPPED, "No KDC addresses to check");
            return;
        }

        for (String kdcAddress : config.kdcAddresses) {
            checkSingleKdc(kdcAddress);
        }
    }

    private static void checkSingleKdc(String kdcAddress) {
        String host;
        int port = 88; // Default Kerberos port

        // Handle different address formats (host:port, host, [ipv6]:port)
        if (kdcAddress.startsWith("[")) {
            // IPv6 format: [::1]:88
            int bracketEnd = kdcAddress.indexOf(']');
            host = kdcAddress.substring(1, bracketEnd);
            if (kdcAddress.length() > bracketEnd + 2) {
                port = Integer.parseInt(kdcAddress.substring(bracketEnd + 2));
            }
        } else if (kdcAddress.contains(":") && !kdcAddress.contains("::")) {
            // IPv4 with port: host:port
            String[] parts = kdcAddress.split(":");
            host = parts[0];
            if (parts.length > 1) {
                try {
                    port = Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {
                    // Keep default port
                }
            }
        } else {
            // Just hostname or IPv6 without port
            host = kdcAddress;
        }

        // DNS resolution check
        try {
            InetAddress address = InetAddress.getByName(host);
            addResult("DNS resolution [" + host + "]", Status.PASSED, 
                "Resolved to: " + address.getHostAddress());
        } catch (Exception e) {
            addResult("DNS resolution [" + host + "]", Status.FAILED, 
                "Cannot resolve hostname: " + host + "\n" +
                "   Suggestion: Check DNS configuration or use IP address directly\n" +
                "   Error: " + e.getMessage());
            return;
        }

        // TCP connectivity check
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5000);
            addResult("TCP connection [" + host + ":" + port + "]", Status.PASSED, 
                "KDC is reachable on port " + port);
        } catch (IOException e) {
            addResult("TCP connection [" + host + ":" + port + "]", Status.FAILED, 
                "Cannot connect to KDC\n" +
                "   Suggestion: Check firewall rules, ensure port " + port + " is open\n" +
                "   Error: " + e.getMessage());
        }
    }

    // ==================== Step 3: Keytab Validation ====================

    private static boolean validateKeytab(String keytabPath, String expectedPrincipal) {
        printSection("Step 3: Keytab File Validation");

        File keytabFile = new File(keytabPath);
        
        // File existence check
        if (!keytabFile.exists()) {
            addResult("Keytab exists", Status.FAILED, 
                "File not found: " + keytabPath + "\n" +
                "   Suggestion: Verify the keytab file path in config.properties");
            return false;
        }
        addResult("Keytab exists", Status.PASSED, "File found: " + keytabPath);

        // File readability check
        if (!keytabFile.canRead()) {
            addResult("Keytab readable", Status.FAILED, 
                "Cannot read file: " + keytabPath + "\n" +
                "   Suggestion: Check file permissions (chmod 400 or 600)");
            return false;
        }
        addResult("Keytab readable", Status.PASSED, "File is readable");

        // Parse keytab content
        try {
            Keytab keytab = Keytab.loadKeytab(keytabFile);
            List<PrincipalName> principals = keytab.getPrincipals();

            if (principals.isEmpty()) {
                addResult("Keytab content", Status.FAILED, 
                    "Keytab file is empty or corrupted\n" +
                    "   Suggestion: Regenerate the keytab file using kadmin or ktutil");
                return false;
            }

            // List all principals in keytab
            StringBuilder principalList = new StringBuilder();
            principalList.append("Principals in keytab:\n");
            boolean foundExpectedPrincipal = false;
            
            for (PrincipalName principal : principals) {
                String principalName = principal.getName();
                principalList.append("   - ").append(principalName);
                
                if (principalName.equals(expectedPrincipal)) {
                    foundExpectedPrincipal = true;
                    principalList.append(" [MATCH]");
                }
                principalList.append("\n");

                // Show key details
                List<KeytabEntry> entries = keytab.getKeytabEntries(principal);
                for (KeytabEntry entry : entries) {
                    String encType = entry.getKey() != null && entry.getKey().getKeyType() != null 
                        ? entry.getKey().getKeyType().getName() : "unknown";
                    int kvno = entry.getKvno();
                    principalList.append("      EncType: ").append(encType)
                                .append(", KVNO: ").append(kvno).append("\n");
                }
            }

            addResult("Keytab content", Status.PASSED, principalList.toString().trim());

            // Check if expected principal exists
            if (foundExpectedPrincipal) {
                addResult("Principal match", Status.PASSED, 
                    "Expected principal found: " + expectedPrincipal);
            } else {
                addResult("Principal match", Status.FAILED, 
                    "Expected principal NOT found in keytab!\n" +
                    "   Expected: " + expectedPrincipal + "\n" +
                    "   Suggestion: Verify the principal name or regenerate keytab with correct principal\n" +
                    "   Common issues:\n" +
                    "     - Case sensitivity mismatch\n" +
                    "     - Missing realm suffix (@REALM.COM)\n" +
                    "     - Hostname mismatch in service principal");
                return false;
            }

            return true;

        } catch (IOException e) {
            addResult("Keytab parsing", Status.FAILED, 
                "Failed to parse keytab file\n" +
                "   Error: " + e.getMessage() + "\n" +
                "   Suggestion: The keytab file may be corrupted. Try regenerating it.");
            return false;
        }
    }

    // ==================== Step 4: Kerberos Authentication ====================

    private static void performKerberosAuth(String krb5ConfPath, String keytabPath, String principal) {
        printSection("Step 4: Kerberos Authentication Test");

        // Set system properties
        System.setProperty("java.security.krb5.conf", krb5ConfPath);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        
        // Enable debug if needed (set to false for cleaner output)
        System.setProperty("sun.security.krb5.debug", "false");

        try {
            // Create programmatic JAAS configuration - NO EXTERNAL FILE NEEDED!
            Configuration jaasConfig = createJaasConfiguration(keytabPath, principal);
            
            // Perform login
            LoginContext loginContext = new LoginContext("KerberosLogin", null, null, jaasConfig);
            
            System.out.println(CYAN + "  Attempting Kerberos authentication..." + RESET);
            loginContext.login();

            Subject subject = loginContext.getSubject();
            
            addResult("Kerberos authentication", Status.PASSED, 
                "Successfully authenticated as: " + principal + "\n" +
                "   Subject principals: " + subject.getPrincipals());

            // Clean up
            loginContext.logout();
            addResult("Kerberos logout", Status.PASSED, "Successfully logged out");

        } catch (LoginException e) {
            String errorMsg = analyzeLoginException(e);
            addResult("Kerberos authentication", Status.FAILED, errorMsg);
        }
    }

    /**
     * Creates JAAS configuration programmatically - NO EXTERNAL jass.conf FILE NEEDED!
     * This eliminates the need for users to provide a separate JAAS configuration file.
     */
    private static Configuration createJaasConfiguration(String keytabPath, String principal) {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                Map<String, String> options = new HashMap<>();
                options.put("useKeyTab", "true");
                options.put("keyTab", keytabPath);
                options.put("principal", principal);
                options.put("storeKey", "true");
                options.put("doNotPrompt", "true");
                options.put("useTicketCache", "false");
                options.put("refreshKrb5Config", "true");
                options.put("isInitiator", "true");

                return new AppConfigurationEntry[]{
                    new AppConfigurationEntry(
                        "com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options
                    )
                };
            }
        };
    }

    /**
     * Analyzes LoginException to provide user-friendly error messages and suggestions
     */
    private static String analyzeLoginException(LoginException e) {
        String message = e.getMessage() != null ? e.getMessage() : "";
        String cause = e.getCause() != null ? e.getCause().getMessage() : "";
        String fullError = message + " " + cause;
        
        StringBuilder result = new StringBuilder();
        result.append("Authentication failed\n");
        result.append("   Error: ").append(message).append("\n");

        // Analyze common Kerberos errors
        if (fullError.contains("Clock skew too great") || fullError.contains("clock skew")) {
            result.append("   Root Cause: TIME SYNCHRONIZATION ERROR\n");
            result.append("   The time difference between your machine and KDC is too large (>5 minutes)\n");
            result.append("   Suggestions:\n");
            result.append("     1. Sync your system clock: sudo ntpdate <ntp-server>\n");
            result.append("     2. Or use: timedatectl set-ntp true\n");
            result.append("     3. Check KDC server time as well\n");
        } else if (fullError.contains("Pre-authentication") || fullError.contains("Preauthentication")) {
            result.append("   Root Cause: PRE-AUTHENTICATION FAILED\n");
            result.append("   Suggestions:\n");
            result.append("     1. Verify the keytab was generated with the correct password\n");
            result.append("     2. Check if the principal exists in KDC: kadmin -q 'getprinc <principal>'\n");
            result.append("     3. Regenerate keytab: kadmin -q 'ktadd -k <keytab> <principal>'\n");
        } else if (fullError.contains("Cannot find key") || fullError.contains("No key")) {
            result.append("   Root Cause: ENCRYPTION TYPE MISMATCH\n");
            result.append("   The encryption types in keytab don't match KDC requirements\n");
            result.append("   Suggestions:\n");
            result.append("     1. List keytab encryption types: klist -kte <keytab>\n");
            result.append("     2. Regenerate keytab with matching encryption types\n");
            result.append("     3. Check permitted_enctypes in krb5.conf\n");
        } else if (fullError.contains("Cannot contact") || fullError.contains("Connection refused") 
                   || fullError.contains("Unknown host")) {
            result.append("   Root Cause: KDC CONNECTIVITY ISSUE\n");
            result.append("   Suggestions:\n");
            result.append("     1. Verify KDC address in krb5.conf\n");
            result.append("     2. Check network connectivity and firewall rules\n");
            result.append("     3. Ensure KDC service is running\n");
        } else if (fullError.contains("Client not found") || fullError.contains("Unknown client")) {
            result.append("   Root Cause: PRINCIPAL NOT FOUND IN KDC\n");
            result.append("   Suggestions:\n");
            result.append("     1. Verify principal exists: kadmin -q 'getprinc <principal>'\n");
            result.append("     2. Check for typos in principal name\n");
            result.append("     3. Ensure the realm is correct\n");
        } else if (fullError.contains("Realm not found") || fullError.contains("Cannot find realm")) {
            result.append("   Root Cause: REALM CONFIGURATION ERROR\n");
            result.append("   Suggestions:\n");
            result.append("     1. Verify realm is defined in krb5.conf [realms] section\n");
            result.append("     2. Check default_realm in [libdefaults]\n");
            result.append("     3. Ensure realm name matches exactly (case-sensitive)\n");
        } else {
            result.append("   Suggestions:\n");
            result.append("     1. Enable debug mode: set sun.security.krb5.debug=true\n");
            result.append("     2. Check /var/log/krb5kdc.log on KDC server\n");
            result.append("     3. Verify all configuration files are correct\n");
        }

        if (e.getCause() != null) {
            result.append("   Underlying cause: ").append(cause).append("\n");
        }

        return result.toString().trim();
    }

    // ==================== Step 5: HMS Connectivity ====================

    private static void checkHmsConnectivity(String hmsHost, String hmsPort) {
        printSection("Step 5: HMS (Hive Metastore) Connectivity");

        if (hmsHost == null || hmsHost.isEmpty()) {
            addResult("HMS connectivity", Status.SKIPPED, 
                "HMS host not configured. Add hms.host and hms.port to config.properties for full test");
            return;
        }

        int port;
        try {
            port = Integer.parseInt(hmsPort);
        } catch (NumberFormatException e) {
            port = 9083; // Default HMS port
        }

        // DNS resolution
        try {
            InetAddress address = InetAddress.getByName(hmsHost);
            addResult("HMS DNS resolution", Status.PASSED, 
                "Resolved to: " + address.getHostAddress());
        } catch (Exception e) {
            addResult("HMS DNS resolution", Status.FAILED, 
                "Cannot resolve HMS host: " + hmsHost + "\n" +
                "   Error: " + e.getMessage());
            return;
        }

        // TCP connectivity
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(hmsHost, port), 5000);
            addResult("HMS TCP connection", Status.PASSED, 
                "HMS is reachable at " + hmsHost + ":" + port);
        } catch (IOException e) {
            addResult("HMS TCP connection", Status.FAILED, 
                "Cannot connect to HMS at " + hmsHost + ":" + port + "\n" +
                "   Error: " + e.getMessage() + "\n" +
                "   Suggestions:\n" +
                "     1. Verify HMS service is running\n" +
                "     2. Check firewall rules for port " + port + "\n" +
                "     3. Ensure correct hostname/IP");
        }
    }

    // ==================== Reporting Utilities ====================

    private static void printSection(String title) {
        System.out.println();
        System.out.println(BOLD + CYAN + "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" + RESET);
        System.out.println(BOLD + "  " + title + RESET);
        System.out.println(CYAN + "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" + RESET);
    }

    private static void addResult(String check, Status status, String detail) {
        DiagnosticResult result = new DiagnosticResult(check, status, detail);
        diagnosticResults.add(result);

        String statusIcon;
        String statusColor;
        switch (status) {
            case PASSED:
                statusIcon = "✓";
                statusColor = GREEN;
                break;
            case FAILED:
                statusIcon = "✗";
                statusColor = RED;
                break;
            case WARNING:
                statusIcon = "⚠";
                statusColor = YELLOW;
                break;
            case SKIPPED:
                statusIcon = "○";
                statusColor = YELLOW;
                break;
            default:
                statusIcon = "ℹ";
                statusColor = CYAN;
        }

        System.out.println();
        System.out.println(statusColor + "  " + statusIcon + " " + check + RESET);
        
        // Indent detail lines
        String[] detailLines = detail.split("\n");
        for (String line : detailLines) {
            System.out.println("    " + line);
        }
    }

    private static void printFinalReport() {
        System.out.println();
        System.out.println(BOLD + CYAN);
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║                    DIAGNOSTIC SUMMARY                        ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println(RESET);

        long passed = diagnosticResults.stream().filter(r -> r.status == Status.PASSED).count();
        long failed = diagnosticResults.stream().filter(r -> r.status == Status.FAILED).count();
        long warnings = diagnosticResults.stream().filter(r -> r.status == Status.WARNING).count();
        long skipped = diagnosticResults.stream().filter(r -> r.status == Status.SKIPPED).count();

        System.out.println(GREEN + "  ✓ Passed:   " + passed + RESET);
        System.out.println(RED + "  ✗ Failed:   " + failed + RESET);
        System.out.println(YELLOW + "  ⚠ Warnings: " + warnings + RESET);
        System.out.println(YELLOW + "  ○ Skipped:  " + skipped + RESET);
        System.out.println();

        if (failed > 0) {
            System.out.println(RED + BOLD + "  RESULT: DIAGNOSTIC FAILED" + RESET);
            System.out.println();
            System.out.println("  Failed checks:");
            for (DiagnosticResult result : diagnosticResults) {
                if (result.status == Status.FAILED) {
                    System.out.println(RED + "    • " + result.check + RESET);
                }
            }
            System.out.println();
            System.out.println("  Please address the failed checks above to resolve connectivity issues.");
        } else if (warnings > 0) {
            System.out.println(YELLOW + BOLD + "  RESULT: PASSED WITH WARNINGS" + RESET);
            System.out.println();
            System.out.println("  Kerberos authentication should work, but consider addressing warnings.");
        } else {
            System.out.println(GREEN + BOLD + "  RESULT: ALL CHECKS PASSED" + RESET);
            System.out.println();
            System.out.println("  Kerberos configuration appears to be correct.");
            System.out.println("  You can now configure Doris HMS Catalog with these settings.");
        }

        System.out.println();
    }

    // ==================== Helper Classes ====================

    private enum Status {
        PASSED, FAILED, WARNING, INFO, SKIPPED
    }

    private static class DiagnosticResult {
        String check;
        Status status;
        String detail;

        DiagnosticResult(String check, Status status, String detail) {
            this.check = check;
            this.status = status;
            this.detail = detail;
        }
    }

    private static class Krb5Config {
        String defaultRealm;
        List<String> kdcAddresses = new ArrayList<>();
        String adminServer;
        int udpPreferenceLimit = 0;
        boolean renewable = false;
        boolean forwardable = false;
    }
}
