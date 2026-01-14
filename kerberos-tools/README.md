# Kerberos Tools

> Part of the [Pulse](../README.md) connectivity testing toolkit

**Native Kerberos Connectivity Diagnostic Tool**

A lightweight, zero-configuration diagnostic utility for validating Kerberos authentication setups — particularly useful for Apache Doris HMS Catalog, Hadoop, and other Kerberized environments.

## Features

- **krb5.conf Validation**: Parses and validates Kerberos configuration files
- **KDC Reachability**: Tests network connectivity to Key Distribution Centers
- **Keytab Inspection**: Validates keytab files and lists principals with encryption types
- **Authentication Test**: Performs actual Kerberos authentication without external JAAS config
- **Smart Diagnostics**: Provides specific fix suggestions for common issues

## Usage

### Option 1: Use Pre-built Binary (Recommended)

Download the latest release JAR and run directly:

```bash
# 1. Create a working directory
mkdir kerberos-test && cd kerberos-test

# 2. Download the JAR (or copy from build)
cp /path/to/native-kerberos-tools-jar-with-dependencies.jar .

# 3. Create config.properties in the SAME directory as the JAR
cat > config.properties << 'EOF'
keytabPath=/path/to/your.keytab
principal=hdfs/hostname@YOUR.REALM
krb5ConfPath=/etc/krb5.conf
EOF

# 4. Run the tool
java -jar native-kerberos-tools-jar-with-dependencies.jar
```

> **Important**: The `config.properties` file MUST be placed in the same directory as the JAR file.

### Option 2: Build from Source

```bash
# Clone and build
git clone https://github.com/user/Pulse.git
cd Pulse
mvn clean package

# Copy JAR to your working directory
cp kerberos-tools/target/native-kerberos-tools-jar-with-dependencies.jar /your/test/dir/

# Create config.properties in the same directory and run
cd /your/test/dir/
java -jar native-kerberos-tools-jar-with-dependencies.jar
```

## Configuration

Create a `config.properties` file in the **same directory** as the JAR:

```properties
# Required: Path to the Kerberos keytab file
keytabPath=/path/to/your.keytab

# Required: Kerberos principal name (must exist in keytab)
# Format: <service>/<hostname>@<REALM> or <username>@<REALM>
principal=hdfs/hostname@YOUR.REALM

# Required: Path to krb5.conf file
krb5ConfPath=/etc/krb5.conf
```

### Directory Structure

```
your-working-directory/
├── native-kerberos-tools-jar-with-dependencies.jar
└── config.properties    # Must be in the same directory!
```

## Sample Output

```
╔══════════════════════════════════════════════════════════════╗
║      Doris Kerberos Connectivity Diagnostic Tool             ║
║                        v1.0.0                                ║
╚══════════════════════════════════════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Step 1: krb5.conf Validation
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ✓ krb5.conf exists
    File found: /etc/krb5.conf

  ✓ default_realm
    Value: YOUR.REALM

  ✓ KDC configuration
    Found 1 KDC server(s): [kdc.your.realm:88]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Step 2: KDC Network Reachability
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ✓ DNS resolution [kdc.your.realm]
    Resolved to: 192.168.1.100

  ✓ TCP connection [kdc.your.realm:88]
    KDC is reachable on port 88

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Step 3: Keytab File Validation
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ✓ Keytab exists
    File found: /path/to/your.keytab

  ✓ Principal match
    Expected principal found: hdfs/hostname@YOUR.REALM

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Step 4: Kerberos Authentication Test
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ✓ Kerberos authentication
    Successfully authenticated as: hdfs/hostname@YOUR.REALM

╔══════════════════════════════════════════════════════════════╗
║                    DIAGNOSTIC SUMMARY                        ║
╚══════════════════════════════════════════════════════════════╝

  ✓ Passed:   8
  ✗ Failed:   0
  ⚠ Warnings: 0

  RESULT: ALL CHECKS PASSED
```

## Diagnostic Steps

| Step | Description |
|------|-------------|
| **1. krb5.conf Validation** | Validates file existence, permissions, default_realm, KDC config, and recommended settings |
| **2. KDC Reachability** | DNS resolution and TCP connectivity test to all configured KDC servers |
| **3. Keytab Validation** | Checks keytab file integrity, lists principals and encryption types |
| **4. Authentication Test** | Performs actual Kerberos login using keytab (no external JAAS config needed) |

## Common Issues & Fixes

### Clock Skew Too Great

```
Error: Clock skew too great
```

**Solution**: Sync system time with NTP server:
```bash
sudo ntpdate pool.ntp.org
# or
timedatectl set-ntp true
```

### Pre-authentication Failed

```
Error: Pre-authentication failed
```

**Solution**: Regenerate keytab with correct credentials:
```bash
kadmin -q "ktadd -k /path/to/new.keytab principal@REALM"
```

### Principal Not Found

```
Error: Expected principal NOT found in keytab
```

**Solution**: Verify principal name matches exactly (case-sensitive), check realm suffix.

### KDC Unreachable

```
Error: Cannot connect to KDC
```

**Solution**: 
- Check firewall rules (port 88 TCP/UDP)
- Verify KDC hostname in krb5.conf
- Ensure KDC service is running

## Configuration Reference

| Property | Required | Description |
|----------|----------|-------------|
| `keytabPath` | Yes | Absolute path to the keytab file |
| `principal` | Yes | Kerberos principal (e.g., `user@REALM` or `service/host@REALM`) |
| `krb5ConfPath` | Yes | Absolute path to krb5.conf |

## Dependencies

- Apache Kerby 2.1.0 (keytab parsing)
- Commons Net 3.11.1 (network utilities)
- Java 8+

## Use Cases

- **Apache Doris**: Validate Kerberos setup before configuring HMS Catalog
- **Hadoop**: Debug HDFS/YARN authentication issues
- **Hive**: Troubleshoot Metastore connectivity
- **General**: Any environment using MIT Kerberos

## License

MIT License - see [LICENSE](../LICENSE) for details.
