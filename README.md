# Pulse

**Lightweight Connectivity Testing Toolkit**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Java](https://img.shields.io/badge/Java-8%2B-orange.svg)](https://www.oracle.com/java/)

Pulse is a lightweight tool for testing connectivity to infrastructure dependencies such as authentication services and object storage. Built for simplicity — no complex setup, no heavy dependencies, just drop-in tools that work.
## Philosophy

- **Lightweight**: Minimal dependencies, single JAR deployment
- **Portable**: Works across environments without installation
- **Actionable**: Clear diagnostics with specific fix suggestions
- **Fast**: Get answers in seconds, not minutes

## Modules

| Module | Description |
|--------|-------------|
| [kerberos-tools](./kerberos-tools) | Kerberos authentication & connectivity diagnostics |
| common | Shared utilities across all tools |

## Quick Start

### Use Pre-built Binary

Each tool is a standalone JAR — just download and run:

```bash
# 1. Download/copy the JAR to your working directory
# 2. Create config.properties in the SAME directory
# 3. Run
java -jar native-kerberos-tools-jar-with-dependencies.jar
```

> **Note**: Configuration files must be placed in the same directory as the JAR.

### Build from Source

Prerequisites: Java 8+, Maven 3.x

```bash
git clone https://github.com/user/Pulse.git
cd Pulse
mvn clean package
```

Output JARs are located in each module's `target/` directory.

## Project Structure

```
Pulse/
├── common/                    # Shared utilities
│   └── src/main/java/
├── kerberos-tools/           # Kerberos diagnostic tool
│   ├── src/main/java/
│   └── src/main/resources/
├── pom.xml                   # Parent POM
└── LICENSE
```

## Adding New Tools

Pulse is designed to be extensible. To add a new connectivity tool:

1. Create a new module directory
2. Add module reference to parent `pom.xml`
3. Depend on `common` module for shared utilities
4. Build a standalone JAR with `maven-assembly-plugin`

## Contributing

Contributions are welcome! Whether it's:

- New connectivity testing tools
- Bug fixes and improvements
- Documentation enhancements

Please feel free to submit issues and pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

