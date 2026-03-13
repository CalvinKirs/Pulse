# Azure Blob Connectivity Client (Standalone C++)

A standalone C++ client for validating Azure Blob connectivity without Doris runtime.

## What it tests

- `basic` mode: authenticate and fetch container properties
- `rw` mode: upload a temporary blob, verify its properties, then delete it

## Project layout

- `src/main.cpp`: client implementation
- `CMakeLists.txt`: build config
- `vcpkg.json`: dependency manifest (`azure-storage-blobs-cpp`)

## Build

Use CMake + vcpkg manifest mode.

```bash
cd /tmp/azure-blob-connectivity-client
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
cmake --build build -j
```

If you don't use vcpkg, install Azure SDK for C++ and pass package paths with `CMAKE_PREFIX_PATH`.

## One-Command Build Scripts

You can use the provided scripts to build directly on each platform.

macOS:

```bash
cd /tmp/azure-blob-connectivity-client
chmod +x build_macos.sh
./build_macos.sh
```

Linux:

```bash
cd /tmp/azure-blob-connectivity-client
chmod +x build_linux.sh
./build_linux.sh
```

Useful env vars for both scripts:

- `VCPKG_ROOT=/path/to/vcpkg` to reuse an existing vcpkg
- `INSTALL_DEPS=0` to skip package installation
- `CLEAN=1` to force a clean build
- `JOBS=8` to control parallel build jobs
- `BUILD_TYPE=Release` (default) or another CMake build type

## Run

Basic connectivity:

```bash
./build/azure_blob_connectivity \
  --account-name <account_name> \
  --account-key <account_key> \
  --container <container_name> \
  --mode basic
```

Read/write/delete validation:

```bash
./build/azure_blob_connectivity \
  --account-name <account_name> \
  --account-key <account_key> \
  --container <container_name> \
  --mode rw \
  --prefix doris-test/
```

Custom endpoint example:

```bash
./build/azure_blob_connectivity \
  --account-name <account_name> \
  --account-key <account_key> \
  --container <container_name> \
  --endpoint blob.core.windows.net \
  --mode basic
```

Or full container URL:

```bash
./build/azure_blob_connectivity \
  --account-name <account_name> \
  --account-key <account_key> \
  --container-url https://<account>.blob.core.windows.net/<container> \
  --mode basic
```

Custom CA bundle or certificate directory:

```bash
./build/azure_blob_connectivity \
  --account-name <account_name> \
  --account-key <account_key> \
  --container <container_name> \
  --mode basic \
  --ca-info /path/to/custom-ca.pem
```

Or:

```bash
./build/azure_blob_connectivity \
  --account-name <account_name> \
  --account-key <account_key> \
  --container <container_name> \
  --mode basic \
  --ca-path /path/to/certs
```

## TLS / Certificate Options

This client supports custom trust material for TLS validation through the Azure SDK curl transport:

- `--ca-info <pem_file>`: use a specific CA bundle file
- `--ca-path <pem_dir>`: use a directory of CA certificates

Use these options when the storage endpoint is behind a private certificate chain or an internal CA.

## CLI Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--account-name` | Yes | Azure storage account name |
| `--account-key` | Yes | Shared key for the storage account |
| `--container` | Yes* | Container name |
| `--container-url` | No* | Full container URL; can be used instead of `--container` |
| `--endpoint` | No | Endpoint host, default `blob.core.windows.net` |
| `--mode` | No | `basic` or `rw`, default `basic` |
| `--prefix` | No | Blob prefix used by `rw` mode |
| `--ca-info` | No | Custom CA bundle file passed to curl transport |
| `--ca-path` | No | Custom CA certificate directory passed to curl transport |
| `--log-level` | No | Azure SDK log level, `0-4`, default `3` |

\* Configure either `--container`, or `--container-url`.

## Exit codes

- `0`: success
- `1`: connectivity or runtime failure
- `2`: invalid arguments
