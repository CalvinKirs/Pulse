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

## Exit codes

- `0`: success
- `1`: connectivity or runtime failure
- `2`: invalid arguments
