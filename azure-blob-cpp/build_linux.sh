#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${ROOT_DIR}/build"

BUILD_TYPE="${BUILD_TYPE:-Release}"
GENERATOR="${GENERATOR:-Ninja}"
VCPKG_ROOT="${VCPKG_ROOT:-$HOME/vcpkg}"
JOBS="${JOBS:-$(nproc)}"
INSTALL_DEPS="${INSTALL_DEPS:-1}"
CLEAN="${CLEAN:-0}"

log() {
  echo "[build_linux] $*"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing command: $1" >&2
    exit 1
  }
}

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This script is for Linux only. Use build_macos.sh on macOS." >&2
  exit 1
fi

if [[ "${INSTALL_DEPS}" == "1" ]]; then
  if command -v apt-get >/dev/null 2>&1; then
    log "Installing build dependencies with apt..."
    if [[ "$(id -u)" -eq 0 ]]; then
      apt-get update
      apt-get install -y build-essential cmake ninja-build git curl zip unzip tar pkg-config
    elif command -v sudo >/dev/null 2>&1; then
      sudo apt-get update
      sudo apt-get install -y build-essential cmake ninja-build git curl zip unzip tar pkg-config
    else
      echo "Need root or sudo to install dependencies with apt-get." >&2
      exit 1
    fi
  else
    log "apt-get not found; skipping dependency installation."
    log "Please ensure these commands exist: cmake ninja git curl zip unzip tar pkg-config"
  fi
fi

need_cmd git
need_cmd cmake

if [[ ! -d "${VCPKG_ROOT}" ]]; then
  log "Cloning vcpkg to ${VCPKG_ROOT}"
  git clone https://github.com/microsoft/vcpkg.git "${VCPKG_ROOT}"
fi

if [[ ! -x "${VCPKG_ROOT}/vcpkg" ]]; then
  log "Bootstrapping vcpkg..."
  "${VCPKG_ROOT}/bootstrap-vcpkg.sh"
fi

if [[ "${CLEAN}" == "1" ]]; then
  log "Cleaning build directory ${BUILD_DIR}"
  rm -rf "${BUILD_DIR}"
fi

log "Configuring project..."
cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" -G "${GENERATOR}" \
  -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
  -DCMAKE_TOOLCHAIN_FILE="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"

log "Building project..."
cmake --build "${BUILD_DIR}" -j "${JOBS}"

BIN="${BUILD_DIR}/azure_blob_connectivity"
if [[ ! -x "${BIN}" ]]; then
  echo "Build finished but binary not found: ${BIN}" >&2
  exit 1
fi

log "Done."
echo "Binary: ${BIN}"
echo "Try: ${BIN} --help"

