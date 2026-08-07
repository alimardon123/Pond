#!/usr/bin/env bash
# Build Pond's Rust workspace and prepare Python + C ABI artifacts.
#
# Usage:
#   ./build.sh                  # release build (default)
#   ./build.sh debug            # debug build
#
# After this script runs:
#   - target/release/pond.so      — Python extension module (import pond)
#   - target/release/libpond_core.a    — C ABI static library for Go/Java/Node/C
#   - target/release/libpond_core.so   — C ABI shared library
#
# Set PYTHONPATH=target/release to import pond from Python without
# installing it.

set -euo pipefail

cd "$(dirname "$0")"

PROFILE="${1:-release}"
if [ "$PROFILE" = "debug" ]; then
    cargo build
else
    cargo build --release
fi

# Locate the profile dir
PROFILE_DIR="target/release"
if [ "$PROFILE" = "debug" ]; then
    PROFILE_DIR="target/debug"
fi

# On Linux, cargo produces libpond.so for the cdylib. Python expects
# pond.so (no lib prefix). Create a hardlink so both names work.
if [ -f "$PROFILE_DIR/libpond.so" ] && [ ! -f "$PROFILE_DIR/pond.so" ]; then
    ln "$PROFILE_DIR/libpond.so" "$PROFILE_DIR/pond.so"
    echo "Linked $PROFILE_DIR/pond.so -> libpond.so"
fi

# On macOS, the equivalent for .dylib.
if [ -f "$PROFILE_DIR/libpond.dylib" ] && [ ! -f "$PROFILE_DIR/pond.dylib" ]; then
    ln "$PROFILE_DIR/libpond.dylib" "$PROFILE_DIR/pond.dylib"
    echo "Linked $PROFILE_DIR/pond.dylib -> libpond.dylib"
fi

echo
echo "Build complete. Artifacts in $PROFILE_DIR/:"
ls -la "$PROFILE_DIR"/libpond_*.* "$PROFILE_DIR"/pond.* "$PROFILE_DIR"/pond 2>/dev/null || true
