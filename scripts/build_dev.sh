#!/usr/bin/env bash
# Build ReCon with a Unix-timestamp build number (like Resonite-style numbering).
# Usage: ./scripts/build_dev.sh [linux|windows|apk|...]
# Requires: flutter in PATH
set -e
cd "$(dirname "$0")/.."
BUILD_NUMBER=$(date +%s)
BUILD_NAME="0.13.0-dev"
TARGET="${1:-linux}"
echo "Building ReCon $BUILD_NAME+$BUILD_NUMBER for $TARGET..."
flutter build "$TARGET" --build-name="$BUILD_NAME" --build-number="$BUILD_NUMBER"
echo "Done. About will show build time (timestamp $BUILD_NUMBER)."