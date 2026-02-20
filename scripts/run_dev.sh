#!/usr/bin/env bash
# Run ReCon with a Unix-timestamp build number so each run gets a unique version.
# Usage: ./scripts/run_dev.sh [device]
# Example: ./scripts/run_dev.sh linux
# Requires: flutter in PATH
set -e
cd "$(dirname "$0")/.."
BUILD_NUMBER=$(date +%s)
BUILD_NAME="0.13.0-dev"
echo "Running ReCon $BUILD_NAME+$BUILD_NUMBER..."
exec flutter run --build-name="$BUILD_NAME" --build-number="$BUILD_NUMBER" "$@"
