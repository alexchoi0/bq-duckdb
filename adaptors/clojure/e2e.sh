#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SERVER_PID=""

cleanup() {
    if [ -n "$SERVER_PID" ]; then
        echo "Stopping server (PID: $SERVER_PID)..."
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "=== bq-runner E2E Tests ==="
echo

# Build server
echo "Building server..."
cd "$PROJECT_ROOT"
cargo build --release
echo "  Build successful"

# Start server
echo "Starting server..."
./target/release/bq-runner &
SERVER_PID=$!

# Wait for server to be ready
echo "Waiting for server..."
for i in {1..50}; do
    if curl -s http://localhost:3000/health > /dev/null 2>&1; then
        echo "  Server ready"
        break
    fi
    if [ $i -eq 50 ]; then
        echo "  Timeout waiting for server"
        exit 1
    fi
    sleep 0.2
done

# Run Clojure tests
echo
echo "Running Clojure client tests..."
cd "$SCRIPT_DIR"
clojure -M:test

echo
echo "=== E2E Tests Complete ==="
