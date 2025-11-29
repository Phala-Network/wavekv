#!/bin/bash
# Quick test script - demonstrates filesync in action

set -e

echo "=== FileSyncQuick Test ==="
echo ""

# Check if tmux is available
if ! command -v tmux &> /dev/null; then
    echo "Error: tmux is required but not installed"
    exit 1
fi

echo "1. Starting 3-node cluster (this takes ~5 seconds)..."
./scripts/test-cluster.sh &
CLUSTER_PID=$!

# Wait for cluster to start
sleep 8

echo ""
echo "2. Cluster is ready!"
echo ""
echo "3. Creating some initial files..."

# Create initial files in different nodes
echo "Hello from Node 1" > /tmp/filesync-test-node1/watch/hello1.txt
echo "Hello from Node 2" > /tmp/filesync-test-node2/watch/hello2.txt
echo "Hello from Node 3" > /tmp/filesync-test-node3/watch/hello3.txt

sleep 5

echo ""
echo "4. Checking consistency (files should be synced)..."
cargo run --release -p filesync -- check \
  --dirs /tmp/filesync-test-node1/watch,/tmp/filesync-test-node2/watch,/tmp/filesync-test-node3/watch

echo ""
echo "5. Try it yourself:"
echo "   - View logs:  tmux attach -t filesync-test"
echo "   - Add files:  echo 'test' > /tmp/filesync-test-node1/watch/myfile.txt"
echo "   - Run ops:    ./scripts/test-ops.sh"
echo "   - Check:      cargo run --release -p filesync -- check --dirs /tmp/filesync-test-node1/watch,/tmp/filesync-test-node2/watch,/tmp/filesync-test-node3/watch"
echo ""
echo "Press Ctrl+C to stop (cluster will be cleaned up automatically)"

wait $CLUSTER_PID
