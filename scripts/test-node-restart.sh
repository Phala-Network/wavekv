#!/bin/bash

# Test script to reproduce the node restart bug
# Scenario: Stop a node, delete its data, restart it

set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=== Testing Node Restart with Data Loss ===${NC}"
echo ""

# Check if cluster is running
if ! tmux has-session -t filesync-test 2>/dev/null; then
    echo -e "${RED}Error: filesync-test cluster is not running${NC}"
    echo "Start it with: ./scripts/test-cluster.sh"
    exit 1
fi

# Get current state of node 1
echo -e "${BLUE}Step 1: Recording Node 1 current state...${NC}"
NODE1_SEQ=$(curl -s http://127.0.0.1:8001/debug/state | jq -r '.kv_entries | to_entries | map(.value.seq) | max')
NODE1_FILES=$(curl -s http://127.0.0.1:8001/debug/files | jq -r '.files | length')
echo "  Node 1 max seq: $NODE1_SEQ"
echo "  Node 1 files: $NODE1_FILES"
echo ""

# Stop node 1
echo -e "${BLUE}Step 2: Stopping Node 1 (pane 0)...${NC}"
tmux send-keys -t filesync-test:0.0 C-c
sleep 2
echo "  Node 1 stopped"
echo ""

# Delete node 1 data
echo -e "${BLUE}Step 3: Deleting Node 1 data...${NC}"
rm -rf /tmp/filesync-test-node1/data/*
rm -f /tmp/filesync-test-node1/watch/*
echo "  Deleted: /tmp/filesync-test-node1/data/*"
echo "  Deleted: /tmp/filesync-test-node1/watch/*"
echo ""

# Restart node 1
echo -e "${BLUE}Step 4: Restarting Node 1...${NC}"
tmux send-keys -t filesync-test:0.0 "cd $PWD/.." C-m
tmux send-keys -t filesync-test:0.0 "RUST_LOG=info,wavekv=debug cargo run --release --bin filesync -- run \
  --id 1 \
  --watch-dir /tmp/filesync-test-node1/watch \
  --data-dir /tmp/filesync-test-node1/data \
  --addr 127.0.0.1:8001 \
  --peers '2=http://127.0.0.1:8002,3=http://127.0.0.1:8003'" C-m
echo "  Node 1 restarting..."
sleep 3
echo ""

# Check new state
echo -e "${BLUE}Step 5: Checking Node 1 new state...${NC}"
NEW_SEQ=$(curl -s http://127.0.0.1:8001/debug/state | jq -r '.kv_entries | to_entries | map(.value.seq) | max // 0')
NEW_FILES=$(curl -s http://127.0.0.1:8001/debug/files | jq -r '.files | length')
echo "  Node 1 new max seq: $NEW_SEQ"
echo "  Node 1 new files: $NEW_FILES"
echo ""

# Create a new file on node 1
echo -e "${BLUE}Step 6: Creating a new file on Node 1...${NC}"
TEST_FILE="/tmp/filesync-test-node1/watch/test_restart_$(date +%s).txt"
echo "This is a test file created after restart" > "$TEST_FILE"
echo "  Created: $TEST_FILE"
sleep 2
echo ""

# Check for conflicts
echo -e "${BLUE}Step 7: Checking for seq conflicts...${NC}"
echo "  Getting entries from all nodes..."

for node in 1 2 3; do
    port=$((8000 + node))
    echo ""
    echo -e "${YELLOW}Node $node entries with origin=node1:${NC}"
    curl -s http://127.0.0.1:$port/debug/state | \
        jq -r '.kv_entries | to_entries[] | select(.value.origin_node == 1) | "  seq=\(.value.seq) key=\(.key) tombstone=\(.value.is_tombstone) ts=\(.value.timestamp)"' | \
        sort -t= -k2 -n | head -10
done

echo ""
echo ""
echo -e "${YELLOW}=== Analysis ===${NC}"
echo "Checking for sequence number conflicts..."
echo ""

# Check if bootstrap recovered next_seq correctly
CURRENT_NEXT_SEQ=$(curl -s http://127.0.0.1:8001/debug/state | jq -r '.kv_entries | to_entries | map(select(.value.origin_node == 1) | .value.seq) | max // 0')
echo "Node 1 current max seq in KV: $CURRENT_NEXT_SEQ"
echo "Expected next_seq: $((CURRENT_NEXT_SEQ + 1))"
echo ""

if [ "$CURRENT_NEXT_SEQ" -ge "$NODE1_SEQ" ]; then
    echo -e "${GREEN}✓ Bootstrap SUCCESS: Node 1 recovered sequence numbers from peers${NC}"
    echo "  Max seq before restart: $NODE1_SEQ"
    echo "  Max seq after restart: $CURRENT_NEXT_SEQ"
else
    echo -e "${RED}✗ Bootstrap FAILED: Sequence numbers were not recovered${NC}"
    echo "  This means the node will reuse sequence numbers!"
fi

echo ""
echo "If you see the same seq number with different keys above, that's a conflict!"
echo "For example: seq=1 with key=old_file AND seq=1 with key=test_restart_*"
