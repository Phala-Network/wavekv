#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

SESSION="filesync-test"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Clean up function for errors during setup
cleanup_on_error() {
	echo -e "${RED}Error during setup, cleaning up...${NC}"
	tmux kill-session -t $SESSION 2>/dev/null || true
	rm -rf /tmp/filesync-test-*
	exit 1
}

# Set up trap for errors during setup only
trap cleanup_on_error ERR

echo -e "${GREEN}Setting up filesync test cluster...${NC}"

# Create test directories
mkdir -p /tmp/filesync-test-node1/watch
mkdir -p /tmp/filesync-test-node1/data
mkdir -p /tmp/filesync-test-node2/watch
mkdir -p /tmp/filesync-test-node2/data
mkdir -p /tmp/filesync-test-node3/watch
mkdir -p /tmp/filesync-test-node3/data

# Build the project
echo -e "${BLUE}Building filesync...${NC}"
cargo build --release --bin filesync

# Kill existing session if it exists
tmux kill-session -t $SESSION 2>/dev/null || true

# Create new tmux session
echo -e "${BLUE}Starting tmux session: $SESSION${NC}"
tmux new-session -d -s $SESSION -n "node1"

# Split into 3 panes
tmux split-window -h -t $SESSION:0
tmux split-window -v -t $SESSION:0.1

# Set up pane titles and start nodes
tmux select-pane -t $SESSION:0.0 -T "Node 1"
tmux send-keys -t $SESSION:0.0 "cd $REPO_ROOT" C-m
tmux send-keys -t $SESSION:0.0 "echo 'Starting Node 1 on :8001, watching /tmp/filesync-test-node1/watch'" C-m
tmux send-keys -t $SESSION:0.0 "RUST_LOG=info,wavekv=debug cargo run --release --bin filesync -- run \
  --id 1 \
  --watch-dir /tmp/filesync-test-node1/watch \
  --data-dir /tmp/filesync-test-node1/data \
  --addr 127.0.0.1:8001 \
  " C-m

sleep 1

tmux select-pane -t $SESSION:0.1 -T "Node 2"
tmux send-keys -t $SESSION:0.1 "cd $REPO_ROOT" C-m
tmux send-keys -t $SESSION:0.1 "echo 'Starting Node 2 on :8002, watching /tmp/filesync-test-node2/watch'" C-m
tmux send-keys -t $SESSION:0.1 "RUST_LOG=info,wavekv=debug cargo run --release --bin filesync -- run \
  --id 2 \
  --watch-dir /tmp/filesync-test-node2/watch \
  --data-dir /tmp/filesync-test-node2/data \
  --addr 127.0.0.1:8002 \
  --peers '1=http://127.0.0.1:8001'" C-m

sleep 1

tmux select-pane -t $SESSION:0.2 -T "Node 3"
tmux send-keys -t $SESSION:0.2 "cd $REPO_ROOT" C-m
tmux send-keys -t $SESSION:0.2 "echo 'Starting Node 3 on :8003, watching /tmp/filesync-test-node3/watch'" C-m
tmux send-keys -t $SESSION:0.2 "RUST_LOG=info,wavekv=debug cargo run --release --bin filesync -- run \
  --id 3 \
  --watch-dir /tmp/filesync-test-node3/watch \
  --data-dir /tmp/filesync-test-node3/data \
  --addr 127.0.0.1:8003 \
  --peers '2=http://127.0.0.1:8002'" C-m

# Fourth pane for continuous consistency watch
tmux split-window -v -t $SESSION:0.2
tmux select-layout -t $SESSION:0 tiled
tmux select-pane -t $SESSION:0.3 -T "Consistency"
tmux send-keys -t $SESSION:0.3 "cd $REPO_ROOT" C-m
tmux send-keys -t $SESSION:0.3 "echo 'Running scripts/check-consistency.sh --watch (press Ctrl+C to stop)'" C-m
tmux send-keys -t $SESSION:0.3 "$REPO_ROOT/scripts/check-consistency.sh --watch" C-m

echo -e "${GREEN}Cluster started!${NC}"
echo -e "${BLUE}Waiting for nodes to initialize...${NC}"
sleep 3

echo -e "${GREEN}Test cluster is ready!${NC}"
echo ""
echo "Usage:"
echo "  - Attach to tmux session: ${BLUE}tmux attach -t $SESSION${NC}"
echo "  - Run test operations:    ${BLUE}./scripts/test-ops.sh${NC}"
echo "  - Check consistency:      ${BLUE}cargo run --release --bin filesync -- check --dirs /tmp/filesync-test-node1/watch,/tmp/filesync-test-node2/watch,/tmp/filesync-test-node3/watch${NC}"
echo "  - Watch consistency:      ${BLUE}./scripts/check-consistency.sh --watch${NC}"
echo "  - Stop cluster:           ${BLUE}./scripts/stop-cluster.sh${NC}"

# Remove error trap now that setup is complete
trap - ERR
