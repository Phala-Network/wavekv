#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

SESSION="filesync-test"

echo -e "${BLUE}Stopping filesync test cluster...${NC}"

# Kill tmux session
if tmux has-session -t $SESSION 2>/dev/null; then
    tmux kill-session -t $SESSION
    echo -e "${GREEN}✓ Killed tmux session${NC}"
else
    echo -e "${RED}No active tmux session found${NC}"
fi

# Remove test directories
if [ -d "/tmp/filesync-test-node1" ] || [ -d "/tmp/filesync-test-node2" ] || [ -d "/tmp/filesync-test-node3" ]; then
    rm -rf /tmp/filesync-test-*
    echo -e "${GREEN}✓ Removed test directories${NC}"
else
    echo -e "${BLUE}No test directories found${NC}"
fi

echo -e "${GREEN}Cleanup complete!${NC}"
