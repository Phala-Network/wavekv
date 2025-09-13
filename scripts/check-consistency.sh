#!/bin/bash

# Colors for output
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

WATCH_MODE=false
INTERVAL=5

while [[ $# -gt 0 ]]; do
  case $1 in
    --watch)
      WATCH_MODE=true
      shift
      ;;
    --interval)
      INTERVAL="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--watch] [--interval seconds]"
      exit 1
      ;;
  esac
done

run_check() {
  echo -e "${BLUE}Checking consistency across all nodes...${NC}"
  echo ""

  cargo run --release --bin filesync -- check \
    --dirs /tmp/filesync-test-node1/watch,/tmp/filesync-test-node2/watch,/tmp/filesync-test-node3/watch
}

if $WATCH_MODE; then
  echo -e "${GREEN}Watch mode enabled (interval ${INTERVAL}s). Press Ctrl+C to stop.${NC}"
  while true; do
    clear
    echo "Timestamp: $(date)"
    echo ""
    run_check
    sleep "$INTERVAL"
  done
else
  run_check
fi
