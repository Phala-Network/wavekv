#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

PORTS=(8001 8002 8003)

show_usage() {
    echo "Usage: $0 [state|files|all] [node_number]"
    echo ""
    echo "Commands:"
    echo "  state [N]  - Show WaveKV internal state for node N (or all nodes)"
    echo "  files [N]  - Show file sync status for node N (or all nodes)"
    echo "  all [N]    - Show both state and files for node N (or all nodes)"
    echo ""
    echo "Examples:"
    echo "  $0 state 1      # Show state for node 1"
    echo "  $0 files        # Show files for all nodes"
    echo "  $0 all          # Show everything for all nodes"
}

query_node() {
    local port=$1
    local endpoint=$2
    local node_num=$((port - 8000))
    
    echo -e "${BLUE}=== Node $node_num (port $port) - $endpoint ===${NC}"
    
    if ! curl -s "http://127.0.0.1:$port/debug/$endpoint" > /tmp/debug_$port.json 2>/dev/null; then
        echo -e "${RED}✗ Node $node_num is not reachable${NC}"
        return 1
    fi
    
    if [ "$endpoint" = "state" ]; then
        echo ""
        echo -e "${YELLOW}Node Info:${NC}"
        jq -r '"  Node ID: \(.node_id)\n  Peers: \(.peers | join(", "))"' /tmp/debug_$port.json
        
        echo ""
        echo -e "${YELLOW}Local Ack (sync progress):${NC}"
        jq -r '.local_ack | to_entries[] | "  Node \(.key): seq \(.value)"' /tmp/debug_$port.json
        
        echo ""
        echo -e "${YELLOW}Peer States:${NC}"
        jq -r '.peer_states | to_entries[] | "  Peer \(.key): log_size=\(.value.log_size), peer_ack=\(.value.peer_ack)"' /tmp/debug_$port.json
        
        echo ""
        echo -e "${YELLOW}KV Entries (\(jq '.kv_entries | length' /tmp/debug_$port.json) total):${NC}"
        jq -r '.kv_entries | to_entries[] | "  \(.key): \(if .value.is_tombstone then "TOMBSTONE" else "\(.value.value_size) bytes" end) (seq=\(.value.seq), ts=\(.value.timestamp), from=node\(.value.origin_node))"' /tmp/debug_$port.json | head -20
        
        local total=$(jq '.kv_entries | length' /tmp/debug_$port.json)
        if [ "$total" -gt 20 ]; then
            echo "  ... and $((total - 20)) more entries"
        fi
    elif [ "$endpoint" = "files" ]; then
        echo ""
        echo -e "${YELLOW}Watch Directory:${NC} $(jq -r '.watch_dir' /tmp/debug_$port.json)"
        
        echo ""
        echo -e "${YELLOW}Files (\(jq '.files | length' /tmp/debug_$port.json) total):${NC}"
        jq -r '.files[] | "  \(.match_status) \(.name): fs=\(.size)B, kv=\(if .wavekv_size then "\(.wavekv_size)B" else "N/A" end)"' /tmp/debug_$port.json
    fi
    
    echo ""
}

# Parse arguments
CMD=${1:-all}
NODE=${2:-all}

if [ "$CMD" = "-h" ] || [ "$CMD" = "--help" ]; then
    show_usage
    exit 0
fi

if [ "$CMD" != "state" ] && [ "$CMD" != "files" ] && [ "$CMD" != "all" ]; then
    echo -e "${RED}Error: Invalid command '$CMD'${NC}"
    show_usage
    exit 1
fi

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo -e "${RED}Error: jq is required but not installed${NC}"
    echo "Install it with: sudo apt-get install jq"
    exit 1
fi

# Determine which nodes to query
if [ "$NODE" = "all" ]; then
    NODES=(1 2 3)
else
    if ! [[ "$NODE" =~ ^[1-3]$ ]]; then
        echo -e "${RED}Error: Node number must be 1, 2, or 3${NC}"
        exit 1
    fi
    NODES=($NODE)
fi

# Query nodes
for node_num in "${NODES[@]}"; do
    port=$((8000 + node_num))
    
    if [ "$CMD" = "state" ] || [ "$CMD" = "all" ]; then
        query_node $port "state"
    fi
    
    if [ "$CMD" = "files" ] || [ "$CMD" = "all" ]; then
        query_node $port "files"
    fi
    
    if [ "${#NODES[@]}" -gt 1 ]; then
        echo ""
    fi
done

# Cleanup
rm -f /tmp/debug_*.json
