#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

DIRS=(
    "/tmp/filesync-test-node1/watch"
    "/tmp/filesync-test-node2/watch"
    "/tmp/filesync-test-node3/watch"
)

# Check if directories exist
for dir in "${DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        echo -e "${RED}Error: Directory $dir does not exist${NC}"
        echo "Please run ./scripts/test-cluster.sh first"
        exit 1
    fi
done

# Generate a random file in a random directory
create_file() {
    local dir=${DIRS[$RANDOM % ${#DIRS[@]}]}
    local filename="file_$(date +%s)_$RANDOM.txt"
    local content="Test content $(date) - Random: $RANDOM"
    
    echo "$content" > "$dir/$filename"
    echo -e "${GREEN}✓ Created${NC} $filename in $(basename $dir)"
}

# Delete a random file from a random directory
delete_file() {
    local dir=${DIRS[$RANDOM % ${#DIRS[@]}]}
    local files=("$dir"/*)
    
    # Check if there are any files
    if [ ${#files[@]} -eq 0 ] || [ ! -f "${files[0]}" ]; then
        echo -e "${YELLOW}⚠ No files to delete in $(basename $dir)${NC}"
        return
    fi
    
    local file=${files[$RANDOM % ${#files[@]}]}
    if [ -f "$file" ]; then
        rm "$file"
        echo -e "${RED}✗ Deleted${NC} $(basename $file) from $(basename $dir)"
    fi
}

# Modify a random file in a random directory
modify_file() {
    local dir=${DIRS[$RANDOM % ${#DIRS[@]}]}
    local files=("$dir"/*)
    
    # Check if there are any files
    if [ ${#files[@]} -eq 0 ] || [ ! -f "${files[0]}" ]; then
        echo -e "${YELLOW}⚠ No files to modify in $(basename $dir)${NC}"
        return
    fi
    
    local file=${files[$RANDOM % ${#files[@]}]}
    if [ -f "$file" ]; then
        echo "Modified at $(date) - Random: $RANDOM" >> "$file"
        echo -e "${BLUE}✎ Modified${NC} $(basename $file) in $(basename $dir)"
    fi
}

# List all files across directories
list_files() {
    echo -e "\n${BLUE}=== Current Files ===${NC}"
    for dir in "${DIRS[@]}"; do
        local count=$(find "$dir" -maxdepth 1 -type f | wc -l)
        echo -e "${GREEN}$(basename $dir):${NC} $count files"
        find "$dir" -maxdepth 1 -type f -exec basename {} \; | sort | sed 's/^/  - /'
    done
    echo ""
}

echo -e "${GREEN}=== Filesync Test Operations ===${NC}"
echo "This script will randomly create, modify, and delete files"
echo "Press Ctrl+C to stop"
echo ""

# Initial state
list_files

# Run random operations
OPERATIONS=0
while true; do
    # Random sleep between 1-3 seconds
    sleep $(( ( RANDOM % 3 )  + 1 ))
    
    # Random operation
    OP=$((RANDOM % 100))
    
    if [ $OP -lt 40 ]; then
        # 40% chance to create
        create_file
    elif [ $OP -lt 70 ]; then
        # 30% chance to modify
        modify_file
    else
        # 30% chance to delete
        delete_file
    fi
    
    OPERATIONS=$((OPERATIONS + 1))
    
    # Show status every 5 operations
    if [ $((OPERATIONS % 5)) -eq 0 ]; then
        list_files
        echo -e "${YELLOW}--- Operations performed: $OPERATIONS ---${NC}"
    fi
done
