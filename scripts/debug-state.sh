#!/bin/bash

echo "=== Checking WaveKV internal state vs filesystem ==="
echo ""

for i in 1 2 3; do
    echo "Node $i:"
    echo "  Watch dir files:"
    ls -la /tmp/filesync-test-node$i/watch/ 2>/dev/null | grep "^-" | awk '{print "    " $9 " (" $5 " bytes)"}' || echo "    (empty)"
    echo "  Data dir:"
    ls -lh /tmp/filesync-test-node$i/data/ 2>/dev/null | grep -E "(wal|snapshot)" | awk '{print "    " $9 " (" $5 ")"}'
    echo ""
done

echo "=== Checking if nodes can reach each other ==="
for port in 8001 8002 8003; do
    if curl -s -X POST http://127.0.0.1:$port/sync \
        -H "Content-Type: application/json" \
        -d '{"sender_id":99,"sender_ack":{},"entries":[]}' >/dev/null 2>&1; then
        echo "✓ Node on port $port is reachable"
    else
        echo "✗ Node on port $port is NOT reachable"
    fi
done
