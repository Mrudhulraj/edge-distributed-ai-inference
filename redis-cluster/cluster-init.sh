#!/bin/sh

echo "Waiting for Redis nodes to be ready..."

# Function to check if Redis is ready
check_redis() {
    redis-cli -h $1 -p 6379 ping > /dev/null 2>&1
}

# Wait for each node to be ready (max 60 seconds)
MAX_WAIT=60
COUNTER=0

echo "Checking redis-node-1..."
until check_redis 172.20.0.2 || [ $COUNTER -eq $MAX_WAIT ]; do
    sleep 1
    COUNTER=$((COUNTER+1))
    if [ $((COUNTER % 5)) -eq 0 ]; then
        echo "Still waiting for redis-node-1... ($COUNTER seconds)"
    fi
done

COUNTER=0
echo "Checking redis-node-2..."
until check_redis 172.20.0.3 || [ $COUNTER -eq $MAX_WAIT ]; do
    sleep 1
    COUNTER=$((COUNTER+1))
    if [ $((COUNTER % 5)) -eq 0 ]; then
        echo "Still waiting for redis-node-2... ($COUNTER seconds)"
    fi
done

COUNTER=0
echo "Checking redis-node-3..."
until check_redis 172.20.0.4 || [ $COUNTER -eq $MAX_WAIT ]; do
    sleep 1
    COUNTER=$((COUNTER+1))
    if [ $((COUNTER % 5)) -eq 0 ]; then
        echo "Still waiting for redis-node-3... ($COUNTER seconds)"
    fi
done

echo "All Redis nodes are ready!"
echo ""

# Additional wait to ensure cluster mode is fully initialized
sleep 5

echo "Creating Redis cluster..."
redis-cli --cluster create \
  172.20.0.2:6379 \
  172.20.0.3:6379 \
  172.20.0.4:6379 \
  --cluster-replicas 0 \
  --cluster-yes

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Redis cluster created successfully!"
    echo ""
else
    echo ""
    echo "❌ Failed to create cluster"
    exit 1
fi

# Check cluster status
echo "Cluster info:"
redis-cli -h 172.20.0.2 -p 6379 cluster info

echo ""
echo "Cluster nodes:"
redis-cli -h 172.20.0.2 -p 6379 cluster nodes

echo ""
echo "✅ Cluster initialization complete!"

# Keep container alive for inspection
tail -f /dev/null