#!/bin/sh

echo "Waiting for Redis nodes to be ready..."
sleep 30

echo "Creating Redis cluster..."
redis-cli --cluster create \
  172.20.0.2:6379 \
  172.20.0.3:6379 \
  172.20.0.4:6379 \
  --cluster-replicas 0 \
  --cluster-yes

# Check cluster status
echo "Cluster info:"
redis-cli -h 172.20.0.2 -p 6379 cluster info

echo ""
echo "Cluster nodes:"
redis-cli -h 172.20.0.2 -p 6379 cluster nodes

# Keep container alive for inspection
tail -f /dev/null