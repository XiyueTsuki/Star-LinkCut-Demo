#!/bin/bash
set -e

echo "=== 容器状态 ==="
docker ps --format "table {{.Names}}\t{{.Status}}"

echo ""
echo "=== Redis Sentinel 主节点 ==="
docker exec linkcut-sentinel-1 redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster

echo ""
echo "=== Redis Sentinel 节点列表 (验证 announce-ip) ==="
docker exec linkcut-sentinel-1 redis-cli -p 26379 SENTINEL SENTINELS mymaster | grep -E "^(ip|port|flags)" -A 1

echo ""
echo "=== Redis 主从复制 ==="
docker exec linkcut-redis-master redis-cli -a redis123 INFO replication | grep connected_slaves

echo ""
echo "=== Prometheus Targets ==="
curl -s http://localhost:9090/api/v1/targets | python3 -c "import sys,json; [print(f\"  {t['labels']['job']}: {t['health']}\") for t in json.load(sys.stdin)['data']['activeTargets']]" 2>/dev/null || \
curl -s http://localhost:9090/api/v1/targets | grep -o '"health":"up"' | wc -l | xargs echo "  targets UP:"

echo ""
echo "=== Grafana ==="
curl -s -o /dev/null -w "  HTTP %{http_code}" http://localhost:3000
echo ""

echo ""
echo "=== 验证完成 ==="