#!/bin/sh
# Sentinel 启动脚本
# 使用 VM 外部 IP 作为 master/sentinel 通告地址，
# 确保外部客户端通过 SENTINEL commands 拿到可路由的 IP

MASTER_EXTERNAL_IP="${MASTER_EXTERNAL_IP:-192.168.100.128}"
ANNOUNCE_IP="${ANNOUNCE_IP:-192.168.100.128}"
ANNOUNCE_PORT="${ANNOUNCE_PORT:-26379}"
MASTER_PORT="${REDIS_MASTER_PORT:-6379}"
QUORUM="${SENTINEL_QUORUM:-2}"

echo "Master IP: ${MASTER_EXTERNAL_IP}:${MASTER_PORT}"
echo "Announce: ${ANNOUNCE_IP}:${ANNOUNCE_PORT}"

cat > /tmp/sentinel-dynamic.conf << EOF
sentinel monitor mymaster ${MASTER_EXTERNAL_IP} ${MASTER_PORT} ${QUORUM}
sentinel announce-ip ${ANNOUNCE_IP}
sentinel announce-port ${ANNOUNCE_PORT}
EOF
cat /usr/local/etc/redis/sentinel.conf >> /tmp/sentinel-dynamic.conf

echo "Starting sentinel with config:"
cat /tmp/sentinel-dynamic.conf

exec redis-sentinel /tmp/sentinel-dynamic.conf