#!/bin/sh
# Redis Slave 启动脚本
# 使用 VM 外部 IP 作为 master 地址，同时宣告自身为外部 IP，
# 确保外部客户端透过 Master 发现 Slave 时拿到可路由的 IP

MASTER_EXTERNAL_IP="${MASTER_EXTERNAL_IP:-192.168.100.128}"
ANNOUNCE_IP="${ANNOUNCE_IP:-192.168.100.128}"
ANNOUNCE_PORT="${ANNOUNCE_PORT:-6380}"
MASTER_PORT="${REDIS_MASTER_PORT:-6379}"

echo "Master IP: ${MASTER_EXTERNAL_IP}:${MASTER_PORT}"
echo "Slave announce: ${ANNOUNCE_IP}:${ANNOUNCE_PORT}"

cp /usr/local/etc/redis/redis-slave.conf /tmp/redis-slave-dynamic.conf
cat >> /tmp/redis-slave-dynamic.conf << EOF

replicaof ${MASTER_EXTERNAL_IP} ${MASTER_PORT}
replica-announce-ip ${ANNOUNCE_IP}
replica-announce-port ${ANNOUNCE_PORT}
EOF

exec redis-server /tmp/redis-slave-dynamic.conf