package com.xiyuetsuki.linkcutbackend.mq;

import org.redisson.api.RList;
import org.redisson.api.RQueue;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 消息积压监控器
 * <p>
 * 定时检查 Redis 降级队列和死信队列的水位，超过阈值时输出 WARN/ERROR 日志。
 * 生产环境可在此接入钉钉/飞书/邮件告警。
 */
@Component
public class BacklogMonitor {

    private static final Logger log = LoggerFactory.getLogger(BacklogMonitor.class);

    @Value("${rocketmq.fallback.queue-key}")
    private String fallbackQueueKey;

    @Value("${rocketmq.fallback.dlq-key}")
    private String dlqKey;

    @Value("${rocketmq.fallback.max-queue-size}")
    private int maxQueueSize;

    @Value("${rocketmq.fallback.backlog-warn-threshold}")
    private double backlogWarnThreshold;

    private final RedissonClient redissonClient;

    public BacklogMonitor(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    @Scheduled(fixedDelayString = "${rocketmq.fallback.monitor-interval-ms}")
    public void checkBacklog() {
        try {
            RQueue<String> fallbackQueue = redissonClient.getQueue(fallbackQueueKey);
            long fallbackSize = fallbackQueue.size();
            long warnThreshold = (long) (maxQueueSize * backlogWarnThreshold);

            if (fallbackSize >= warnThreshold) {
                log.warn("BACKLOG ALERT: Fallback queue approaching limit ({}/{}), "
                        + "Producer may be failing to send to RocketMQ", fallbackSize, maxQueueSize);
            }

            RList<String> dlq = redissonClient.getList(dlqKey);
            long dlqSize = dlq.size();
            if (dlqSize > 0) {
                log.warn("BACKLOG ALERT: DLQ contains {} messages waiting for manual inspection", dlqSize);
                if (dlqSize > 100) {
                    log.error("BACKLOG CRITICAL: DLQ growing rapidly ({}), check Consumer immediately", dlqSize);
                }
            }

            if (fallbackSize == 0 && dlqSize == 0) {
                log.debug("Backlog check: all clear");
            }
        } catch (Exception e) {
            log.error("Failed to check backlog status", e);
        }
    }
}