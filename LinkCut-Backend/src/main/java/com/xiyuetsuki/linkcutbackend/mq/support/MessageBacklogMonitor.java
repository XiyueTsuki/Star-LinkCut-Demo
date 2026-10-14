package com.xiyuetsuki.linkcutbackend.mq.support;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.api.RList;
import org.redisson.api.RQueue;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * 消息积压监控器
 * <p>
 * 定时检查 Redis 降级队列和死信队列的水位，超过阈值时输出 WARN/ERROR 日志。
 * 生产环境可在此接入钉钉/飞书/邮件告警。
 */
@Component
public class MessageBacklogMonitor {

    private static final Logger log = LoggerFactory.getLogger(MessageBacklogMonitor.class);

    @Value("${rocketmq.fallback.queue-key}")
    private String fallbackQueueKey;

    @Value("${rocketmq.fallback.dlq-key}")
    private String dlqKey;

    @Value("${rocketmq.fallback.max-queue-size}")
    private int maxQueueSize;

    @Value("${rocketmq.fallback.backlog-warn-threshold}")
    private double backlogWarnThreshold;

    private final RedissonClient redissonClient;
    private final ObjectMapper objectMapper;
    private final MessagePersistenceHelper persistenceHelper;

    @Value("${rocketmq.fallback.dlq-max-size}")
    private int dlqMaxSize;

    @Value("${rocketmq.fallback.dlq-retain-hours}")
    private int dlqRetainHours;

    @Value("${rocketmq.fallback.local-fallback-max-size-mb}")
    private int localFallbackMaxSizeMb;

    public MessageBacklogMonitor(RedissonClient redissonClient,
                                 ObjectMapper objectMapper,
                                 MessagePersistenceHelper persistenceHelper) {
        this.redissonClient = redissonClient;
        this.objectMapper = objectMapper;
        this.persistenceHelper = persistenceHelper;
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

    /**
     * 定时清理 DLQ：容量淘汰（FIFO）+ 时间淘汰
     * <p>
     * 容量维度：DLQ 超过 max-size 时从头部修剪最旧条目
     * 时间维度：单条消息在 DLQ 中超过 retain-hours 时自动删除
     */
    @Scheduled(fixedDelayString = "${rocketmq.fallback.dlq-cleanup-interval-ms}")
    public void cleanupDlq() {
        try {
            RList<String> dlq = redissonClient.getList(dlqKey);
            int size = dlq.size();
            if (size == 0) {
                return;
            }

            int trimmed = 0;

            // 1. 容量淘汰：超过 max-size 时从头部(FIFO)修剪
            int overflow = size - dlqMaxSize;
            if (overflow > 0) {
                for (int i = 0; i < overflow && dlq.size() > 0; i++) {
                    dlq.remove(0);
                }
                trimmed += overflow;
            }

            // 2. 时间淘汰：超过 retain-hours 的条目从头部移除
            int remaining = dlq.size();
            long cutoff = System.currentTimeMillis() - (long) dlqRetainHours * 3600 * 1000;

            if (remaining <= 500) {
                // RList 数据量不大：直接索引遍历头部
                while (dlq.size() > 0) {
                    String entry = dlq.get(0);
                    Long msgTime = extractTime(entry);
                    if (msgTime != null && msgTime < cutoff) {
                        dlq.remove(0);
                        trimmed++;
                    } else {
                        break;
                    }
                }
            } else {
                // RList 数据量大：readAll 到内存后批量重建（减少网络 IO）
                List<String> all = dlq.readAll();
                int firstValidIdx = 0;
                for (String entry : all) {
                    Long msgTime = extractTime(entry);
                    if (msgTime != null && msgTime < cutoff) {
                        firstValidIdx++;
                    } else {
                        break;
                    }
                }
                if (firstValidIdx > 0) {
                    dlq.delete();
                    dlq.addAll(all.subList(firstValidIdx, all.size()));
                    trimmed += firstValidIdx;
                }
            }

            if (trimmed > 0) {
                log.info("DLQ cleanup: trimmed {} entries (capacity={}, time={})",
                        trimmed, trimmed > 0 && overflow > 0 ? "mixed" : "time", dlq.size());
            }
        } catch (Exception e) {
            log.error("Failed to clean up DLQ", e);
        }
    }

    /**
     * 监控本地降级文件，防止磁盘被写满
     */
    @Scheduled(fixedDelayString = "${rocketmq.fallback.monitor-interval-ms}")
    public void checkLocalFallback() {
        try {
            Path file = persistenceHelper.getLocalFallbackFilePath();
            if (!Files.exists(file)) {
                return;
            }
            long fileSize = Files.size(file);
            if (fileSize > 0) {
                long sizeMB = fileSize / (1024 * 1024);
                log.warn("LOCAL FALLBACK ALERT: {} exists ({}) MB, manual replay required",
                        file.toAbsolutePath(), sizeMB);
                if (sizeMB >= localFallbackMaxSizeMb) {
                    log.error("CRITICAL: Local fallback file exceeded {}MB, disk may fill up",
                            localFallbackMaxSizeMb);
                }
            }
        } catch (Exception e) {
            log.error("Failed to check local fallback file", e);
        }
    }

    /**
     * 从 DLQ JSON 条目中提取时间戳（支持 "time" 和 "moveTime" 两种字段名）
     */
    private Long extractTime(String entry) {
        try {
            Map<String, Object> map = objectMapper.readValue(
                    entry, new TypeReference<Map<String, Object>>() {});
            Object timeVal = map.get("time");
            if (timeVal == null) {
                timeVal = map.get("moveTime");
            }
            if (timeVal instanceof Number) {
                return ((Number) timeVal).longValue();
            }
        } catch (JsonProcessingException e) {
            log.debug("Failed to parse DLQ entry for time extraction", e);
        }
        return null;
    }
}