package com.xiyuetsuki.linkcutbackend.mq.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.api.RList;
import org.redisson.api.RQueue;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 消息持久化助手（三级降级链）
 * <p>
 * 为 Producer 的降级队列写入和 Consumer 的死信队列写入提供统一的三级降级保障：
 * <ol>
 *   <li>直接写入 Redis（RQueue / RList）</li>
 *   <li>Redis 写入失败 → 重试（指数退避，可配次数和间隔）</li>
 *   <li>重试全部失败 → 写入本地文件作为终极兜底</li>
 * </ol>
 */
@Component
public class MessagePersistenceHelper {

    private static final Logger log = LoggerFactory.getLogger(MessagePersistenceHelper.class);
    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private final RedissonClient redissonClient;
    private final ObjectMapper objectMapper;

    @Value("${rocketmq.fallback.dlq-write-retries}")
    private int dlqWriteRetries;

    @Value("${rocketmq.fallback.dlq-write-retry-interval-ms}")
    private long dlqWriteRetryIntervalMs;

    @Value("${rocketmq.fallback.local-fallback-dir}")
    private String localFallbackDir;

    public MessagePersistenceHelper(RedissonClient redissonClient,
                                    ObjectMapper objectMapper) {
        this.redissonClient = redissonClient;
        this.objectMapper = objectMapper;
    }

    /**
     * 安全写入 Redis RQueue（降级队列）
     *
     * @param queueKey     Redis 队列 key
     * @param payload      序列化后的消息 JSON
     * @param maxQueueSize 队列最大容量，超限时拒绝写入
     * @param targetName   目标名称（用于日志）
     * @return true=写入成功，false=全部降级链路失败
     */
    public boolean safeWriteQueue(String queueKey, String payload, int maxQueueSize, String targetName) {
        return safePersist(() -> {
            RQueue<String> queue = redissonClient.getQueue(queueKey);
            if (queue.size() >= maxQueueSize) {
                throw new IllegalStateException("Queue full: " + queue.size());
            }
            queue.add(payload);
            return true;
        }, targetName, payload);
    }

    /**
     * 安全写入 Redis RList（死信队列 DLQ）
     *
     * @param listKey    Redis 列表 key
     * @param payload    序列化后的消息 JSON
     * @param targetName 目标名称（用于日志）
     * @return true=写入成功，false=全部降级链路失败
     */
    public boolean safeWriteList(String listKey, String payload, String targetName) {
        return safePersist(() -> {
            RList<String> list = redissonClient.getList(listKey);
            list.add(payload);
            return true;
        }, targetName, payload);
    }

    /**
     * 三级降级链核心逻辑
     *
     * @param redisWrite Redis 写入操作
     * @param targetName 目标名称（fallback-queue / dlq）
     * @param payload    消息 JSON
     * @return true=写入成功
     */
    private boolean safePersist(Supplier<Boolean> redisWrite, String targetName, String payload) {
        // Level 1: 直接写 Redis
        try {
            if (redisWrite.get()) {
                return true;
            }
        } catch (Exception e) {
            log.warn("[{}] Level-1 Redis write failed: {}", targetName, e.getMessage());
        }

        // Level 2: 重试 Redis（指数退避）
        for (int i = 0; i < dlqWriteRetries; i++) {
            long backoffMs = dlqWriteRetryIntervalMs * (1L << i);
            try {
                Thread.sleep(backoffMs);
                if (redisWrite.get()) {
                    log.warn("[{}] Level-2 Redis write recovered after {} retries ({}ms)",
                            targetName, i + 1, backoffMs);
                    return true;
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("[{}] Level-2 retry {}/{} failed: {}",
                        targetName, i + 1, dlqWriteRetries, e.getMessage());
            }
        }

        // Level 3: 写本地文件（终极兜底）
        try {
            appendToLocalFile(targetName, payload);
            return true;
        } catch (Exception e) {
            log.error("[{}] Level-3 local file write also FAILED, message permanently lost", targetName, e);
            return false;
        }
    }

    /**
     * 追加写入本地降级文件（每行一条 JSON）
     *
     * @param target  目标名称
     * @param payload 消息 JSON
     */
    private void appendToLocalFile(String target, String payload) throws IOException {
        Path dir = Paths.get(localFallbackDir);
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }

        Path file = dir.resolve("mq_fallback.log");
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("timestamp", LocalDateTime.now().format(DT_FMT));
        entry.put("target", target);
        entry.put("payload", payload);

        String line = objectMapper.writeValueAsString(entry) + System.lineSeparator();
        Files.write(file, line.getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        log.warn("[{}] Message persisted to local file: {} ({} bytes)",
                target, file.toAbsolutePath(), line.length());
    }

    /**
     * 获取本地降级文件路径（供 Monitor 使用）
     */
    public Path getLocalFallbackFilePath() {
        return Paths.get(localFallbackDir, "mq_fallback.log");
    }
}