package com.xiyuetsuki.linkcutbackend.mq.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiyuetsuki.linkcutbackend.config.RocketMQConfig;
import com.xiyuetsuki.linkcutbackend.entity.AccessLog;
import com.xiyuetsuki.linkcutbackend.mq.MessagePersistenceHelper;
import com.xiyuetsuki.linkcutbackend.mq.MessageTracer;
import com.xiyuetsuki.linkcutbackend.repository.AccessLogRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 访问日志消费者（业务包装）
 * <p>
 * 由 {@link RocketMQConfig} 中的 {@code DefaultMQPushConsumer} 回调调用，负责：
 * 1. 批量反序列化 RocketMQ MessageExt → AccessLog
 * 2. 批量 INSERT IGNORE 写入 MySQL
 * 3. 解析/校验失败的单条消息写入 Redis 死信队列，不阻塞整批
 * <p>
 * 对应 Producer 端：{@code AccessLogProducer}
 */
@Slf4j
@Component
public class AccessLogConsumer {

    private final AccessLogRepository accessLogRepository;
    private final ObjectMapper objectMapper;
    private final MessagePersistenceHelper persistenceHelper;
    private final MessageTracer messageTracer;

    @Value("${rocketmq.fallback.dlq-key}")
    private String dlqKey;

    public AccessLogConsumer(AccessLogRepository accessLogRepository,
                             ObjectMapper objectMapper,
                             MessagePersistenceHelper persistenceHelper,
                             MessageTracer messageTracer) {
        this.accessLogRepository = accessLogRepository;
        this.objectMapper = objectMapper;
        this.persistenceHelper = persistenceHelper;
        this.messageTracer = messageTracer;
    }

    /**
     * 批量处理消息：解析 → 校验 → INSERT IGNORE
     * <p>
     * 由 RocketMQ 原生 MessageListenerConcurrently 回调，
     * msgs 已是批量拉取的结果（batchMaxSize 条）。
     * <p>
     * 每条消息记录 RECEIVED 轨迹（含端到端延迟），整批完成后记录 HANDLE_BATCH 汇总轨迹。
     */
    public void handleBatch(List<MessageExt> msgs) {
        long batchStartMs = System.currentTimeMillis();
        int totalMsgs = msgs.size();
        List<AccessLog> batch = new ArrayList<>(totalMsgs);
        int successCount = 0;

        for (MessageExt msg : msgs) {
            AccessLog accessLog = parseMessage(msg);
            if (accessLog != null) {
                batch.add(accessLog);
                successCount++;
                // 记录每条消息的端到端延迟
                long delayMs = System.currentTimeMillis() - msg.getBornTimestamp();
                messageTracer.traceReceived(accessLog.getMessageKey(), msg.getMsgId(), delayMs);
            }
        }

        int failCount = totalMsgs - successCount;
        if (batch.isEmpty()) {
            log.debug("Batch all invalid, skipped, size={}", totalMsgs);
            messageTracer.traceHandleBatch(totalMsgs, 0, failCount,
                    System.currentTimeMillis() - batchStartMs);
            return;
        }

        try {
            accessLogRepository.insertIgnoreBatch(batch);
            log.debug("Batch saved: msgs={}, success={}", totalMsgs, successCount);
        } catch (DataAccessException e) {
            log.error("Batch insert failed (size={}), attempting individual fallback", batch.size(), e);
            for (AccessLog item : batch) {
                try {
                    accessLogRepository.insertIgnore(item);
                } catch (Exception ex) {
                    log.error("Individual insert failed for messageKey={}", item.getMessageKey(), ex);
                }
            }
        }

        long totalCost = System.currentTimeMillis() - batchStartMs;
        messageTracer.traceHandleBatch(totalMsgs, successCount, failCount, totalCost);
    }

    /**
     * 解析单条消息为 AccessLog，失败时写入 DLQ 并返回 null
     */
    private AccessLog parseMessage(MessageExt msg) {
        String body = new String(msg.getBody(), StandardCharsets.UTF_8);
        Map<String, Object> map;
        try {
            map = objectMapper.readValue(body, new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize message, moved to DLQ: msgId={}", msg.getMsgId(), e);
            messageTracer.traceHandleFail(msg.getMsgId(), "Deserialize: " + e.getMessage());
            moveToDlq(body, e.getClass().getSimpleName() + ": " + e.getMessage());
            return null;
        }

        try {
            String shortCode = (String) map.get("shortCode");
            if (shortCode == null || shortCode.isBlank()) {
                throw new IllegalArgumentException("shortCode is required");
            }

            String accessTimeStr = (String) map.get("accessTime");
            LocalDateTime accessTime;
            try {
                accessTime = LocalDateTime.parse(accessTimeStr);
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid accessTime: " + accessTimeStr, e);
            }

            return AccessLog.builder()
                    .shortCode(shortCode)
                    .accessIp((String) map.get("accessIp"))
                    .userAgent((String) map.get("userAgent"))
                    .referer((String) map.get("referer"))
                    .accessTime(accessTime)
                    .messageKey((String) map.get("messageKey"))
                    .build();
        } catch (IllegalArgumentException e) {
            log.error("Invalid message data, moved to DLQ: msgId={}", msg.getMsgId(), e);
            messageTracer.traceHandleFail(msg.getMsgId(), "Validate: " + e.getMessage());
            moveToDlq(body, e.getClass().getSimpleName() + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * 写入 Redis 死信队列（三级降级链：Redis → 重试 → 本地文件）
     */
    private void moveToDlq(String originalMessage, String errorReason) {
        Map<String, Object> dlqEntry = new HashMap<>();
        dlqEntry.put("message", originalMessage);
        dlqEntry.put("error", errorReason);
        dlqEntry.put("time", System.currentTimeMillis());
        try {
            String payload = objectMapper.writeValueAsString(dlqEntry);
            boolean ok = persistenceHelper.safeWriteList(dlqKey, payload, "dlq");
            if (!ok) {
                log.error("DLQ persistence failed at all levels, message permanently lost");
            }
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize DLQ entry, message discarded", e);
        }
    }
}