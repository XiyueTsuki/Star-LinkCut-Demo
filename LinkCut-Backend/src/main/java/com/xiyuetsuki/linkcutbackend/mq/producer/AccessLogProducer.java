package com.xiyuetsuki.linkcutbackend.mq.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiyuetsuki.linkcutbackend.mq.MessagePersistenceHelper;
import com.xiyuetsuki.linkcutbackend.mq.MessageTracer;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.redisson.api.RQueue;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * RocketMQ 访问日志生产者（原生客户端 API）
 * <p>
 * 在短链重定向后异步发送访问日志消息，不阻塞主流程。
 * 发送失败时降级写入 Redis List 待重试队列，通过定时任务补偿重发。
 * 当降级队列水位超过阈值时，自动精简消息体以降低积压压力。
 */
@Slf4j
@Component
public class AccessLogProducer {

    @Value("${rocketmq.access-log.topic}")
    private String topic;

    @Value("${rocketmq.access-log.tag}")
    private String tag;

    @Value("${rocketmq.fallback.queue-key}")
    private String fallbackQueueKey;

    @Value("${rocketmq.fallback.dlq-key}")
    private String dlqKey;

    @Value("${rocketmq.fallback.max-queue-size}")
    private int maxQueueSize;

    @Value("${rocketmq.fallback.max-retries}")
    private int maxRetries;

    @Value("${rocketmq.fallback.backlog-warn-threshold}")
    private double backlogWarnThreshold;

    private final DefaultMQProducer producer;
    private final ObjectMapper objectMapper;
    private final RedissonClient redissonClient;
    private final MessagePersistenceHelper persistenceHelper;
    private final MessageTracer messageTracer;

    public AccessLogProducer(DefaultMQProducer producer,
                             ObjectMapper objectMapper,
                             RedissonClient redissonClient,
                             MessagePersistenceHelper persistenceHelper,
                             MessageTracer messageTracer) {
        this.producer = producer;
        this.objectMapper = objectMapper;
        this.redissonClient = redissonClient;
        this.persistenceHelper = persistenceHelper;
        this.messageTracer = messageTracer;
    }

    /**
     * 异步发送访问日志消息
     *
     * @param shortCode 短链码
     * @param accessIp  访问者IP
     * @param userAgent 浏览器UA
     * @param referer   来源
     */
    public void sendAccessLog(String shortCode, String accessIp, String userAgent, String referer) {
        try {
            String messageKey = UUID.randomUUID().toString();
            Map<String, Object> message = new HashMap<>();
            message.put("shortCode", shortCode);
            message.put("accessIp", accessIp);
            message.put("accessTime", LocalDateTime.now().toString());
            message.put("messageKey", messageKey);

            boolean degraded = isBacklogDegraded();
            if (degraded) {
                log.debug("Backlog degraded mode: dropping userAgent/referer for code={}", shortCode);
            } else {
                message.put("userAgent", userAgent);
                message.put("referer", referer);
            }

            String payload = objectMapper.writeValueAsString(message);
            Message msg = new Message(topic, tag, payload.getBytes(StandardCharsets.UTF_8));

            // 记录发送开始时间，用于在回调中计算耗时
            long sendStartMs = System.currentTimeMillis();
            final String traceShortCode = shortCode;
            final String traceMessageKey = messageKey;
            final String tracePayload = payload;

            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    long costMs = System.currentTimeMillis() - sendStartMs;
                    messageTracer.traceSendSuccess(traceMessageKey, topic,
                            sendResult.getMsgId(), costMs, traceShortCode);
                    log.debug("Access log sent: code={}, msgId={}, costMs={}",
                            traceShortCode, sendResult.getMsgId(), costMs);
                }

                @Override
                public void onException(Throwable e) {
                    long costMs = System.currentTimeMillis() - sendStartMs;
                    messageTracer.traceSendFail(traceMessageKey, topic,
                            e.getMessage(), costMs, traceShortCode);
                    log.error("Failed to send access log, fallback to Redis: code={}", traceShortCode, e);
                    fallbackToRedis(tracePayload, 0);
                }
            });
        } catch (JsonProcessingException e) {
            messageTracer.traceSerializeFail(shortCode, e.getMessage());
            log.error("Failed to serialize access log: code={}", shortCode, e);
        } catch (MQClientException | RemotingException | InterruptedException e) {
            messageTracer.traceSendFail(null, topic, e.getMessage(), -1, shortCode);
            log.error("Failed to send access log async: code={}", shortCode, e);
        }
    }

    /**
     * 检查降级队列是否达到警告水位，触发消息体精简
     */
    private boolean isBacklogDegraded() {
        try {
            RQueue<String> fallbackQueue = redissonClient.getQueue(fallbackQueueKey);
            return fallbackQueue.size() >= maxQueueSize * backlogWarnThreshold;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 降级写入 Redis 待重试队列（三级降级链：Redis → 重试 → 本地文件）
     *
     * @param payload 消息 JSON
     * @param retries 已重试次数
     */
    private void fallbackToRedis(String payload, int retries) {
        try {
            RQueue<String> fallbackQueue = redissonClient.getQueue(fallbackQueueKey);
            if (fallbackQueue.size() >= maxQueueSize) {
                log.error("Fallback queue full ({}), message discarded", fallbackQueue.size());
                return;
            }
            Map<String, Object> envelope = new HashMap<>();
            envelope.put("payload", payload);
            envelope.put("retries", retries);
            envelope.put("createTime", System.currentTimeMillis());
            String envelopeJson = objectMapper.writeValueAsString(envelope);
            boolean ok = persistenceHelper.safeWriteQueue(fallbackQueueKey, envelopeJson,
                    maxQueueSize, "fallback-queue");
            if (!ok) {
                log.error("Fallback queue persistence failed at all levels, message permanently lost");
            }
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize fallback envelope, message discarded", e);
        }
    }

    /**
     * 定时任务：从 Redis 降级队列取出消息并重试发送
     */
    @Scheduled(fixedDelayString = "${rocketmq.fallback.retry-interval-ms}")
    public void retryFailedMessages() {
        RQueue<String> fallbackQueue = redissonClient.getQueue(fallbackQueueKey);
        if (fallbackQueue.isEmpty()) {
            return;
        }

        log.debug("Retry started: fallback queue size = {}", fallbackQueue.size());
        int processed = 0;
        int successCount = 0;

        while (processed < 100) {
            String envelopeJson = fallbackQueue.poll();
            if (envelopeJson == null) {
                break;
            }
            processed++;

            try {
                @SuppressWarnings("unchecked")
                Map<String, Object> envelope = objectMapper.readValue(envelopeJson, Map.class);
                String payload = (String) envelope.get("payload");
                int retries = envelope.get("retries") instanceof Number
                        ? ((Number) envelope.get("retries")).intValue() : 0;

                if (retries >= maxRetries) {
                    moveToDlq(payload, retries);
                    continue;
                }

                Message msg = new Message(topic, tag, payload.getBytes(StandardCharsets.UTF_8));
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        log.debug("Fallback message retry success: msgId={}", sendResult.getMsgId());
                    }

                    @Override
                    public void onException(Throwable e) {
                        log.warn("Fallback message retry failed (retries={})", retries, e);
                        fallbackToRedis(payload, retries + 1);
                    }
                });
                successCount++;
            } catch (Exception e) {
                log.error("Failed to parse/retry fallback message, discarded", e);
            }
        }

        if (processed > 0) {
            log.info("Retry completed: processed={}, success={}, remaining={}",
                    processed, successCount, fallbackQueue.size());
        }
    }

    /**
     * 超过最大重试次数，移入死信队列（三级降级链：Redis → 重试 → 本地文件）
     */
    private void moveToDlq(String payload, int retries) {
        Map<String, Object> dlqEntry = new HashMap<>();
        dlqEntry.put("payload", payload);
        dlqEntry.put("retries", retries);
        dlqEntry.put("moveTime", System.currentTimeMillis());
        try {
            String envelopeJson = objectMapper.writeValueAsString(dlqEntry);
            boolean ok = persistenceHelper.safeWriteList(dlqKey, envelopeJson, "dlq");
            if (ok) {
                log.error("Message moved to DLQ after {} retries", retries);
            } else {
                log.error("DLQ persistence failed at all levels, message permanently lost");
            }
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize DLQ entry, message discarded", e);
        }
    }
}