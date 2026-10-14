package com.xiyuetsuki.linkcutbackend.mq.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiyuetsuki.linkcutbackend.mq.MessageTracer;
import com.xiyuetsuki.linkcutbackend.repository.ShortLinkRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 访问计数 Increment 消费者
 * <p>
 * 从 {@code linkcut_access_incr} Topic 批量拉取 increment 消息，
 * 按 shortCode 合并计数后，逐条执行 {@code UPDATE ... SET access_count = access_count + N}。
 * <p>
 * <b>合并逻辑</b>：同一批次中某 shortCode 出现 8 次，则只执行 1 条 UPDATE 加 8，
 * 而非 8 条 +1 UPDATE，显著降低 DB 写压力。
 * <p>
 * 消费失败不触发降级/DLQ：计数丢失可通过 {@code t_access_log} 重算，容忍度较高。
 */
@Slf4j
@Component
public class IncrementConsumer {

    private final ShortLinkRepository shortLinkRepository;
    private final ObjectMapper objectMapper;
    private final MessageTracer messageTracer;

    public IncrementConsumer(ShortLinkRepository shortLinkRepository,
                             ObjectMapper objectMapper,
                             MessageTracer messageTracer) {
        this.shortLinkRepository = shortLinkRepository;
        this.objectMapper = objectMapper;
        this.messageTracer = messageTracer;
    }

    /**
     * 批量处理 increment 消息：合并 → 更新 DB
     * <p>
     * 由 RocketMQ MessageListenerConcurrently 回调触发。
     * 每批次中同一 shortCode 的多条 +1 合并为一次 +N 的 UPDATE 操作。
     *
     * @param msgs RocketMQ 批量拉取的消息列表
     */
    public void handleBatch(List<MessageExt> msgs) {
        long batchStartMs = System.currentTimeMillis();
        int totalMsgs = msgs.size();

        // 1. 合并：统计每批次中每个 shortCode 的累计次数
        Map<String, Integer> mergeMap = new LinkedHashMap<>();
        for (MessageExt msg : msgs) {
            try {
                String body = new String(msg.getBody(), StandardCharsets.UTF_8);
                Map<String, String> map = objectMapper.readValue(
                        body, new TypeReference<Map<String, String>>() {});
                String shortCode = map.get("shortCode");
                if (shortCode != null && !shortCode.isEmpty()) {
                    mergeMap.merge(shortCode, 1, Integer::sum);
                }
            } catch (Exception e) {
                // 单条解析失败跳过，不影响整批
                log.error("Failed to parse increment message: msgId={}", msg.getMsgId(), e);
                messageTracer.traceHandleFail(msg.getMsgId(),
                        "Parse increment: " + e.getMessage());
            }
        }

        if (mergeMap.isEmpty()) {
            log.debug("Increment batch all invalid, skipped, size={}", totalMsgs);
            messageTracer.traceHandleBatch(totalMsgs, 0, totalMsgs,
                    System.currentTimeMillis() - batchStartMs);
            return;
        }

        // 2. 批量更新：每个 shortCode 一条 UPDATE ... SET access_count + N
        int successCount = 0;
        int failCount = 0;
        for (Map.Entry<String, Integer> entry : mergeMap.entrySet()) {
            try {
                shortLinkRepository.incrementAccessCount(entry.getKey(), entry.getValue());
                successCount += entry.getValue();  // 按原始消息条数统计
            } catch (DataAccessException e) {
                log.error("Failed to increment access_count: shortCode={}, delta={}",
                        entry.getKey(), entry.getValue(), e);
                failCount += entry.getValue();
            }
        }

        long totalCost = System.currentTimeMillis() - batchStartMs;
        log.debug("Increment batch done: msgs={}, distinctCodes={}, success={}, fail={}, costMs={}",
                totalMsgs, mergeMap.size(), successCount, failCount, totalCost);
        messageTracer.traceHandleBatch(totalMsgs, successCount, failCount, totalCost);
    }
}