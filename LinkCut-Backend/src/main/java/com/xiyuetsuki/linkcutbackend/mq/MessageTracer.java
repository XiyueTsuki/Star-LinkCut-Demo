package com.xiyuetsuki.linkcutbackend.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 消息轨迹记录器
 * <p>
 * 以 messageKey 作为 TraceId，在 Producer 发送和 Consumer 消费两端记录消息的生命周期事件。
 * 每条轨迹事件输出为一行 JSON，使用独立 logger "linkcut.mq.trace"，方便 grep 排查或接入 ELK/Loki 采集。
 *
 * <h3>事件类型</h3>
 * <ul>
 *   <li>SEND_SUCCESS  — Producer 异步发送成功</li>
 *   <li>SEND_FAIL     — Producer 异步发送失败（触发降级队列）</li>
 *   <li>RECEIVED      — Consumer 拉取到消息</li>
 *   <li>HANDLE_BATCH  — Consumer 批量处理完成</li>
 *   <li>HANDLE_FAIL   — Consumer 单条解析/校验失败（已入 DLQ）</li>
 * </ul>
 *
 * <h3>使用方式</h3>
 * <pre>
 * grep "traceId":"a1b2c3d4" logs/linkcut-trace.log
 * </pre>
 * 即可看到该消息在 Producer → Broker → Consumer 的完整链路。
 */
@Component
public class MessageTracer {

    private static final Logger traceLog = LoggerFactory.getLogger("linkcut.mq.trace");
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT;

    /**
     * Producer 端：记录消息发送成功
     *
     * @param messageKey 追踪 ID
     * @param topic      RocketMQ Topic
     * @param msgId      Broker 返回的消息 ID
     * @param costMs     发送耗时（毫秒）
     * @param shortCode  短链码
     */
    public void traceSendSuccess(String messageKey, String topic, String msgId,
                                  long costMs, String shortCode) {
        Map<String, Object> event = newEvent("SEND_SUCCESS", messageKey);
        event.put("topic", topic);
        event.put("msgId", msgId);
        event.put("shortCode", shortCode);
        event.put("costMs", costMs);
        traceLog.info(toJson(event));
    }

    /**
     * Producer 端：记录消息发送失败（将触发降级队列写入）
     *
     * @param messageKey 追踪 ID
     * @param topic      RocketMQ Topic
     * @param error      失败原因
     * @param costMs     发送耗时（毫秒）
     * @param shortCode  短链码
     */
    public void traceSendFail(String messageKey, String topic, String error,
                               long costMs, String shortCode) {
        Map<String, Object> event = newEvent("SEND_FAIL", messageKey);
        event.put("topic", topic);
        event.put("shortCode", shortCode);
        event.put("error", error);
        event.put("costMs", costMs);
        traceLog.warn(toJson(event));
    }

    /**
     * Producer 端：记录消息序列化失败（消息体本身有问题）
     *
     * @param shortCode 短链码
     * @param error     失败原因
     */
    public void traceSerializeFail(String shortCode, String error) {
        Map<String, Object> event = newEvent("SERIALIZE_FAIL", null);
        event.put("shortCode", shortCode);
        event.put("error", error);
        traceLog.warn(toJson(event));
    }

    /**
     * Consumer 端：记录单条消息被拉取
     *
     * @param messageKey 追踪 ID（从消息体解析）
     * @param msgId      Broker 消息 ID
     * @param delayMs    端到端延迟（当前时间 - 消息出生时间）
     */
    public void traceReceived(String messageKey, String msgId, long delayMs) {
        Map<String, Object> event = newEvent("RECEIVED", messageKey);
        event.put("msgId", msgId);
        event.put("delayMs", delayMs);
        traceLog.debug(toJson(event));
    }

    /**
     * Consumer 端：记录批量处理完成
     *
     * @param totalMsgs   本批消息总数
     * @param successCount 成功解析+入库条数
     * @param failCount   解析失败条数（已入 DLQ）
     * @param costMs      整批处理耗时（毫秒）
     */
    public void traceHandleBatch(int totalMsgs, int successCount, int failCount, long costMs) {
        Map<String, Object> event = newEvent("HANDLE_BATCH", null);
        event.put("totalMsgs", totalMsgs);
        event.put("successCount", successCount);
        event.put("failCount", failCount);
        event.put("costMs", costMs);
        traceLog.info(toJson(event));
    }

    /**
     * Consumer 端：记录单条消息解析/校验失败（已入 DLQ）
     *
     * @param msgId  Broker 消息 ID
     * @param error  失败原因
     */
    public void traceHandleFail(String msgId, String error) {
        Map<String, Object> event = newEvent("HANDLE_FAIL", null);
        event.put("msgId", msgId);
        event.put("error", error);
        traceLog.warn(toJson(event));
    }

    /**
     * Producer 端：记录降级队列重试发送
     *
     * @param payload  消息体
     * @param retries  当前重试次数
     * @param success  本次重试是否成功
     * @param costMs   耗时
     */
    @SuppressWarnings("unused")
    public void traceRetry(String payload, int retries, boolean success, long costMs) {
        Map<String, Object> event = newEvent(success ? "RETRY_SUCCESS" : "RETRY_FAIL", null);
        event.put("retries", retries);
        event.put("costMs", costMs);
        traceLog.info(toJson(event));
    }

    // ==================== 内部工具方法 ====================

    /**
     * 构建公共字段：event、timestamp
     *
     * @param eventType  事件类型
     * @param traceId    追踪 ID（messageKey）
     * @return 基础事件 Map
     */
    private Map<String, Object> newEvent(String eventType, String traceId) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("timestamp", ISO_FORMATTER.format(Instant.now()));
        event.put("event", eventType);
        if (traceId != null) {
            event.put("traceId", traceId);
        }
        return event;
    }

    /**
     * 将 Map 转为紧凑 JSON 字符串（不依赖 ObjectMapper，避免循环依赖）
     * <p>
     * 只处理 String / Number / Boolean / null，因为轨迹字段类型是可控的。
     */
    private static String toJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder(256);
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            appendJsonString(sb, entry.getKey());
            sb.append(':');
            appendJsonValue(sb, entry.getValue());
        }
        sb.append('}');
        return sb.toString();
    }

    private static void appendJsonString(StringBuilder sb, String value) {
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default   -> sb.append(c);
            }
        }
        sb.append('"');
    }

    private static void appendJsonValue(StringBuilder sb, Object value) {
        if (value == null) {
            sb.append("null");
        } else if (value instanceof String s) {
            appendJsonString(sb, s);
        } else if (value instanceof Number || value instanceof Boolean) {
            sb.append(value);
        } else {
            // 兜底：转为字符串
            appendJsonString(sb, value.toString());
        }
    }
}