package com.xiyuetsuki.linkcutbackend.mq.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiyuetsuki.linkcutbackend.mq.MessageTracer;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 访问计数 Increment 生产者
 * <p>
 * 短链每次被跳转时，由 {@code LinkCutController.redirect()} 调用 {@link #sendIncrement(String)}，
 * 将短链码异步发送到 RocketMQ，由 {@link com.xiyuetsuki.linkcutbackend.mq.consumer.IncrementConsumer}
 * 批量合并后执行 {@code UPDATE ... SET access_count = access_count + N}。
 * <p>
 * 与访问日志解耦为独立 Topic（{@code linkcut_access_incr}），各自独立扩展和消费。
 * <p>
 * 发送失败不降级：计数丢失可以通过 {@code t_access_log} 重算，容忍度高于日志丢失。
 */
@Slf4j
@Component
public class IncrementProducer {

    @Value("${rocketmq.access-incr.topic}")
    private String topic;

    @Value("${rocketmq.access-incr.tag}")
    private String tag;

    private final DefaultMQProducer producer;
    private final ObjectMapper objectMapper;
    private final MessageTracer messageTracer;

    public IncrementProducer(DefaultMQProducer producer,
                             ObjectMapper objectMapper,
                             MessageTracer messageTracer) {
        this.producer = producer;
        this.objectMapper = objectMapper;
        this.messageTracer = messageTracer;
    }

    /**
     * 异步发送一次访问计数 +1
     * <p>
     * 消息体仅包含 shortCode 和 messageKey，极端精简。
     * 发送失败不会触发降级队列，避免引入过多复杂度；
     * access_count 可以在必要时通过 t_access_log 重算。
     *
     * @param shortCode 短链码
     */
    public void sendIncrement(String shortCode) {
        try {
            String messageKey = UUID.randomUUID().toString();
            Map<String, Object> message = new LinkedHashMap<>(2);
            message.put("shortCode", shortCode);
            message.put("messageKey", messageKey);

            String payload = objectMapper.writeValueAsString(message);
            Message msg = new Message(topic, tag, payload.getBytes(StandardCharsets.UTF_8));

            long sendStartMs = System.currentTimeMillis();
            final String traceShortCode = shortCode;
            final String traceMessageKey = messageKey;

            producer.send(msg, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    long costMs = System.currentTimeMillis() - sendStartMs;
                    messageTracer.traceSendSuccess(traceMessageKey, topic,
                            sendResult.getMsgId(), costMs, traceShortCode);
                    log.debug("Increment sent: code={}, msgId={}, costMs={}",
                            traceShortCode, sendResult.getMsgId(), costMs);
                }

                @Override
                public void onException(Throwable e) {
                    long costMs = System.currentTimeMillis() - sendStartMs;
                    messageTracer.traceSendFail(traceMessageKey, topic,
                            e.getMessage(), costMs, traceShortCode);
                    log.warn("Failed to send increment (tolerable): code={}", traceShortCode, e);
                }
            });
        } catch (JsonProcessingException e) {
            messageTracer.traceSerializeFail(shortCode, e.getMessage());
            log.error("Failed to serialize increment: code={}", shortCode, e);
        } catch (MQClientException | RemotingException | InterruptedException e) {
            log.error("Failed to send increment async: code={}", shortCode, e);
        }
    }
}