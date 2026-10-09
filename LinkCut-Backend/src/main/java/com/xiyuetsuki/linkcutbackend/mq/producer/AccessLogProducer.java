package com.xiyuetsuki.linkcutbackend.mq.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * RocketMQ 访问日志生产者
 * <p>
 * 在短链重定向后异步发送访问日志消息，不阻塞主流程。
 */
@Component
public class AccessLogProducer {

    private static final Logger log = LoggerFactory.getLogger(AccessLogProducer.class);

    @Value("${rocketmq.consumer.access-log.topic}")
    private String topic;

    @Value("${rocketmq.consumer.access-log.tag}")
    private String tag;

    private final RocketMQTemplate rocketMQTemplate;
    private final ObjectMapper objectMapper;

    public AccessLogProducer(RocketMQTemplate rocketMQTemplate, ObjectMapper objectMapper) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.objectMapper = objectMapper;
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
            Map<String, Object> message = new HashMap<>();
            message.put("shortCode", shortCode);
            message.put("accessIp", accessIp);
            message.put("userAgent", userAgent);
            message.put("referer", referer);
            message.put("accessTime", LocalDateTime.now().toString());

            String payload = objectMapper.writeValueAsString(message);
            String destination = topic + ":" + tag;

            rocketMQTemplate.asyncSend(destination, MessageBuilder.withPayload(payload).build(),
                    new org.apache.rocketmq.client.producer.SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            log.debug("Access log sent: code={}, msgId={}", shortCode, sendResult.getMsgId());
                        }

                        @Override
                        public void onException(Throwable e) {
                            log.error("Failed to send access log: code={}", shortCode, e);
                        }
                    });
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize access log: code={}", shortCode, e);
        }
    }
}