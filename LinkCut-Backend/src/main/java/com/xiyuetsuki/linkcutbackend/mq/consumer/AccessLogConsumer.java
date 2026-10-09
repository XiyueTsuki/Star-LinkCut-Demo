package com.xiyuetsuki.linkcutbackend.mq.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xiyuetsuki.linkcutbackend.entity.AccessLog;
import com.xiyuetsuki.linkcutbackend.repository.AccessLogRepository;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * RocketMQ 访问日志消费者
 * <p>
 * 消费访问日志消息，批量写入 MySQL t_access_log 表。
 * 采用异步消费模式，不阻塞 MQ 投递。
 */
@Component
@RocketMQMessageListener(
        topic = "${rocketmq.consumer.access-log.topic}",
        selectorExpression = "${rocketmq.consumer.access-log.tag}",
        consumerGroup = "${rocketmq.consumer.group}"
)
public class AccessLogConsumer implements RocketMQListener<String> {

    private static final Logger log = LoggerFactory.getLogger(AccessLogConsumer.class);

    private final AccessLogRepository accessLogRepository;
    private final ObjectMapper objectMapper;

    public AccessLogConsumer(AccessLogRepository accessLogRepository, ObjectMapper objectMapper) {
        this.accessLogRepository = accessLogRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public void onMessage(String message) {
        try {
            Map<String, Object> map = objectMapper.readValue(message,
                    new TypeReference<Map<String, Object>>() {});

            AccessLog accessLog = AccessLog.builder()
                    .shortCode((String) map.get("shortCode"))
                    .accessIp((String) map.get("accessIp"))
                    .userAgent((String) map.get("userAgent"))
                    .referer((String) map.get("referer"))
                    .accessTime(LocalDateTime.parse((String) map.get("accessTime")))
                    .build();

            accessLogRepository.insert(accessLog);
            log.debug("Access log saved: code={}, ip={}", accessLog.getShortCode(), accessLog.getAccessIp());
        } catch (Exception e) {
            log.error("Failed to consume access log: {}", message, e);
        }
    }
}