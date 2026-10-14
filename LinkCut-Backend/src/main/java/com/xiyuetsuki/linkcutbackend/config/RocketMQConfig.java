package com.xiyuetsuki.linkcutbackend.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.xiyuetsuki.linkcutbackend.mq.consumer.AccessLogConsumer;
import com.xiyuetsuki.linkcutbackend.mq.consumer.IncrementConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RocketMQ 原生客户端配置
 * <p>
 * 不再依赖 rocketmq-spring-boot-starter，直接使用原生 {@link DefaultMQProducer}
 * 和 {@link DefaultMQPushConsumer}，通过 @Bean 管理生命周期。
 * <p>
 * Consumer 使用批量消费模式：一次拉取 batchMaxSize 条消息，
 * 通过 {@link AccessLogConsumer} 批量解析并 INSERT IGNORE 写入 MySQL。
 */
@Slf4j
@Configuration
public class RocketMQConfig {

    @Value("${rocketmq.name-server}")
    private String nameServer;

    @Value("${rocketmq.producer.group}")
    private String producerGroup;

    @Value("${rocketmq.producer.send-msg-timeout}")
    private int sendMsgTimeout;

    @Value("${rocketmq.producer.retry-times}")
    private int retryTimes;

    @Value("${rocketmq.consumer.group}")
    private String consumerGroup;

    @Value("${rocketmq.consumer.thread-max}")
    private int consumeThreadMax;

    @Value("${rocketmq.consumer.batch-max-size}")
    private int batchMaxSize;

    @Value("${rocketmq.consumer.max-reconsume-times}")
    private int maxReconsumeTimes;

    @Value("${rocketmq.access-log.topic}")
    private String topic;

    @Value("${rocketmq.access-log.tag}")
    private String tag;

    @Value("${rocketmq.access-incr.consumer-group}")
    private String incrConsumerGroup;

    @Value("${rocketmq.access-incr.topic}")
    private String incrTopic;

    @Value("${rocketmq.access-incr.tag}")
    private String incrTag;

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public DefaultMQProducer defaultMQProducer() throws MQClientException {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(nameServer);
        producer.setSendMsgTimeout(sendMsgTimeout);
        producer.setRetryTimesWhenSendFailed(retryTimes);
        producer.setRetryTimesWhenSendAsyncFailed(retryTimes);
        log.info("RocketMQ Producer started: group={}, nameServer={}", producerGroup, nameServer);
        return producer;
    }

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public DefaultMQPushConsumer defaultMQPushConsumer(AccessLogConsumer accessLogConsumer) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(nameServer);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMax(consumeThreadMax);
        consumer.setConsumeMessageBatchMaxSize(batchMaxSize);
        consumer.setMaxReconsumeTimes(maxReconsumeTimes);
        consumer.subscribe(topic, tag);

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            accessLogConsumer.handleBatch(msgs);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        log.info("RocketMQ Consumer started: group={}, batchMaxSize={}, threadMax={}",
                consumerGroup, batchMaxSize, consumeThreadMax);
        return consumer;
    }

    /**
     * 访问计数增量 Consumer
     * <p>
     * 独立的 ConsumerGroup 和 Topic（{@code linkcut_access_incr}），
     * 与访问日志 Consumer 隔离，避免互相影响消费进度。
     * <p>
     * 线程数和批量大小沿用主 Consumer 配置，增量消息极小（~100 字节），压力可控。
     */
    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public DefaultMQPushConsumer incrPushConsumer(IncrementConsumer incrementConsumer) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(incrConsumerGroup);
        consumer.setNamesrvAddr(nameServer);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMax(Math.max(4, consumeThreadMax / 2));
        consumer.setConsumeThreadMin(Math.max(2, consumeThreadMax / 4));
        consumer.setConsumeMessageBatchMaxSize(batchMaxSize);
        consumer.setMaxReconsumeTimes(maxReconsumeTimes);
        consumer.subscribe(incrTopic, incrTag);

        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            incrementConsumer.handleBatch(msgs);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        log.info("RocketMQ Increment Consumer started: group={}, topic={}, batchMaxSize={}",
                incrConsumerGroup, incrTopic, batchMaxSize);
        return consumer;
    }
}