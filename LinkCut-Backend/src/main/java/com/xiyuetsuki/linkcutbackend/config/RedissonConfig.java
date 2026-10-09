package com.xiyuetsuki.linkcutbackend.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redisson 3.43 客户端配置
 */
@Configuration
public class RedissonConfig {

    @Value("${redisson.single-server-config.address}")
    private String address;

    @Value("${redisson.single-server-config.password}")
    private String password;

    @Value("${redisson.single-server-config.connection-pool-size}")
    private int connectionPoolSize;

    @Value("${redisson.single-server-config.connection-minimum-idle-size}")
    private int connectionMinimumIdleSize;

    @Value("${redisson.single-server-config.idle-connection-timeout}")
    private int idleConnectionTimeout;

    @Value("${redisson.single-server-config.timeout}")
    private int timeout;

    @Value("${redisson.single-server-config.retry-attempts}")
    private int retryAttempts;

    @Value("${redisson.single-server-config.retry-interval}")
    private int retryInterval;

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress(address)
                .setPassword(password.isBlank() ? null : password)
                .setConnectionPoolSize(connectionPoolSize)
                .setConnectionMinimumIdleSize(connectionMinimumIdleSize)
                .setIdleConnectionTimeout(idleConnectionTimeout)
                .setTimeout(timeout)
                .setRetryAttempts(retryAttempts)
                .setRetryInterval(retryInterval)
                .setSubscriptionConnectionPoolSize(16)
                .setSubscriptionConnectionMinimumIdleSize(1);
        return Redisson.create(config);
    }
}