package com.xiyuetsuki.linkcutbackend.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

/**
 * Redisson 3.43 客户端配置 — Sentinel 哨兵模式
 * <p>
 * 客户端通过 Sentinel 节点自动发现当前主节点，
 * 主节点故障时 Sentinel 完成自动故障转移，客户端无感知切换。
 */
@Configuration
public class RedissonConfig {

    @Value("${redisson.sentinel-config.sentinel-addresses}")
    private String sentinelAddresses;

    @Value("${redisson.sentinel-config.master-name}")
    private String masterName;

    @Value("${redisson.sentinel-config.password}")
    private String password;

    @Value("${redisson.sentinel-config.database}")
    private int database;

    @Value("${redisson.sentinel-config.connection-pool-size}")
    private int connectionPoolSize;

    @Value("${redisson.sentinel-config.connection-minimum-idle-size}")
    private int connectionMinimumIdleSize;

    @Value("${redisson.sentinel-config.idle-connection-timeout}")
    private int idleConnectionTimeout;

    @Value("${redisson.sentinel-config.timeout}")
    private int timeout;

    @Value("${redisson.sentinel-config.retry-attempts}")
    private int retryAttempts;

    @Value("${redisson.sentinel-config.retry-interval}")
    private int retryInterval;

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient() {
        String[] addresses = Arrays.stream(sentinelAddresses.split(","))
                .map(String::trim)
                .toArray(String[]::new);

        Config config = new Config();
        config.useSentinelServers()
                .setMasterName(masterName)
                .addSentinelAddress(addresses)
                .setCheckSentinelsList(false)
                .setReadMode(ReadMode.MASTER_SLAVE)
                .setPassword(password.isBlank() ? null : password)
                .setDatabase(database)
                .setMasterConnectionPoolSize(connectionPoolSize)
                .setSlaveConnectionPoolSize(connectionPoolSize)
                .setMasterConnectionMinimumIdleSize(connectionMinimumIdleSize)
                .setSlaveConnectionMinimumIdleSize(connectionMinimumIdleSize)
                .setIdleConnectionTimeout(idleConnectionTimeout)
                .setTimeout(timeout)
                .setRetryAttempts(retryAttempts)
                .setRetryInterval(retryInterval);
        return Redisson.create(config);
    }
}