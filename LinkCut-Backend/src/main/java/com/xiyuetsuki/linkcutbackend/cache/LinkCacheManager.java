package com.xiyuetsuki.linkcutbackend.cache;

import org.redisson.api.RBloomFilter;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 短链缓存管理器
 * <p>
 * 封装 Redis 缓存读写操作，包括：
 * - 短链码 → 原始URL 的 K-V 缓存
 * - 布隆过滤器的增删查
 * - 分布式锁（防缓存击穿）
 */
@Component
public class LinkCacheManager {

    private static final Logger log = LoggerFactory.getLogger(LinkCacheManager.class);

    private static final String CACHE_KEY_PREFIX = "linkcut:code:";
    private static final String LOCK_KEY_PREFIX = "linkcut:lock:";
    private static final long DEFAULT_TTL_SECONDS = TimeUnit.DAYS.toSeconds(7);
    private static final long LOCK_WAIT_SECONDS = 3;
    private static final long LOCK_LEASE_SECONDS = 10;

    private final RedissonClient redissonClient;
    private final RBloomFilter<String> bloomFilter;

    public LinkCacheManager(RedissonClient redissonClient, RBloomFilter<String> bloomFilter) {
        this.redissonClient = redissonClient;
        this.bloomFilter = bloomFilter;
    }

    /**
     * 缓存短链映射
     */
    public void putShortLink(String shortCode, String originUrl) {
        putShortLink(shortCode, originUrl, DEFAULT_TTL_SECONDS);
    }

    /**
     * 缓存短链映射（指定过期时间）
     */
    public void putShortLink(String shortCode, String originUrl, long ttlSeconds) {
        RBucket<String> bucket = redissonClient.getBucket(CACHE_KEY_PREFIX + shortCode);
        if (ttlSeconds > 0) {
            bucket.set(originUrl, ttlSeconds + randomOffset(ttlSeconds), TimeUnit.SECONDS);
        } else {
            bucket.set(originUrl);
        }
    }

    /**
     * 从缓存中获取原始URL
     *
     * @return 原始URL，缓存未命中返回 null
     */
    public String getOriginUrl(String shortCode) {
        RBucket<String> bucket = redissonClient.getBucket(CACHE_KEY_PREFIX + shortCode);
        return bucket.get();
    }

    /**
     * 检查短链码是否可能存在于布隆过滤器中
     *
     * @return true=可能存在，false=一定不存在
     */
    public boolean mightContain(String shortCode) {
        return bloomFilter.contains(shortCode);
    }

    /**
     * 将短链码添加到布隆过滤器
     */
    public void addToBloomFilter(String shortCode) {
        bloomFilter.add(shortCode);
    }

    /**
     * 获取分布式锁（用于缓存击穿保护）
     */
    public RLock getLock(String shortCode) {
        return redissonClient.getLock(LOCK_KEY_PREFIX + shortCode);
    }

    /**
     * 尝试获取分布式锁
     *
     * @return 是否获取成功
     */
    public boolean tryLock(String shortCode) {
        RLock lock = getLock(shortCode);
        try {
            return lock.tryLock(LOCK_WAIT_SECONDS, LOCK_LEASE_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 释放分布式锁
     */
    public void unlock(String shortCode) {
        RLock lock = getLock(shortCode);
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    /**
     * 获取布隆过滤器当前元素数量
     */
    public long getBloomFilterCount() {
        return bloomFilter.count();
    }

    /**
     * 在基础 TTL 上添加随机偏移（±30%），防止缓存雪崩
     */
    private long randomOffset(long baseSeconds) {
        double offset = baseSeconds * 0.3;
        long random = (long) (Math.random() * offset * 2 - offset);
        return random;
    }
}