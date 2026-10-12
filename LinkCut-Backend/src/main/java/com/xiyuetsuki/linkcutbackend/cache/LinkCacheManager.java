package com.xiyuetsuki.linkcutbackend.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RBucket;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 短链缓存管理器
 * <p>
 * 两级缓存架构：
 * <ul>
 *   <li>L1 - Caffeine 本地缓存：JVM 堆内存，0 网络延迟，TTL 较短（可配置，默认 5 分钟）</li>
 *   <li>L2 - Redis 远程缓存：分布式 K-V 存储，TTL 与短链过期时间对齐</li>
 * </ul>
 * 此外还封装布隆过滤器和分布式锁。
 * <p>
 * 写操作同时写入 L1 和 L2；读操作按 L1 → L2 级联查询，L2 命中时自动回填 L1。
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
    private final Cache<String, String> localCache;

    public LinkCacheManager(RedissonClient redissonClient,
                            RBloomFilter<String> bloomFilter,
                            @Value("${linkcut.caffeine.max-size}") long maxSize,
                            @Value("${linkcut.caffeine.expire-minutes}") long expireMinutes) {
        this.redissonClient = redissonClient;
        this.bloomFilter = bloomFilter;
        this.localCache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireMinutes, TimeUnit.MINUTES)
                .recordStats()
                .build();
        log.info("Caffeine local cache initialized: maxSize={}, expireMinutes={}", maxSize, expireMinutes);
    }

    /**
     * 缓存短链映射（写入 L1 + L2）
     */
    public void putShortLink(String shortCode, String originUrl) {
        putShortLink(shortCode, originUrl, DEFAULT_TTL_SECONDS);
    }

    /**
     * 缓存短链映射（写入 L1 + L2，指定 L2 过期时间）
     */
    public void putShortLink(String shortCode, String originUrl, long ttlSeconds) {
        localCache.put(shortCode, originUrl);
        RBucket<String> bucket = redissonClient.getBucket(CACHE_KEY_PREFIX + shortCode);
        if (ttlSeconds > 0) {
            bucket.set(originUrl, ttlSeconds + randomOffset(ttlSeconds), TimeUnit.SECONDS);
        } else {
            bucket.set(originUrl);
        }
    }

    /**
     * 从缓存中获取原始URL（L1 → L2 级联查询，L2 命中时回填 L1）
     *
     * @return 原始URL，缓存未命中返回 null
     */
    public String getOriginUrl(String shortCode) {
        String originUrl = localCache.getIfPresent(shortCode);
        if (originUrl != null) {
            log.debug("L1 cache hit: shortCode={}", shortCode);
            return originUrl;
        }

        RBucket<String> bucket = redissonClient.getBucket(CACHE_KEY_PREFIX + shortCode);
        originUrl = bucket.get();
        if (originUrl != null) {
            localCache.put(shortCode, originUrl);
            log.debug("L2 cache hit, backfilled L1: shortCode={}", shortCode);
        }
        return originUrl;
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