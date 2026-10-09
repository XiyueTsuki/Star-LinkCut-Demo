package com.xiyuetsuki.linkcutbackend.service.impl;

import com.xiyuetsuki.linkcutbackend.cache.LinkCacheManager;
import com.xiyuetsuki.linkcutbackend.dto.CreateLinkRequest;
import com.xiyuetsuki.linkcutbackend.dto.CreateLinkResponse;
import com.xiyuetsuki.linkcutbackend.entity.ShortCodeSnapshot;
import com.xiyuetsuki.linkcutbackend.entity.ShortLink;
import com.xiyuetsuki.linkcutbackend.generator.ShortCodeGenerator;
import com.xiyuetsuki.linkcutbackend.repository.ShortCodeSnapshotRepository;
import com.xiyuetsuki.linkcutbackend.repository.ShortLinkRepository;
import com.xiyuetsuki.linkcutbackend.service.LinkCutService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

/**
 * 短链服务实现
 * <p>
 * 核心逻辑：
 * 1. 创建：Snowflake生成ID → Base62编码 → 冲突检测 → MySQL持久化 → Redis缓存 → 布隆过滤器
 * 2. 查询：布隆过滤器 → Redis缓存 → (分布式锁) → MySQL → 回写缓存
 */
@Service
public class LinkCutServiceImpl implements LinkCutService {

    private static final Logger log = LoggerFactory.getLogger(LinkCutServiceImpl.class);
    private static final int MAX_GENERATE_RETRIES = 5;

    @Value("${linkcut.domain}")
    private String domain;

    private final ShortCodeGenerator shortCodeGenerator;
    private final ShortLinkRepository shortLinkRepository;
    private final ShortCodeSnapshotRepository shortCodeSnapshotRepository;
    private final LinkCacheManager linkCacheManager;

    public LinkCutServiceImpl(ShortCodeGenerator shortCodeGenerator,
                              ShortLinkRepository shortLinkRepository,
                              ShortCodeSnapshotRepository shortCodeSnapshotRepository,
                              LinkCacheManager linkCacheManager) {
        this.shortCodeGenerator = shortCodeGenerator;
        this.shortLinkRepository = shortLinkRepository;
        this.shortCodeSnapshotRepository = shortCodeSnapshotRepository;
        this.linkCacheManager = linkCacheManager;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CreateLinkResponse createShortLink(CreateLinkRequest request, String creatorIp) {
        String originUrl = request.getOriginUrl();
        Long expireSeconds = request.getExpireSeconds();
        LocalDateTime expireTime = null;
        if (expireSeconds != null && expireSeconds > 0) {
            expireTime = LocalDateTime.now().plusSeconds(expireSeconds);
        }

        String shortCode = generateUniqueShortCode();

        ShortLink shortLink = ShortLink.builder()
                .shortCode(shortCode)
                .originUrl(originUrl)
                .expireTime(expireTime)
                .creatorIp(creatorIp)
                .accessCount(0L)
                .status(1)
                .build();
        shortLinkRepository.insert(shortLink);

        linkCacheManager.putShortLink(shortCode, originUrl);
        linkCacheManager.addToBloomFilter(shortCode);

        shortCodeSnapshotRepository.insert(
                ShortCodeSnapshot.builder().shortCode(shortCode).build());

        log.info("Short link created: code={}, originUrl={}, expireTime={}", shortCode, originUrl, expireTime);

        return CreateLinkResponse.builder()
                .shortCode(shortCode)
                .shortUrl(domain + "/" + shortCode)
                .originUrl(originUrl)
                .expireTime(expireTime)
                .build();
    }

    @Override
    public String getOriginUrl(String shortCode) {
        if (!linkCacheManager.mightContain(shortCode)) {
            log.debug("BloomFilter miss: shortCode={}", shortCode);
            return null;
        }

        String originUrl = linkCacheManager.getOriginUrl(shortCode);
        if (originUrl != null) {
            log.debug("Redis cache hit: shortCode={}", shortCode);
            return originUrl;
        }

        log.debug("Redis cache miss, query DB with distributed lock: shortCode={}", shortCode);
        if (!linkCacheManager.tryLock(shortCode)) {
            log.warn("Failed to acquire distributed lock: shortCode={}", shortCode);
            originUrl = linkCacheManager.getOriginUrl(shortCode);
            if (originUrl != null) {
                return originUrl;
            }
            return null;
        }

        try {
            originUrl = linkCacheManager.getOriginUrl(shortCode);
            if (originUrl != null) {
                return originUrl;
            }

            ShortLink shortLink = shortLinkRepository.findByShortCode(shortCode);
            if (shortLink == null) {
                log.info("Short link not found in DB: shortCode={}", shortCode);
                return null;
            }

            if (shortLink.getExpireTime() != null
                    && shortLink.getExpireTime().isBefore(LocalDateTime.now())) {
                log.info("Short link expired: shortCode={}, expireTime={}", shortCode, shortLink.getExpireTime());
                return null;
            }

            originUrl = shortLink.getOriginUrl();
            long ttlSeconds = calculateTtl(shortLink.getExpireTime());
            linkCacheManager.putShortLink(shortCode, originUrl, ttlSeconds);
            log.info("Cache loaded from DB: shortCode={}", shortCode);
            return originUrl;
        } finally {
            linkCacheManager.unlock(shortCode);
        }
    }

    /**
     * 生成唯一的短链码，最多重试 MAX_GENERATE_RETRIES 次
     */
    private String generateUniqueShortCode() {
        for (int i = 0; i < MAX_GENERATE_RETRIES; i++) {
            String shortCode = shortCodeGenerator.generate();
            if (!linkCacheManager.mightContain(shortCode)) {
                return shortCode;
            }
            log.warn("Short code collision detected (retry {}/{}): code={}", i + 1, MAX_GENERATE_RETRIES, shortCode);
        }
        throw new RuntimeException("Failed to generate unique short code after " + MAX_GENERATE_RETRIES + " retries");
    }

    /**
     * 计算缓存 TTL（秒）
     * 如果短链有明确过期时间，则以过期时间为准；否则使用默认7天
     */
    private long calculateTtl(LocalDateTime expireTime) {
        if (expireTime == null) {
            return TimeUnit.DAYS.toSeconds(7);
        }
        long seconds = java.time.Duration.between(LocalDateTime.now(), expireTime).getSeconds();
        return Math.max(seconds, 60);
    }
}