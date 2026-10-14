package com.xiyuetsuki.linkcutbackend.service.impl;

import com.alibaba.csp.sentinel.annotation.SentinelResource;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.xiyuetsuki.linkcutbackend.cache.LinkCacheManager;
import com.xiyuetsuki.linkcutbackend.dto.CreateLinkRequest;
import com.xiyuetsuki.linkcutbackend.dto.CreateLinkResponse;
import com.xiyuetsuki.linkcutbackend.dto.ExpireExtendResponse;
import com.xiyuetsuki.linkcutbackend.dto.LinkInfoResponse;
import com.xiyuetsuki.linkcutbackend.dto.LinkStatsResponse;
import com.xiyuetsuki.linkcutbackend.entity.ShortCodeSnapshot;
import com.xiyuetsuki.linkcutbackend.entity.ShortLink;
import com.xiyuetsuki.linkcutbackend.generator.ShortCodeGenerator;
import com.xiyuetsuki.linkcutbackend.repository.AccessLogRepository;
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
 * <p>
 * Sentinel 熔断保护：
 * <ul>
 *   <li>{@code db-create-short-link}  — DB 写入异常比例过高时熔断，返回错误提示</li>
 *   <li>{@code db-query-short-link}   — DB 查询慢调用比例过高时熔断，返回 null</li>
 * </ul>
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
    private final AccessLogRepository accessLogRepository;
    private final LinkCacheManager linkCacheManager;

    public LinkCutServiceImpl(ShortCodeGenerator shortCodeGenerator,
                              ShortLinkRepository shortLinkRepository,
                              ShortCodeSnapshotRepository shortCodeSnapshotRepository,
                              AccessLogRepository accessLogRepository,
                              LinkCacheManager linkCacheManager) {
        this.shortCodeGenerator = shortCodeGenerator;
        this.shortLinkRepository = shortLinkRepository;
        this.shortCodeSnapshotRepository = shortCodeSnapshotRepository;
        this.accessLogRepository = accessLogRepository;
        this.linkCacheManager = linkCacheManager;
    }

    /**
     * 创建短链
     * <p>
     * Sentinel 熔断保护：当 DB 写入异常比例超过阈值时，
     * 熔断器打开，由 {@link #createShortLinkBlockHandler} 返回错误响应。
     */
    @Override
    @SentinelResource(value = "db-create-short-link",
            blockHandler = "createShortLinkBlockHandler")
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

    /**
     * Sentinel 熔断 blockHandler：DB 写入熔断时返回错误提示
     * <p>
     * 当 db-create-short-link 资源的熔断器打开时，
     * 直接返回包含错误信息的 CreateLinkResponse，不执行 DB 操作。
     */
    public CreateLinkResponse createShortLinkBlockHandler(CreateLinkRequest request,
                                                           String creatorIp,
                                                           BlockException e) {
        log.warn("DB write circuit breaker open, returning error: creatorIp={}", creatorIp);
        return CreateLinkResponse.builder()
                .shortCode(null)
                .shortUrl(null)
                .originUrl(request.getOriginUrl())
                .expireTime(null)
                .build();
    }

    @Override
    public String getOriginUrl(String shortCode) {
        // L1/L2 缓存命中直接返回，不触发 Sentinel 统计
        String originUrl = linkCacheManager.getOriginUrl(shortCode);
        if (originUrl != null) {
            log.debug("Cache hit (L1/L2): shortCode={}", shortCode);
            return originUrl;
        }

        // 布隆过滤器判定不存在，直接返回 null
        if (!linkCacheManager.mightContain(shortCode)) {
            log.debug("BloomFilter miss: shortCode={}", shortCode);
            return null;
        }

        // 缓存未命中，走 DB 查询（受 Sentinel 熔断保护）
        return queryDbForShortLink(shortCode);
    }

    /**
     * DB 查询短链（带分布式锁 + Sentinel 熔断保护）
     * <p>
     * 当 {@code db-query-short-link} 资源的慢调用比例超过阈值时，
     * Sentinel 抛出 DegradeException，由 {@link #queryDbForShortLinkBlockHandler} 接管，
     * 直接返回 null（即 404），避免 DB 被持续压垮。
     * <p>
     * 此方法从 {@link #getOriginUrl(String)} 中提取，确保缓存命中的请求
     * 不受熔断影响，只有真正穿透到 DB 的请求才参与熔断统计。
     */
    @SentinelResource(value = "db-query-short-link",
            blockHandler = "queryDbForShortLinkBlockHandler")
    public String queryDbForShortLink(String shortCode) {
        log.debug("Cache miss, query DB with distributed lock: shortCode={}", shortCode);
        if (!linkCacheManager.tryLock(shortCode)) {
            log.warn("Failed to acquire distributed lock: shortCode={}", shortCode);
            String originUrl = linkCacheManager.getOriginUrl(shortCode);
            if (originUrl != null) {
                return originUrl;
            }
            return null;
        }

        try {
            String cachedUrl = linkCacheManager.getOriginUrl(shortCode);
            if (cachedUrl != null) {
                return cachedUrl;
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

            String originUrl = shortLink.getOriginUrl();
            long ttlSeconds = calculateTtl(shortLink.getExpireTime());
            linkCacheManager.putShortLink(shortCode, originUrl, ttlSeconds);
            log.info("Cache loaded from DB: shortCode={}", shortCode);
            return originUrl;
        } finally {
            linkCacheManager.unlock(shortCode);
        }
    }

    /**
     * Sentinel 熔断 blockHandler：DB 查询熔断时返回 null
     * <p>
     * 当 db-query-short-link 熔断器打开时，直接返回 null，
     * 上游 controller 将返回 404 给客户端。
     */
    public String queryDbForShortLinkBlockHandler(String shortCode, BlockException e) {
        log.warn("DB query circuit breaker open, returning null: shortCode={}", shortCode);
        return null;
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

    @Override
    public LinkInfoResponse getLinkInfo(String shortCode) {
        if (!linkCacheManager.mightContain(shortCode)) {
            return null;
        }
        ShortLink shortLink = shortLinkRepository.findByShortCode(shortCode);
        if (shortLink == null) {
            return null;
        }
        return LinkInfoResponse.builder()
                .shortCode(shortLink.getShortCode())
                .shortUrl(domain + "/" + shortLink.getShortCode())
                .originUrl(shortLink.getOriginUrl())
                .createTime(shortLink.getCreateTime())
                .expireTime(shortLink.getExpireTime())
                .accessCount(shortLink.getAccessCount())
                .status(shortLink.getStatus())
                .build();
    }

    @Override
    public LinkStatsResponse getLinkStats(String shortCode) {
        if (!linkCacheManager.mightContain(shortCode)) {
            return null;
        }

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime todayStart = now.toLocalDate().atStartOfDay();
        LocalDateTime weekStart = now.minusDays(7);

        Long totalPV = accessLogRepository.countTotalByShortCode(shortCode);
        Long todayPV = accessLogRepository.countByShortCodeSince(shortCode, todayStart);
        Long weekPV = accessLogRepository.countByShortCodeSince(shortCode, weekStart);

        java.util.List<LinkStatsResponse.RefererItem> topReferers =
                accessLogRepository.topReferers(shortCode, 10).stream()
                        .map(row -> LinkStatsResponse.RefererItem.builder()
                                .referer((String) row.get("referer"))
                                .count(((Number) row.get("cnt")).longValue())
                                .build())
                        .toList();

        return LinkStatsResponse.builder()
                .shortCode(shortCode)
                .totalPV(totalPV)
                .todayPV(todayPV)
                .weekPV(weekPV)
                .topReferers(topReferers)
                .build();
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean deleteShortLink(String shortCode) {
        ShortLink shortLink = shortLinkRepository.findByShortCode(shortCode);
        if (shortLink == null) {
            return false;
        }

        shortLinkRepository.deleteById(shortLink.getId());

        linkCacheManager.invalidate(shortCode);

        log.info("Short link deleted: shortCode={}", shortCode);
        return true;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public ExpireExtendResponse extendExpireTime(String shortCode, Long extendSeconds) {
        ShortLink shortLink = shortLinkRepository.findByShortCode(shortCode);
        if (shortLink == null) {
            return null;
        }

        LocalDateTime baseTime = shortLink.getExpireTime();
        if (baseTime == null || baseTime.isBefore(LocalDateTime.now())) {
            baseTime = LocalDateTime.now();
        }
        LocalDateTime newExpireTime = baseTime.plusSeconds(extendSeconds);

        shortLink.setExpireTime(newExpireTime);
        shortLinkRepository.updateById(shortLink);

        long ttlSeconds = calculateTtl(newExpireTime);
        linkCacheManager.putShortLink(shortCode, shortLink.getOriginUrl(), ttlSeconds);

        log.info("Expire time extended: shortCode={}, newExpireTime={}", shortCode, newExpireTime);
        return ExpireExtendResponse.builder()
                .shortCode(shortCode)
                .newExpireTime(newExpireTime)
                .build();
    }
}