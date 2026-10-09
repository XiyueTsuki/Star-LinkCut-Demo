package com.xiyuetsuki.linkcutbackend.config;

import com.xiyuetsuki.linkcutbackend.repository.ShortLinkRepository;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 布隆过滤器配置
 * <p>
 * 使用 Redisson 3.43 RBloomFilter，支持分布式环境。
 * 服务启动时从数据库加载已有的短链码初始化过滤器。
 */
@Configuration
public class BloomFilterConfig {

    private static final Logger log = LoggerFactory.getLogger(BloomFilterConfig.class);

    @Value("${linkcut.bloom-filter.expected-insertions}")
    private long expectedInsertions;

    @Value("${linkcut.bloom-filter.false-probability}")
    private double falseProbability;

    @Value("${linkcut.bloom-filter.name}")
    private String bloomFilterName;

    @Bean
    public RBloomFilter<String> bloomFilter(RedissonClient redissonClient) {
        RBloomFilter<String> bloomFilter = redissonClient.getBloomFilter(bloomFilterName);
        if (!bloomFilter.isExists()) {
            bloomFilter.tryInit(expectedInsertions, falseProbability);
            log.info("BloomFilter [{}] initialized: expectedInsertions={}, falseProbability={}",
                    bloomFilterName, expectedInsertions, falseProbability);
        }
        return bloomFilter;
    }

    /**
     * 服务启动完成后，从数据库加载已有的短链码到布隆过滤器。
     * 如果 BloomFilter 已经存在（Redis 中有数据），则跳过重建。
     */
    @Bean
    public ApplicationRunner bloomFilterInitializer(RBloomFilter<String> bloomFilter,
                                                     ShortLinkRepository shortLinkRepository) {
        return (ApplicationArguments args) -> {
            long currentCount = bloomFilter.count();
            if (currentCount > 0) {
                log.info("BloomFilter [{}] already contains {} entries, skip loading from DB",
                        bloomFilterName, currentCount);
                return;
            }
            log.info("Loading short codes from DB to BloomFilter [{}]...", bloomFilterName);
            java.util.List<String> shortCodes = shortLinkRepository.findAllActiveShortCodes();
            if (shortCodes != null) {
                for (String shortCode : shortCodes) {
                    bloomFilter.add(shortCode);
                }
                log.info("BloomFilter [{}] loaded {} short codes from database", bloomFilterName, shortCodes.size());
            }
        };
    }
}