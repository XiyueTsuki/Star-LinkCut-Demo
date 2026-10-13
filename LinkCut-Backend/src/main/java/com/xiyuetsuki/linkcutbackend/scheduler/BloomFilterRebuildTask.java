package com.xiyuetsuki.linkcutbackend.scheduler;

import com.xiyuetsuki.linkcutbackend.repository.ShortLinkRepository;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class BloomFilterRebuildTask {

    private static final Logger log = LoggerFactory.getLogger(BloomFilterRebuildTask.class);

    private final RedissonClient redissonClient;
    private final ShortLinkRepository shortLinkRepository;

    @Value("${linkcut.bloom-filter.name}")
    private String bloomFilterName;

    @Value("${linkcut.bloom-filter.expected-insertions}")
    private long expectedInsertions;

    @Value("${linkcut.bloom-filter.false-probability}")
    private double falseProbability;

    public BloomFilterRebuildTask(RedissonClient redissonClient,
                                  ShortLinkRepository shortLinkRepository) {
        this.redissonClient = redissonClient;
        this.shortLinkRepository = shortLinkRepository;
    }

    @Scheduled(cron = "${linkcut.bloom-filter.rebuild-cron:0 0 3 * * ?}")
    public void rebuild() {
        log.info("BloomFilter rebuild started...");
        long start = System.currentTimeMillis();

        RBloomFilter<String> newFilter = null;
        String tempName = bloomFilterName + "_rebuild";

        try {
            newFilter = redissonClient.getBloomFilter(tempName);
            newFilter.delete();
            if (!newFilter.tryInit(expectedInsertions, falseProbability)) {
                log.error("BloomFilter rebuild failed: tryInit returned false");
                return;
            }

            List<String> shortCodes = shortLinkRepository.findAllActiveShortCodes();
            if (shortCodes != null) {
                for (String code : shortCodes) {
                    newFilter.add(code);
                }
            }

            redissonClient.getKeys().rename(tempName, bloomFilterName);

            long elapsed = System.currentTimeMillis() - start;
            log.info("BloomFilter rebuilt: loaded {} active codes in {}ms",
                    shortCodes != null ? shortCodes.size() : 0, elapsed);
        } catch (Exception e) {
            log.error("BloomFilter rebuild failed", e);
            if (newFilter != null && newFilter.isExists()) {
                newFilter.delete();
            }
        }
    }
}