package com.xiyuetsuki.linkcutbackend.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@Schema(description = "缓存统计")
public class CacheStatsResponse {

    @Schema(description = "Caffeine 命中次数")
    private long hitCount;

    @Schema(description = "Caffeine 未命中次数")
    private long missCount;

    @Schema(description = "Caffeine 命中率")
    private double hitRate;

    @Schema(description = "Caffeine 淘汰次数")
    private long evictionCount;

    @Schema(description = "Caffeine 当前条目数")
    private long estimatedSize;

    @Schema(description = "布隆过滤器当前条目数（近似）")
    private long bloomFilterCount;
}