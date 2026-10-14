package com.xiyuetsuki.linkcutbackend.model.response;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "短链访问统计")
public class LinkAccessStatsResponse {

    @Schema(description = "短链码", example = "abc123")
    private String shortCode;

    @Schema(description = "累计总PV", example = "5280")
    private Long totalPV;

    @Schema(description = "今日PV", example = "347")
    private Long todayPV;

    @Schema(description = "近7日PV", example = "2100")
    private Long weekPV;

    @Schema(description = "Top10来源Referer")
    private List<RefererItem> topReferers;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @Schema(description = "Referer统计项")
    public static class RefererItem {

        @Schema(description = "来源URL", example = "https://twitter.com")
        private String referer;

        @Schema(description = "访问次数", example = "1200")
        private Long count;
    }
}