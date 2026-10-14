package com.xiyuetsuki.linkcutbackend.model.response;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "短链详情")
public class LinkDetailResponse {

    @Schema(description = "短链码", example = "abc123")
    private String shortCode;

    @Schema(description = "完整短链接", example = "http://localhost:8080/abc123")
    private String shortUrl;

    @Schema(description = "原始URL", example = "https://www.example.com/very/long/url")
    private String originUrl;

    @Schema(description = "创建时间")
    private LocalDateTime createTime;

    @Schema(description = "过期时间，null表示永不过期")
    private LocalDateTime expireTime;

    @Schema(description = "累计访问次数", example = "1523")
    private Long accessCount;

    @Schema(description = "状态：1=有效，0=已删除", example = "1")
    private Integer status;
}