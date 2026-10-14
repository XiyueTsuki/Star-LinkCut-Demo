package com.xiyuetsuki.linkcutbackend.model.response;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 创建短链响应
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Schema(description = "创建短链响应")
public class CreateLinkResponse {

    @Schema(description = "短链码", example = "abc123")
    private String shortCode;

    @Schema(description = "完整短链接", example = "http://localhost:8080/abc123")
    private String shortUrl;

    @Schema(description = "原始URL", example = "https://www.example.com/very/long/url")
    private String originUrl;

    @Schema(description = "过期时间，null表示永不过期")
    private LocalDateTime expireTime;
}