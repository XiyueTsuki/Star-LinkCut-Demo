package com.xiyuetsuki.linkcutbackend.dto;

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
@Schema(description = "过期时间延期响应")
public class ExpireExtendResponse {

    @Schema(description = "短链码", example = "abc123")
    private String shortCode;

    @Schema(description = "延期后的过期时间，null表示永不过期")
    private LocalDateTime newExpireTime;
}