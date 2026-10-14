package com.xiyuetsuki.linkcutbackend.model.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

/**
 * 创建短链请求
 */
@Data
@Schema(description = "创建短链请求")
public class CreateLinkRequest {

    @NotBlank(message = "原始URL不能为空")
    @Pattern(regexp = "^https?://.*", message = "URL必须以http://或https://开头")
    @Schema(description = "原始URL", example = "https://www.example.com/very/long/url")
    private String originUrl;

    @Schema(description = "过期秒数，null表示永不过期", example = "86400")
    private Long expireSeconds;
}