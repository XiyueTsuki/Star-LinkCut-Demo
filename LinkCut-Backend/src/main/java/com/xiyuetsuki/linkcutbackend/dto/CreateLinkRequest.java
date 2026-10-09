package com.xiyuetsuki.linkcutbackend.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

/**
 * 创建短链请求 DTO
 */
@Data
public class CreateLinkRequest {

    @NotBlank(message = "原始URL不能为空")
    @Pattern(regexp = "^https?://.*", message = "URL必须以http://或https://开头")
    private String originUrl;

    private Long expireSeconds;
}