package com.xiyuetsuki.linkcutbackend.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 创建短链响应 DTO
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateLinkResponse {

    private String shortCode;
    private String shortUrl;
    private String originUrl;
    private LocalDateTime expireTime;
}