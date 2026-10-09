package com.xiyuetsuki.linkcutbackend.service;

import com.xiyuetsuki.linkcutbackend.dto.CreateLinkRequest;
import com.xiyuetsuki.linkcutbackend.dto.CreateLinkResponse;

/**
 * 短链服务接口
 */
public interface LinkCutService {

    /**
     * 创建短链
     *
     * @param request 创建请求
     * @param creatorIp 创建者IP
     * @return 短链信息
     */
    CreateLinkResponse createShortLink(CreateLinkRequest request, String creatorIp);

    /**
     * 根据短链码获取原始URL
     *
     * @param shortCode 短链码
     * @return 原始URL，不存在返回 null
     */
    String getOriginUrl(String shortCode);
}