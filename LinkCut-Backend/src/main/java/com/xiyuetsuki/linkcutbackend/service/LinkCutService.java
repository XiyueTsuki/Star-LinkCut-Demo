package com.xiyuetsuki.linkcutbackend.service;

import com.xiyuetsuki.linkcutbackend.model.request.CreateLinkRequest;
import com.xiyuetsuki.linkcutbackend.model.response.CreateLinkResponse;
import com.xiyuetsuki.linkcutbackend.model.response.ExtendExpireResponse;
import com.xiyuetsuki.linkcutbackend.model.response.LinkAccessStatsResponse;
import com.xiyuetsuki.linkcutbackend.model.response.LinkDetailResponse;

/**
 * 短链服务接口
 */
public interface LinkCutService {

    /**
     * 创建短链
     */
    CreateLinkResponse createShortLink(CreateLinkRequest request, String creatorIp);

    /**
     * 根据短链码获取原始URL
     */
    String getOriginUrl(String shortCode);

    /**
     * 查询短链详情
     */
    LinkDetailResponse getLinkInfo(String shortCode);

    /**
     * 查询短链访问统计
     */
    LinkAccessStatsResponse getLinkStats(String shortCode);

    /**
     * 删除短链（逻辑删除）
     *
     * @param shortCode 短链码
     * @return true=删除成功，false=短链不存在或已删除
     */
    boolean deleteShortLink(String shortCode);

    /**
     * 延期短链过期时间
     *
     * @param shortCode     短链码
     * @param extendSeconds 延期秒数
     * @return 延期后的过期时间，短链不存在返回 null
     */
    ExtendExpireResponse extendExpireTime(String shortCode, Long extendSeconds);
}