package com.xiyuetsuki.linkcutbackend.service;

import com.xiyuetsuki.linkcutbackend.dto.CreateLinkRequest;
import com.xiyuetsuki.linkcutbackend.dto.CreateLinkResponse;
import com.xiyuetsuki.linkcutbackend.dto.ExpireExtendResponse;
import com.xiyuetsuki.linkcutbackend.dto.LinkInfoResponse;
import com.xiyuetsuki.linkcutbackend.dto.LinkStatsResponse;

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
    LinkInfoResponse getLinkInfo(String shortCode);

    /**
     * 查询短链访问统计
     */
    LinkStatsResponse getLinkStats(String shortCode);

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
    ExpireExtendResponse extendExpireTime(String shortCode, Long extendSeconds);
}