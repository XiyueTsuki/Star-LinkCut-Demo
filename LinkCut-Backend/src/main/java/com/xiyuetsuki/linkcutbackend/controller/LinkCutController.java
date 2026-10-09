package com.xiyuetsuki.linkcutbackend.controller;

import com.xiyuetsuki.linkcutbackend.common.Result;
import com.xiyuetsuki.linkcutbackend.dto.CreateLinkRequest;
import com.xiyuetsuki.linkcutbackend.dto.CreateLinkResponse;
import com.xiyuetsuki.linkcutbackend.mq.producer.AccessLogProducer;
import com.xiyuetsuki.linkcutbackend.service.LinkCutService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

/**
 * 短链 REST 控制器
 */
@RestController
public class LinkCutController {

    private static final Logger log = LoggerFactory.getLogger(LinkCutController.class);

    private final LinkCutService linkCutService;
    private final AccessLogProducer accessLogProducer;

    public LinkCutController(LinkCutService linkCutService, AccessLogProducer accessLogProducer) {
        this.linkCutService = linkCutService;
        this.accessLogProducer = accessLogProducer;
    }

    /**
     * 创建短链
     * POST /api/link/create
     */
    @PostMapping("/api/link/create")
    public Result<CreateLinkResponse> createShortLink(@Valid @RequestBody CreateLinkRequest request,
                                                       HttpServletRequest httpRequest) {
        String creatorIp = getClientIp(httpRequest);
        CreateLinkResponse response = linkCutService.createShortLink(request, creatorIp);
        return Result.success(response);
    }

    /**
     * 短链重定向
     * GET /{shortCode}
     */
    @GetMapping("/{shortCode}")
    public void redirect(@PathVariable String shortCode,
                         HttpServletRequest request,
                         HttpServletResponse response) throws IOException {
        String originUrl = linkCutService.getOriginUrl(shortCode);
        if (originUrl == null) {
            response.sendError(HttpStatus.NOT_FOUND.value(), "短链不存在或已过期");
            return;
        }
        String clientIp = getClientIp(request);
        String userAgent = request.getHeader("User-Agent");
        String referer = request.getHeader("Referer");
        accessLogProducer.sendAccessLog(shortCode, clientIp, userAgent, referer);
        response.sendRedirect(originUrl);
    }

    /**
     * 获取客户端真实IP
     */
    private String getClientIp(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.isBlank() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("X-Real-IP");
        }
        if (ip == null || ip.isBlank() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.isBlank() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.isBlank() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        if (ip != null && ip.contains(",")) {
            ip = ip.split(",")[0].trim();
        }
        return ip;
    }
}