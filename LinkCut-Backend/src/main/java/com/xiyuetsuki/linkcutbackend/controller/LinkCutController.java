package com.xiyuetsuki.linkcutbackend.controller;

import com.xiyuetsuki.linkcutbackend.common.Result;
import com.xiyuetsuki.linkcutbackend.dto.CreateLinkRequest;
import com.xiyuetsuki.linkcutbackend.dto.CreateLinkResponse;
import com.xiyuetsuki.linkcutbackend.dto.LinkInfoResponse;
import com.xiyuetsuki.linkcutbackend.dto.LinkStatsResponse;
import com.xiyuetsuki.linkcutbackend.mq.producer.AccessLogProducer;
import com.xiyuetsuki.linkcutbackend.service.LinkCutService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "短链管理", description = "短链的创建、跳转、详情与统计接口")
public class LinkCutController {

    private static final Logger log = LoggerFactory.getLogger(LinkCutController.class);

    private final LinkCutService linkCutService;
    private final AccessLogProducer accessLogProducer;

    public LinkCutController(LinkCutService linkCutService, AccessLogProducer accessLogProducer) {
        this.linkCutService = linkCutService;
        this.accessLogProducer = accessLogProducer;
    }

    @Operation(summary = "创建短链", description = "传入原始URL，生成唯一的短链码并返回短链接")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "创建成功"),
            @ApiResponse(responseCode = "429", description = "请求被限流")
    })
    @PostMapping("/api/link/create")
    public Result<CreateLinkResponse> createShortLink(@Valid @RequestBody CreateLinkRequest request,
                                                       HttpServletRequest httpRequest) {
        String creatorIp = getClientIp(httpRequest);
        CreateLinkResponse response = linkCutService.createShortLink(request, creatorIp);
        return Result.success(response);
    }

    @Operation(summary = "短链重定向", description = "访问短链码，302跳转到原始URL。若短链不存在或已过期则返回404")
    @ApiResponses({
            @ApiResponse(responseCode = "302", description = "重定向到原始URL"),
            @ApiResponse(responseCode = "404", description = "短链不存在或已过期"),
            @ApiResponse(responseCode = "429", description = "请求被限流")
    })
    @GetMapping("/{shortCode:[0-9a-zA-Z]{4,15}}")
    public void redirect(
            @Parameter(description = "短链码（4-15位字母数字）", required = true, example = "abc123")
            @PathVariable String shortCode,
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

    @Operation(summary = "查询短链详情", description = "根据短链码查询原始URL、创建时间、过期时间、访问次数等信息")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "查询成功"),
            @ApiResponse(responseCode = "404", description = "短链不存在")
    })
    @GetMapping("/api/link/{shortCode}/info")
    public Result<LinkInfoResponse> getLinkInfo(
            @Parameter(description = "短链码", required = true, example = "abc123")
            @PathVariable String shortCode) {
        LinkInfoResponse response = linkCutService.getLinkInfo(shortCode);
        if (response == null) {
            return Result.error(404, "短链不存在");
        }
        return Result.success(response);
    }

    @Operation(summary = "查询访问统计", description = "查询短链的总PV、今日PV、近7日PV及Top10来源Referer")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "查询成功"),
            @ApiResponse(responseCode = "404", description = "短链不存在")
    })
    @GetMapping("/api/link/{shortCode}/stats")
    public Result<LinkStatsResponse> getLinkStats(
            @Parameter(description = "短链码", required = true, example = "abc123")
            @PathVariable String shortCode) {
        LinkStatsResponse response = linkCutService.getLinkStats(shortCode);
        if (response == null) {
            return Result.error(404, "短链不存在");
        }
        return Result.success(response);
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