package com.xiyuetsuki.linkcutbackend.filter;

import com.alibaba.csp.sentinel.Entry;
import com.alibaba.csp.sentinel.EntryType;
import com.alibaba.csp.sentinel.SphU;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Sentinel 限流过滤器
 * <p>
 * 对创建短链接口和重定向接口分别进行全局限流和 IP 限流。
 * 限流规则由 {@link com.xiyuetsuki.linkcutbackend.config.SentinelConfig} 在启动时加载，
 * 通过 {@link SphU#entry(String, EntryType, int, Object...)} 触发校验：
 * <ul>
 *   <li>FlowRule   —— 全局限流（基于资源名称匹配）</li>
 *   <li>ParamFlowRule —— IP 级限流（基于热点参数 clientIp 匹配）</li>
 * </ul>
 * 两种规则叠加校验，任一超限均抛出 BlockException，由
 * {@link com.xiyuetsuki.linkcutbackend.common.SentinelBlockHandler} 统一返回 429。
 * <p>
 * 相比 Guava RateLimiter 方案的优势：
 * <ul>
 *   <li>IP 级限流不再需要 Caffeine 缓存维护 IP→RateLimiter 映射</li>
 *   <li>参数统计窗口自动回收，无内存泄漏风险</li>
 *   <li>规则可通过 Sentinel API 运行时动态调整</li>
 *   <li>与熔断降级统一在 Sentinel 体系内管理</li>
 * </ul>
 */
@Component
@Order(1)
public class RateLimitFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);

    /**
     * Sentinel 资源名称常量
     */
    private static final String RESOURCE_CREATE = "create-api";
    private static final String RESOURCE_REDIRECT = "redirect-api";

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        String path = httpRequest.getRequestURI();
        String clientIp = getClientIp(httpRequest);

        // 白名单路径：Swagger 文档、静态资源，跳过限流
        if (path.startsWith("/docs.html") || path.startsWith("/v3/api-docs")
                || path.startsWith("/swagger-ui") || path.startsWith("/swagger-resources")
                || path.startsWith("/webjars") || path.startsWith("/favicon.ico")) {
            chain.doFilter(request, response);
            return;
        }

        // 创建短链接口：POST /api/link/create
        if (path.equals("/api/link/create") && "POST".equalsIgnoreCase(httpRequest.getMethod())) {
            Entry entry = null;
            try {
                // SphU.entry 传入资源名 + clientIp 作为热点参数
                // Sentinel 会自动叠加校验 FlowRule（全局）和 ParamFlowRule（IP级）
                entry = SphU.entry(RESOURCE_CREATE, EntryType.IN, 1, clientIp);
                chain.doFilter(request, response);
            } catch (BlockException e) {
                log.warn("Sentinel flow limited: resource={}, ip={}, rule={}",
                        RESOURCE_CREATE, clientIp, e.getRule().getResource());
                httpResponse.setStatus(429);
                httpResponse.setContentType("application/json;charset=UTF-8");
                httpResponse.getWriter().write("{\"code\":429,\"message\":\"请求过于频繁，请稍后再试\"}");
            } finally {
                if (entry != null) {
                    entry.exit();
                }
            }
            return;
        }

        // 短链重定向接口：GET /{shortCode}（路径长度 ≤ 12 且非 /api/link/create）
        if (!path.equals("/api/link/create") && path.length() <= 12) {
            Entry entry = null;
            try {
                entry = SphU.entry(RESOURCE_REDIRECT, EntryType.IN, 1, clientIp);
                chain.doFilter(request, response);
            } catch (BlockException e) {
                log.warn("Sentinel flow limited: resource={}, ip={}, rule={}",
                        RESOURCE_REDIRECT, clientIp, e.getRule().getResource());
                httpResponse.setStatus(429);
                httpResponse.setContentType("application/json;charset=UTF-8");
                httpResponse.getWriter().write("{\"code\":429,\"message\":\"请求过于频繁，请稍后再试\"}");
            } finally {
                if (entry != null) {
                    entry.exit();
                }
            }
            return;
        }

        // 其他接口（统计/详情/删除/延期/缓存）不走限流
        chain.doFilter(request, response);
    }

    /**
     * 获取客户端真实 IP（支持反向代理）
     */
    private String getClientIp(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.isBlank() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("X-Real-IP");
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