package com.xiyuetsuki.linkcutbackend.filter;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.RateLimiter;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Guava RateLimiter 限流过滤器
 * <p>
 * 对创建短链接口和重定向接口分别进行全局限流和IP限流。
 * IP 级别限流器使用 Caffeine 缓存存储，过期自动淘汰，避免内存泄漏。
 */
@Component
@Order(1)
public class RateLimitFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);

    private final RateLimiter createGlobalLimiter;
    private final RateLimiter redirectGlobalLimiter;
    private final Cache<String, RateLimiter> createIpLimiters;
    private final Cache<String, RateLimiter> redirectIpLimiters;

    private final double createApiIpQps;
    private final double redirectApiIpQps;

    public RateLimitFilter(
            @Value("${linkcut.rate-limit.create-api-qps}") double createApiQps,
            @Value("${linkcut.rate-limit.create-api-ip-qps}") double createApiIpQps,
            @Value("${linkcut.rate-limit.redirect-api-qps}") double redirectApiQps,
            @Value("${linkcut.rate-limit.redirect-api-ip-qps}") double redirectApiIpQps) {
        this.createGlobalLimiter = RateLimiter.create(createApiQps);
        this.redirectGlobalLimiter = RateLimiter.create(redirectApiQps);
        this.createApiIpQps = createApiIpQps;
        this.redirectApiIpQps = redirectApiIpQps;
        this.createIpLimiters = Caffeine.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .maximumSize(10000)
                .build();
        this.redirectIpLimiters = Caffeine.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .maximumSize(50000)
                .build();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        String path = httpRequest.getRequestURI();
        String clientIp = getClientIp(httpRequest);

        if (path.startsWith("/docs.html") || path.startsWith("/v3/api-docs")
                || path.startsWith("/swagger-ui") || path.startsWith("/swagger-resources")
                || path.startsWith("/webjars") || path.startsWith("/favicon.ico")) {
            chain.doFilter(request, response);
            return;
        }

        if (path.equals("/api/link/create") && "POST".equalsIgnoreCase(httpRequest.getMethod())) {
            if (!createGlobalLimiter.tryAcquire()) {
                log.warn("Global rate limit exceeded for create API: ip={}", clientIp);
                httpResponse.setStatus(429);
                httpResponse.setContentType("application/json;charset=UTF-8");
                httpResponse.getWriter().write("{\"code\":429,\"message\":\"请求过于频繁，请稍后再试\"}");
                return;
            }

            RateLimiter ipLimiter = createIpLimiters.get(clientIp,
                    k -> RateLimiter.create(createApiIpQps));
            if (!ipLimiter.tryAcquire()) {
                log.warn("IP rate limit exceeded for create API: ip={}", clientIp);
                httpResponse.setStatus(429);
                httpResponse.setContentType("application/json;charset=UTF-8");
                httpResponse.getWriter().write("{\"code\":429,\"message\":\"IP请求过于频繁，请稍后再试\"}");
                return;
            }
        }

        if (!path.equals("/api/link/create") && path.length() <= 12) {
            if (!redirectGlobalLimiter.tryAcquire()) {
                log.warn("Global rate limit exceeded for redirect API: ip={}", clientIp);
                httpResponse.setStatus(429);
                httpResponse.setContentType("application/json;charset=UTF-8");
                httpResponse.getWriter().write("{\"code\":429,\"message\":\"请求过于频繁，请稍后再试\"}");
                return;
            }

            RateLimiter ipLimiter = redirectIpLimiters.get(clientIp,
                    k -> RateLimiter.create(redirectApiIpQps));
            if (!ipLimiter.tryAcquire()) {
                log.warn("IP rate limit exceeded for redirect API: ip={}", clientIp);
                httpResponse.setStatus(429);
                httpResponse.setContentType("application/json;charset=UTF-8");
                httpResponse.getWriter().write("{\"code\":429,\"message\":\"IP请求过于频繁，请稍后再试\"}");
                return;
            }
        }

        chain.doFilter(request, response);
    }

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