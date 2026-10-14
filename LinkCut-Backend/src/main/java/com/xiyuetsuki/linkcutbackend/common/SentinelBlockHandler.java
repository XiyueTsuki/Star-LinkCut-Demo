package com.xiyuetsuki.linkcutbackend.common;

import com.alibaba.csp.sentinel.slots.block.BlockException;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeException;
import com.alibaba.csp.sentinel.slots.block.flow.FlowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Sentinel BlockException 全局异常处理
 * <p>
 * 当请求被 Sentinel 限流（FlowException）或熔断（DegradeException）时，
 * 由本处理器统一返回标准 JSON 错误响应，避免抛出原始 BlockException 给客户端。
 * <p>
 * 限流返回 HTTP 429，熔断返回 HTTP 503。
 */
@RestControllerAdvice
public class SentinelBlockHandler {

    private static final Logger log = LoggerFactory.getLogger(SentinelBlockHandler.class);

    /**
     * 限流异常处理 —— 返回 429 Too Many Requests
     *
     * @param e FlowException（含 ParamFlowException，两者均继承 FlowException）
     */
    @ExceptionHandler(FlowException.class)
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    public Result<Void> handleFlowException(FlowException e) {
        log.warn("Sentinel flow limited: rule={}, resource={}", e.getRule().getResource(), e.getRuleLimitApp());
        return Result.error(429, "请求过于频繁，请稍后再试");
    }

    /**
     * 熔断降级异常处理 —— 返回 503 Service Unavailable
     *
     * @param e DegradeException
     */
    @ExceptionHandler(DegradeException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Result<Void> handleDegradeException(DegradeException e) {
        log.warn("Sentinel circuit breaker open: rule={}, resource={}", e.getRule().getResource(), e.getRuleLimitApp());
        return Result.error(503, "服务暂时不可用，请稍后再试");
    }

    /**
     * 兜底处理所有 BlockException 子类
     */
    @ExceptionHandler(BlockException.class)
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    public Result<Void> handleBlockException(BlockException e) {
        log.warn("Sentinel blocked: type={}, rule={}", e.getClass().getSimpleName(), e.getRule().getResource());
        return Result.error(429, "请求被限流，请稍后再试");
    }
}