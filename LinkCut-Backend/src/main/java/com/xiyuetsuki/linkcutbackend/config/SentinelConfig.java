package com.xiyuetsuki.linkcutbackend.config;

import com.alibaba.csp.sentinel.annotation.aspectj.SentinelResourceAspect;
import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRule;
import com.alibaba.csp.sentinel.slots.block.degrade.DegradeRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRuleManager;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Sentinel 限流熔断配置
 * <p>
 * 初始化时加载限流规则（FlowRule + ParamFlowRule）和熔断规则（DegradeRule）。
 * 启用 {@link SentinelResourceAspect} 以支持 @SentinelResource 注解的 AOP 拦截。
 * 不使用 Dashboard，规则由代码定义，可通过 Sentinel API 运行时动态调整。
 */
@Configuration
public class SentinelConfig {

    private static final Logger log = LoggerFactory.getLogger(SentinelConfig.class);

    // ==================== 限流参数（从配置文件读取） ====================

    @Value("${linkcut.rate-limit.create-api-qps}")
    private double createApiQps;

    @Value("${linkcut.rate-limit.create-api-ip-qps}")
    private double createApiIpQps;

    @Value("${linkcut.rate-limit.redirect-api-qps}")
    private double redirectApiQps;

    @Value("${linkcut.rate-limit.redirect-api-ip-qps}")
    private double redirectApiIpQps;

    // ==================== 熔断参数 ====================

    @Value("${linkcut.sentinel.degrade.mq-exception-ratio:0.3}")
    private double mqExceptionRatio;

    @Value("${linkcut.sentinel.degrade.mq-time-window:10}")
    private int mqTimeWindow;

    @Value("${linkcut.sentinel.degrade.db-slow-ratio:0.5}")
    private double dbSlowRatio;

    @Value("${linkcut.sentinel.degrade.db-max-rt:200}")
    private int dbMaxRt;

    @Value("${linkcut.sentinel.degrade.db-time-window:5}")
    private int dbTimeWindow;

    @Value("${linkcut.sentinel.degrade.create-exception-ratio:0.2}")
    private double createExceptionRatio;

    @Value("${linkcut.sentinel.degrade.create-time-window:10}")
    private int createTimeWindow;

    /**
     * 注册 SentinelResourceAspect 切面，使 @SentinelResource 注解生效
     */
    @Bean
    public SentinelResourceAspect sentinelResourceAspect() {
        return new SentinelResourceAspect();
    }

    /**
     * 应用启动后加载所有限流和熔断规则
     */
    @PostConstruct
    public void initRules() {
        loadFlowRules();
        loadDegradeRules();
        log.info("Sentinel rules loaded: flow={}, degrade={}",
                FlowRuleManager.getRules().size(),
                DegradeRuleManager.getRules().size());
    }

    /**
     * 加载限流规则（全局 QPS + IP 级 QPS）
     * <p>
     * 每个接口绑定两条规则：
     * <ul>
     *   <li>FlowRule  —— 全局限流（所有请求共享同一个 QPS 阈值）</li>
     *   <li>ParamFlowRule —— IP 级限流（以 IP 作为热点参数，每个 IP 独立计数）</li>
     * </ul>
     */
    private void loadFlowRules() {
        List<FlowRule> flowRules = new ArrayList<>();
        List<ParamFlowRule> paramRules = new ArrayList<>();

        // ---------- 创建短链接口 ----------
        // 全局限流
        FlowRule createFlowRule = new FlowRule("create-api");
        createFlowRule.setGrade(RuleConstant.FLOW_GRADE_QPS);
        createFlowRule.setCount(createApiQps);
        createFlowRule.setControlBehavior(RuleConstant.CONTROL_BEHAVIOR_DEFAULT);
        flowRules.add(createFlowRule);

        // IP 级限流（热点参数流控，paramIdx=0 即 clientIp）
        ParamFlowRule createIpRule = new ParamFlowRule("create-api");
        createIpRule.setParamIdx(0);
        createIpRule.setGrade(RuleConstant.FLOW_GRADE_QPS);
        createIpRule.setCount(createApiIpQps);
        createIpRule.setControlBehavior(RuleConstant.CONTROL_BEHAVIOR_DEFAULT);
        paramRules.add(createIpRule);

        // ---------- 短链重定向接口 ----------
        // 全局限流
        FlowRule redirectFlowRule = new FlowRule("redirect-api");
        redirectFlowRule.setGrade(RuleConstant.FLOW_GRADE_QPS);
        redirectFlowRule.setCount(redirectApiQps);
        redirectFlowRule.setControlBehavior(RuleConstant.CONTROL_BEHAVIOR_DEFAULT);
        flowRules.add(redirectFlowRule);

        // IP 级限流
        ParamFlowRule redirectIpRule = new ParamFlowRule("redirect-api");
        redirectIpRule.setParamIdx(0);
        redirectIpRule.setGrade(RuleConstant.FLOW_GRADE_QPS);
        redirectIpRule.setCount(redirectApiIpQps);
        redirectIpRule.setControlBehavior(RuleConstant.CONTROL_BEHAVIOR_DEFAULT);
        paramRules.add(redirectIpRule);

        FlowRuleManager.loadRules(flowRules);
        ParamFlowRuleManager.loadRules(paramRules);

        log.info("Flow rules loaded: createApiQps={}, createApiIpQps={}, redirectApiQps={}, redirectApiIpQps={}",
                createApiQps, createApiIpQps, redirectApiQps, redirectApiIpQps);
    }

    /**
     * 加载熔断降级规则
     * <p>
     * 熔断策略说明：
     * <ul>
     *   <li>mq-send-access-log    —— 异常比例熔断：RocketMQ 发送失败率过高时熔断，直接走降级队列</li>
     *   <li>db-query-short-link   —— 慢调用比例熔断：DB 查询 RT 过高时熔断，直接返回 null</li>
     *   <li>db-create-short-link  —— 异常比例熔断：DB 写入失败率过高时熔断，返回错误提示</li>
     * </ul>
     */
    private void loadDegradeRules() {
        List<DegradeRule> rules = new ArrayList<>();

        // ---------- RocketMQ Producer 熔断 ----------
        DegradeRule mqDegradeRule = new DegradeRule("mq-send-access-log");
        mqDegradeRule.setGrade(RuleConstant.DEGRADE_GRADE_EXCEPTION_RATIO);
        mqDegradeRule.setCount(mqExceptionRatio);
        mqDegradeRule.setTimeWindow(mqTimeWindow);
        mqDegradeRule.setMinRequestAmount(20);
        mqDegradeRule.setStatIntervalMs(10000);
        rules.add(mqDegradeRule);

        // ---------- DB 查询熔断（慢调用比例） ----------
        DegradeRule dbQueryDegradeRule = new DegradeRule("db-query-short-link");
        dbQueryDegradeRule.setGrade(RuleConstant.DEGRADE_GRADE_RT);
        dbQueryDegradeRule.setCount(dbMaxRt);
        dbQueryDegradeRule.setTimeWindow(dbTimeWindow);
        dbQueryDegradeRule.setSlowRatioThreshold(dbSlowRatio);
        dbQueryDegradeRule.setMinRequestAmount(10);
        dbQueryDegradeRule.setStatIntervalMs(5000);
        rules.add(dbQueryDegradeRule);

        // ---------- DB 写入熔断（异常比例） ----------
        DegradeRule dbCreateDegradeRule = new DegradeRule("db-create-short-link");
        dbCreateDegradeRule.setGrade(RuleConstant.DEGRADE_GRADE_EXCEPTION_RATIO);
        dbCreateDegradeRule.setCount(createExceptionRatio);
        dbCreateDegradeRule.setTimeWindow(createTimeWindow);
        dbCreateDegradeRule.setMinRequestAmount(10);
        dbCreateDegradeRule.setStatIntervalMs(10000);
        rules.add(dbCreateDegradeRule);

        DegradeRuleManager.loadRules(rules);

        log.info("Degrade rules loaded: mqExceptionRatio={}, dbSlowRatio={}, dbMaxRt={}ms, createExceptionRatio={}",
                mqExceptionRatio, dbSlowRatio, dbMaxRt, createExceptionRatio);
    }
}