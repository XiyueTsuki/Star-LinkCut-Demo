# LinkCut - 高并发短链系统

> 面向求职展示的高并发短链服务，系统性解决缓存穿透、击穿、雪崩三大经典问题，具备多实例缓存一致性、消息可靠性保障及全链路可观测性。

## 目录

- [项目简介](#项目简介)
- [技术栈](#技术栈)
- [架构设计](#架构设计)
  - [请求流转全链路](#请求流转全链路)
  - [缓存策略全景](#缓存策略全景)
  - [限流体系](#限流体系)
- [消息可靠性保障](#消息可靠性保障)
- [消息轨迹](#消息轨迹)
- [项目结构](#项目结构)
- [数据库设计](#数据库设计)
- [快速开始](#快速开始)
- [API 接口](#api-接口)
- [环境与配置](#环境与配置)
- [测试](#测试)

---

## 项目简介

LinkCut 是一个高并发短链服务，采用 **Caffeine L1 本地缓存 + Redis L2 分布式缓存 + Redisson 布隆过滤器 + 分布式锁 + RocketMQ 异步日志** 架构，完整覆盖了高并发场景下缓存的经典问题及多实例一致性，同时具备消息降级、死信队列、积压监控等生产级可靠性设计。

| 维度 | 方案 |
|------|------|
| 短码算法 | Snowflake 雪花算法 + Base62 编码（7~8 位） |
| 缓存层 | Caffeine L1 (TTL 5min) + Redis RBucket L2 (TTL 对齐过期时间，±30% 随机偏移) |
| 防穿透 | Redisson RBloomFilter（1kw 容量 / 0.1% 误判率）+ DB `status=1` 兜底 |
| 防击穿 | Redisson RLock 分布式锁（tryLock 3s 等待 / 10s 租约），锁内 double-check + 单线程回源 DB |
| 防雪崩 | `calculateTtl()` 对 Redis TTL 施加 ±30% 随机偏移 |
| 多实例一致性 | Redis Pub/Sub 广播 `linkcut:cache:invalidate`，毫秒级 Caffeine 跨实例失效 |
| 限流 | Guava RateLimiter 全局 + IP 两级限流；IP 级限流器用 Caffeine 存储防泄漏 |
| 异步日志 | RocketMQ 5.3.0 原生客户端（零 Spring 依赖），异步发送 + 三级降级链 + DLQ 自动清理 + 消息轨迹 |
| 访问计数 | RocketMQ 异步增量 → Consumer 批量合并 `access_count + N`，一次 UPDATE 替代 N 次 |
| 接口文档 | SpringDoc OpenAPI → `http://localhost:8080/docs.html` |
| 联调环境 | MySQL 8.4 / Redis 8.2 / RocketMQ 5.3.0 全链路 Docker 部署 |

---

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Spring Boot | 4.1.0 | 基础框架 |
| Java | 21 | 运行环境 |
| MyBatis-Plus | 3.5.16 | ORM 持久化，`@TableLogic` 逻辑删除 |
| MySQL | 8.4 | 短链数据与访问日志存储 |
| Redis | 8.2 | 缓存层（Redisson 3.43） |
| Redisson | 3.43.0 | 分布式锁 + RBloomFilter + RBucket + RTopic(Pub/Sub) |
| RocketMQ | 5.3.0 | 异步访问日志采集（原生 `rocketmq-client`） |
| Guava | 33.4.0 | RateLimiter 限流 |
| Caffeine | 3.2.0 | L1 本地缓存 + IP 限流器存储 |
| SpringDoc | 2.8.5 | OpenAPI 接口文档与在线调试（Swagger UI） |
| Hutool | 5.8.34 | 通用工具库 |
| Docker Compose | 3.3 | 容器编排（MySQL + Redis + RocketMQ） |

---

## 架构设计

### 请求流转全链路

```
                            客户端请求
                               │
              ┌────────────────┼────────────────┐
              ▼                                  ▼
      创建短链 (POST)                      短链跳转 (GET)
              │                                  │
  ┌───────────────────────┐          ┌──────────────────────────┐
  │ Guava RateLimiter      │          │  Caffeine L1 本地缓存     │  ← 0 网络 IO
  │ 全局 + IP 两级限流      │          │  ├─ 命中 → 直接返回 URL    │
  └───────────┬───────────┘          │  └─ 未命中 → 查 Redis L2  │
              │                      └────────────┬─────────────┘
  ┌───────────▼───────────┐                       │ 未命中
  │ Snowflake → Base62    │          ┌────────────▼─────────────┐
  │ 短码生成 + 冲突检测     │          │  Redis L2 分布式缓存      │  ← Redis RBucket
  │ 最多重试 5 次          │          │  ├─ 命中 → 回填 L1 + 返回  │
  └───────────┬───────────┘          │  └─ 未命中 → 布隆过滤器    │
              │                      └────────────┬─────────────┘
  ┌───────────▼───────────┐                       │
  │ MySQL 持久化 (status=1) │          ┌────────────▼─────────────┐
  │ 写 Caffeine L1 + L2   │          │  Redisson RBloomFilter   │  ← 防穿透
  │ 写入布隆过滤器          │          │  ├─ 不含 → 直接返回 404    │
  │ 写 t_short_code_snap  │          │  └─ 含 → 获取分布式锁      │
  └───────────────────────┘          └────────────┬─────────────┘
                                                  │
                                     ┌────────────▼─────────────┐
                                     │  Redisson RLock 分布式锁   │  ← 防击穿
                                     │  锁内 double-check +      │
                                     │  单线程查 DB → 回写 L1+L2  │
                                     └────────────┬─────────────┘
                                                  │
                                     ┌────────────▼─────────────┐
                                     │  MySQL t_short_link       │
                                     │  WHERE short_code = ?     │
                                     │    AND status = 1         │
                                     │    AND expire_time > NOW  │
                                     └──────────────────────────┘

                          ┌────────────────────┐
                          │ RocketMQ Producer   │  ← 异步发送访问日志
                          │ (redirect 成功后)    │
                          └────────┬───────────┘
                                   ▼
                          ┌────────────────────┐
                          │ RocketMQ Consumer   │
                          │ 批量 INSERT IGNORE   │
                          │ 写入 t_access_log    │
                          └────────────────────┘

          ┌──────────────────────────────────────────┐
          │  Redis Pub/Sub (linkcut:cache:invalidate)  │  ← 多实例 Caffeine 一致性
          │  删除/更新时广播 → 所有实例失效本地缓存      │
          └──────────────────────────────────────────┘

          ┌──────────────────────────────────────────┐
          │  布隆过滤器定时重建 (每日 03:00)            │
          │  扫描 DB status=1 → RENAME 原子切换        │
          └──────────────────────────────────────────┘
```

### 缓存策略全景

| 缓存层 | 技术 | 作用 | 关键设计 |
|--------|------|------|---------|
| L1 本地缓存 | Caffeine | 热点数据 0 网络延迟 | TTL 5min（可配），`recordStats` 可观测 |
| L2 分布式缓存 | Redis RBucket | 跨实例共享 | TTL 对齐短链过期时间，±30% 随机偏移防雪崩 |
| 布隆过滤器 | Redisson RBloomFilter | 拦截不存在 key，防穿透 | 1kw 预期容量 / 0.1% 误判率；每日凌晨 3 点定时重建（`_rebuild` 临时 key → `RENAME` 原子切换） |
| 分布式锁 | Redisson RLock | 单线程回源 DB，防击穿 | tryLock 3s 等待 / 10s 租约；DB 查询后回写 L1 + L2 |
| Pub/Sub 广播 | Redisson RTopic | 多实例 Caffeine 一致性 | 删除/更新时发布至 `linkcut:cache:invalidate`，所有实例毫秒级失效本地缓存 |

#### 四大缓存问题的解决

| 问题 | 根因 | 方案 | 实现细节 |
|------|------|------|---------|
| **缓存穿透** | 查询不存在的数据直达 DB | 布隆过滤器 + DB `status=1` 过滤兜底 | 创建短链时同步写入 BF；BF 判断不存在则直接返回 null，不查 DB |
| **缓存击穿** | 热点 key 过期，大量请求涌入 DB | Redisson 分布式锁 | 锁内 double-check，仅一个线程查 DB 并回写 L1+L2；获取锁失败则再次尝试读缓存 |
| **缓存雪崩** | 大量 key 同时过期 | `randomOffset()` 随机偏移 | `putShortLink` 时对 Redis TTL 施加 ±30% 随机值，错开过期时间 |
| **多实例一致性** | Caffeine 本地缓存过期窗口不一致 | Redis Pub/Sub 广播失效 + 布隆过滤器定时重建 | 删除时同时失效本地 Caffeine、删除 Redis key、发布 Pub/Sub 通知其他实例 |

### 限流体系

| 维度 | 方案 | 配置 |
|------|------|------|
| 全局限流（创建） | Guava RateLimiter | dev: 1000 QPS / test: 100000 QPS |
| IP 限流（创建） | Caffeine 缓存 Limiter，自动过期防泄漏 | dev: 10 QPS / test: 100000 QPS |
| 全局限流（跳转） | Guava RateLimiter | dev: 10000 QPS / test: 100000 QPS |
| IP 限流（跳转） | Caffeine 缓存 Limiter，自动过期防泄漏 | dev: 100 QPS / test: 100000 QPS |

- IP 限流器使用 Caffeine `expireAfterAccess(10min, maxSize=10000/50000)` 存储，IP 无活跃访问后自动淘汰，无内存泄漏风险。
- Swagger/OpenAPI 文档路径自动跳过限流检查。
- 限流超限返回 `429 Too Many Requests`。

### 消息可靠性保障

```
┌─────────────────────────────────────────────────────────────────┐
│                     访问日志发送 (AccessLogProducer)              │
│                     增量计数发送 (IncrementProducer)              │
└────────────────────────────┬────────────────────────────────────┘
                             │
                ┌────────────┼────────────┐
                ▼                         ▼
         RocketMQ Broker            发送失败 ──→ MessagePersistenceHelper
         linkcut_access_log                        │
         linkcut_access_incr        ┌──────────────┼──────────────┐
                │                   ▼              ▼              ▼
                ▼              L1: Redis       L2: 重试 3 次   L3: 本地文件
    ┌───────────────────┐      直接写入        指数退避       logs/mq_fallback.log
    │ Consumer 批量消费   │                    (500ms→1s→2s)
    │ INSERT IGNORE      │                         │
    │ access_count + N   │              仍失败 ──→ 本地文件兜底
    └────────┬──────────┘                         │
             │                            BacklogMonitor 60s 监控
             └────────────── MySQL ────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                         定时任务 (BacklogMonitor)                  │
│  checkBacklog()         60s  → 队列/DLQ 水位告警                   │
│  cleanupDlq()            1h  → DLQ 双维度清理 (容量溢出+超时淘汰)    │
│  checkLocalFallback()   60s  → 本地兜底文件大小监控                 │
│  retryFailedMessages()  30s  → 降级队列重发 RocketMQ               │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                       消息轨迹 (MessageTracer)                    │
│  Producer 端: SEND_SUCCESS / SEND_FAIL / SERIALIZE_FAIL          │
│  Consumer 端: RECEIVED (含端到端 delayMs) / HANDLE_BATCH /        │
│              HANDLE_FAIL (入 DLQ)                                │
│  独立 logger: linkcut.mq.trace → grep traceId 一行追全链路         │
└─────────────────────────────────────────────────────────────────┘
```

| 机制 | 说明 |
|------|------|
| 异步发送 | `SendCallback` 回调，不阻塞 `redirect` 主流程 |
| 降级队列 | Redis `RQueue`，MQ 发送失败时写入，定时任务每 30s 重试 |
| 三级降级链 | `MessagePersistenceHelper`：L1 Redis → L2 3 次指数退避重试 → L3 本地文件 `logs/mq_fallback.log` |
| 死信队列 (DLQ) | Redis `RList`，超过 3 次重试失败后移入，**支持自动清理**（容量溢出 FIFO 删头 + 超 72h 超时淘汰）|
| 本地文件兜底 | Redis 不可用时写入本地文件，BacklogMonitor 60s 监控文件大小 |
| 积压监控 | `BacklogMonitor` 每 60s 检查降级队列和 DLQ 水位，超阈值告警 |
| DLQ 自动清理 | `cleanupDlq()` 每小时执行：容量超 `dlq-max-size` FIFO 删头 + 消息超 `dlq-retain-hours(72h)` 自动删除 |
| 消息精简 | 降级队列水位超过 80% 时，自动丢弃 `userAgent`/`referer` 字段，仅保留核心数据 |
| 幂等去重 | 每条消息带 `message_key` UUID，Consumer 使用 `INSERT IGNORE` 防止重复消费 |
| 批量合并 | `IncrementConsumer` 按 shortCode 聚合同一批 +1 为 `access_count + N`，一次 UPDATE 替代 N 次 |
| 全链路追踪 | `MessageTracer` 以 messageKey 为 traceId，Producer/Consumer 双向埋点，`grep traceId` 一行追全链路 |

---

### 消息轨迹

`MessageTracer` 以 messageKey 为 traceId，在 Producer 发送和 Consumer 消费两端记录消息生命周期事件。输出至独立 logger `linkcut.mq.trace`，与业务日志隔离，可接入 ELK/Loki 生产级数据采集。

#### 轨迹事件

| 事件 | 端 | 级别 | 关键字段 | 触发时机 |
|------|-----|------|---------|---------|
| `SEND_SUCCESS` | Producer | INFO | traceId, msgId, costMs, shortCode | 异步发送 Broker ACK 成功 |
| `SEND_FAIL` | Producer | WARN | traceId, error, costMs, shortCode | 异步发送 Broker ACK 失败（触发降级）|
| `SERIALIZE_FAIL` | Producer | WARN | shortCode, error | 消息体 JSON 序列化失败 |
| `RECEIVED` | Consumer | DEBUG | traceId, msgId, delayMs | 单条消息被拉取（delayMs = 当前时间 - bornTimestamp）|
| `HANDLE_BATCH` | Consumer | INFO | totalMsgs, successCount, failCount, costMs | 整批处理完成 |
| `HANDLE_FAIL` | Consumer | WARN | msgId, error | 单条解析/校验失败（已入 DLQ）|

#### 轨迹示例

一次短链跳转 `aB3xY9k` 产生的轨迹：

```json
{"timestamp":"2026-10-14T12:00:00.100Z","event":"SEND_SUCCESS","traceId":"a1b2c3d4-...","topic":"linkcut_access_log","msgId":"7F000001...","shortCode":"aB3xY9k","costMs":12}
{"timestamp":"2026-10-14T12:00:00.150Z","event":"RECEIVED","traceId":"a1b2c3d4-...","msgId":"7F000001...","delayMs":38}
{"timestamp":"2026-10-14T12:00:00.158Z","event":"HANDLE_BATCH","totalMsgs":8,"successCount":8,"failCount":0,"costMs":6}
```

排查方式：

```bash
# 追踪某条消息完整链路
grep '"traceId":"a1b2c3d4"' app.log

# 统计端到端延迟分布
grep '"event":"RECEIVED"' app.log | grep -oP '"delayMs":\d+'
```

---

## 项目结构

```
Star-LinkCut-Demo/
├── README.md
├── .gitignore
├── LinkCut-Backend/                    # Spring Boot 后端项目
│   ├── pom.xml                         # Maven 依赖（Spring Boot 4.1.0 / Java 21）
│   ├── src/main/java/.../linkcutbackend/
│   │   ├── LinkCutBackendApplication.java   # 启动类（@EnableScheduling）
│   │   ├── cache/
│   │   │   └── LinkCacheManager.java        # L1 Caffeine + L2 Redis 缓存管理器
│   │   ├── common/
│   │   │   ├── Base62Utils.java             # Base62 编解码工具
│   │   │   ├── GlobalExceptionHandler.java  # 全局异常处理器
│   │   │   └── Result.java                  # 统一响应结果封装
│   │   ├── config/
│   │   │   ├── BloomFilterConfig.java        # RBloomFilter 初始化 + 启动加载
│   │   │   ├── OpenApiConfig.java            # SpringDoc OpenAPI 配置
│   │   │   ├── RedissonConfig.java           # Redisson 客户端配置
│   │   │   └── RocketMQConfig.java           # RocketMQ Producer/Consumer 配置
│   │   ├── controller/
│   │   │   └── LinkCutController.java        # REST 控制器（7 个端点 + IP 提取）
│   │   ├── dto/
│   │   │   ├── CacheStatsResponse.java       # 缓存统计响应
│   │   │   ├── CreateLinkRequest.java        # 创建短链请求（含 @Valid 校验）
│   │   │   ├── CreateLinkResponse.java       # 创建短链响应
│   │   │   ├── ExpireExtendRequest.java      # 延期请求
│   │   │   ├── ExpireExtendResponse.java     # 延期响应
│   │   │   ├── LinkInfoResponse.java         # 短链详情响应
│   │   │   └── LinkStatsResponse.java        # 访问统计响应（含 RefererItem）
│   │   ├── entity/
│   │   │   ├── AccessLog.java                # 访问日志实体
│   │   │   ├── ShortCodeSnapshot.java        # 短链码快照实体
│   │   │   └── ShortLink.java                # 短链主表实体（@TableLogic）
│   │   ├── filter/
│   │   │   └── RateLimitFilter.java          # Guava 全局+IP 两级限流过滤器
│   │   ├── generator/
│   │   │   ├── ShortCodeGenerator.java       # 短码生成器接口
│   │   │   └── SnowflakeShortCodeGenerator.java  # Snowflake 雪花算法实现
│   │   ├── mq/
│   │   │   ├── BacklogMonitor.java           # 消息积压监控 + DLQ 清理 + 本地文件监控
│   │   │   ├── MessagePersistenceHelper.java # 三级降级持久化 (Redis→重试→本地文件)
│   │   │   ├── MessageTracer.java            # 消息轨迹记录器 (6 种事件, 独立 logger)
│   │   │   ├── consumer/
│   │   │   │   ├── AccessLogConsumer.java    # 访问日志批量消费 + INSERT IGNORE + DLQ
│   │   │   │   └── IncrementConsumer.java    # 访问计数批量消费 (按 shortCode 合并 +N)
│   │   │   └── producer/
│   │   │       ├── AccessLogProducer.java    # 访问日志异步发送 + 降级队列 + 定时重试
│   │   │       └── IncrementProducer.java    # 访问计数异步发送 (无降级, 丢失可重算)
│   │   ├── repository/
│   │   │   ├── AccessLogRepository.java      # 访问日志 Mapper（统计+批量写入）
│   │   │   ├── ShortCodeSnapshotRepository.java  # 快照表 Mapper
│   │   │   └── ShortLinkRepository.java      # 短链主表 Mapper
│   │   ├── scheduler/
│   │   │   └── BloomFilterRebuildTask.java   # 布隆过滤器定时重建（每日 03:00）
│   │   └── service/
│   │       ├── LinkCutService.java           # 短链服务接口
│   │       └── impl/
│   │           └── LinkCutServiceImpl.java   # 业务逻辑实现（创建/跳转/详情/统计/删除/延期）
│   └── src/main/resources/
│       ├── application.yml                   # 主配置（MySQL/Redis/RocketMQ/Snowflake/Bloom）
│       ├── application-dev.yml               # dev 环境配置（限流 + 日志）
│       └── application-test.yml              # test 环境配置（≈关闭限流 + 减少日志）
├── docker/                                  # Docker 容器编排
│   ├── docker-compose.yml                   # MySQL 8.4 + Redis 8.2 + RocketMQ 5.3.0
│   ├── MySQL-8.4/
│   │   ├── conf/my.cnf                      # InnoDB 512M / 慢查询 2s
│   │   └── init.sql                         # 建库建表脚本
│   ├── Redis-8.2/
│   │   └── conf/redis.conf                  # AOF + RDB + allkeys-lru / 512MB
│   └── RocketMQ-5.3.0/
│       └── broker.conf                      # ASYNC_MASTER / 自动创建 Topic
├── jmeter/                                  # 压力测试
│   ├── LinkCut压测.jmx
│   └── redirect_codes.csv
└── postman/                                 # Postman 测试集合
    └── collections/LinkCut/
        ├── 创建短链.request.yaml
        └── 访问短链.request.yaml
```

---

## 数据库设计

### t_short_link（短链主表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | BIGINT PK | Snowflake 雪花 ID |
| `short_code` | VARCHAR(10) UNIQUE | Base62 短链码（7~8 位）|
| `origin_url` | TEXT | 原始长 URL |
| `expire_time` | DATETIME | 过期时间（NULL = 永不过期）|
| `create_time` | DATETIME | 创建时间 |
| `creator_ip` | VARCHAR(45) | 创建者 IP |
| `access_count` | BIGINT DEFAULT 0 | 总访问次数 |
| `status` | TINYINT DEFAULT 1 | 状态（1=有效，0=已删除，@TableLogic）|

### t_short_code_snapshot（短链码快照表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | BIGINT PK AUTO_INCREMENT | 自增主键 |
| `short_code` | VARCHAR(10) UNIQUE | 短链码（布隆过滤器重建数据源）|

### t_access_log（访问日志表）

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | BIGINT PK AUTO_INCREMENT | 自增主键 |
| `short_code` | VARCHAR(10) | 短链码 |
| `access_ip` | VARCHAR(45) | 访问者 IP |
| `user_agent` | VARCHAR(500) | 浏览器 UA |
| `referer` | VARCHAR(500) | 来源 URL |
| `access_time` | DATETIME | 访问时间 |
| `message_key` | VARCHAR(64) UNIQUE | RocketMQ 消息唯一键（幂等去重）|

---

## 快速开始

### 1. 启动 Docker 容器

将 `docker/` 目录复制到虚拟机 `/XiyueTsuki-Star-LinkCut-Demo/`，创建必要目录并启动：

```bash
mkdir -p /XiyueTsuki-Star-LinkCut-Demo/{MySQL-8.4/data,Redis-8.2/data,RocketMQ-5.3.0/{namesrv/logs,broker/{logs,store}}}
cd /XiyueTsuki-Star-LinkCut-Demo
docker compose up -d
```

验证容器状态：

```bash
docker ps
# linkcut-mysql-8.4          → 3306   (MySQL 8.4)
# linkcut-redis-8.2           → 6379   (Redis 8.2)
# linkcut-rmq-namesrv-5.3.0   → 9876   (RocketMQ Name Server)
# linkcut-rmq-broker-5.3.0    → 10911  (RocketMQ Broker)
# linkcut-rmq-dashboard        → 4003   (RocketMQ Dashboard)
```

> 可访问 `http://<虚拟机IP>:4003` 打开 RocketMQ Dashboard 查看消息投递情况，包含 `linkcut_access_log` 和 `linkcut_access_incr` 两个 Topic。

### 2. 配置后端连接

编辑 `LinkCut-Backend/src/main/resources/application.yml` 第 3 行，改为虚拟机实际 IP：

```yaml
app:
  host: 192.168.100.128    # ← 改成你的虚拟机 IP
```

> **注意**：`docker/RocketMQ-5.3.0/broker.conf` 第 17 行的 `brokerIP1` 也需要同步修改为虚拟机 IP，否则 Broker 注册到 Name Server 的地址将是容器内部 IP，导致客户端连接失败。

### 3. 启动后端

```bash
cd LinkCut-Backend
mvn spring-boot:run
```

启动成功后可访问：

- **接口文档**：http://localhost:8080/docs.html
- **RocketMQ Dashboard**：http://<虚拟机IP>:4003

---

## API 接口

> 所有接口均可在 http://localhost:8080/docs.html 中在线调试。

### 接口总览

| 方法 | 路径 | 说明 | 限流 |
|------|------|------|------|
| `POST` | `/api/link/create` | 创建短链 | 全局 + IP 两级 |
| `GET` | `/{shortCode}` | 短链跳转（302 重定向）| 全局 + IP 两级 |
| `GET` | `/api/link/{shortCode}/info` | 查询短链详情 | 无 |
| `GET` | `/api/link/{shortCode}/stats` | 查询访问统计（总PV/今日/近7日/Top10 Referer）| 无 |
| `DELETE` | `/api/link/{shortCode}` | 删除短链（逻辑删除 + 失效 L1/L2 + Pub/Sub 广播）| 无 |
| `PUT` | `/api/link/{shortCode}/expire` | 延期过期时间（刷新 Redis TTL）| 无 |
| `GET` | `/api/cache/stats` | 缓存命中率与布隆过滤器状态 | 无 |

### 创建短链

```http
POST /api/link/create
Content-Type: application/json

{
    "originUrl": "https://www.example.com/very/long/path?param1=value1&param2=value2",
    "expireSeconds": 86400
}
```

响应示例：

```json
{
    "code": 200,
    "message": "success",
    "data": {
        "shortCode": "aB3xY9k",
        "shortUrl": "http://localhost:8080/aB3xY9k",
        "originUrl": "https://www.example.com/very/long/path?param1=value1&param2=value2",
        "expireTime": "2026-10-14T12:00:00"
    }
}
```

- `originUrl`：必填，必须以 `http://` 或 `https://` 开头
- `expireSeconds`：可选，过期秒数，省略表示永不过期

### 短链跳转

```http
GET /aB3xY9k
```

- 存在且未过期 → `302 Found` 重定向至原始 URL
- 不存在（布隆过滤器拦截）→ `404 Not Found`
- 布隆过滤器误判 + 数据库无记录/已过期 → `404 Not Found`

### 查询短链详情

```http
GET /api/link/aB3xY9k/info
```

响应示例：

```json
{
    "code": 200,
    "data": {
        "shortCode": "aB3xY9k",
        "shortUrl": "http://localhost:8080/aB3xY9k",
        "originUrl": "https://www.example.com/very/long/path?param1=value1&param2=value2",
        "createTime": "2026-10-13T12:00:00",
        "expireTime": "2026-10-14T12:00:00",
        "accessCount": 1523,
        "status": 1
    }
}
```

### 查询访问统计

```http
GET /api/link/aB3xY9k/stats
```

响应示例：

```json
{
    "code": 200,
    "data": {
        "shortCode": "aB3xY9k",
        "totalPV": 5280,
        "todayPV": 347,
        "weekPV": 2100,
        "topReferers": [
            { "referer": "https://twitter.com", "count": 1200 },
            { "referer": "https://t.co", "count": 890 }
        ]
    }
}
```

### 删除短链

```http
DELETE /api/link/aB3xY9k
```

- 执行 MyBatis-Plus `@TableLogic` 逻辑删除（status → 0）
- 失效 L1 Caffeine + L2 Redis 缓存
- 通过 Redis Pub/Sub `linkcut:cache:invalidate` 广播，所有实例同步失效 L1
- 返回 `404` 表示短链不存在或已被删除

### 延期过期时间

```http
PUT /api/link/aB3xY9k/expire
Content-Type: application/json

{
    "extendSeconds": 86400
}
```

- `extendSeconds`：必填，最少 60 秒
- 在原有过期时间基础上增加；若已过期，从当前时间开始计算
- 同步刷新 Redis 缓存 TTL

### 缓存统计

```http
GET /api/cache/stats
```

响应示例：

```json
{
    "code": 200,
    "data": {
        "hitCount": 12345,
        "missCount": 123,
        "hitRate": 0.9901,
        "evictionCount": 0,
        "estimatedSize": 876,
        "bloomFilterCount": 1500
    }
}
```

---

## 环境与配置

### Profile 切换

| Profile | 用途 | 限流 QPS | 日志级别 |
|---------|------|---------|---------|
| `dev` | 本地开发 | 严格限流 | DEBUG |
| `test` | 压力测试 | ≈关闭限流（100000 QPS）| WARN |

切换方式：修改 `application.yml` 中的 `spring.profiles.active`：

```yaml
spring:
  profiles:
    active: dev     # 改为 test 即压测模式
```

### 关键配置项

| 配置项 | 位置 | 默认值 | 说明 |
|--------|------|--------|------|
| `app.host` | application.yml | `192.168.100.128` | Docker 宿主机 IP |
| `linkcut.domain` | application.yml / dev | `http://<ip>:8080` | 对外展示的短链域名 |
| `linkcut.snowflake.worker-id` | application.yml | `1` | 分布式实例需互不相同 |
| `linkcut.snowflake.datacenter-id` | application.yml | `1` | 数据中心 ID |
| `linkcut.bloom-filter.expected-insertions` | application.yml | `10000000` | 布隆过滤器预期容量 |
| `linkcut.bloom-filter.false-probability` | application.yml | `0.001` | 误判率（0.1%）|
| `linkcut.bloom-filter.rebuild-cron` | application.yml | `0 0 3 * * ?` | 重建 Cron 表达式 |
| `linkcut.caffeine.max-size` | application.yml | `10000` | L1 缓存最大条目数 |
| `linkcut.caffeine.expire-minutes` | application.yml | `5` | L1 缓存 TTL（分钟）|
| `rocketmq.fallback.max-retries` | application.yml | `3` | 降级队列最大重试次数 |
| `rocketmq.fallback.dlq-max-size` | application.yml | `5000` | DLQ 最大容量（超出 FIFO 删头）|
| `rocketmq.fallback.dlq-retain-hours` | application.yml | `72` | DLQ 消息最长保留小时数 |
| `rocketmq.fallback.dlq-cleanup-interval-ms` | application.yml | `3600000` | DLQ 清理间隔（1 小时）|
| `rocketmq.fallback.dlq-write-retries` | application.yml | `3` | 三级降级链中 Redis 写入重试次数 |
| `rocketmq.fallback.local-fallback-max-size-mb` | application.yml | `100` | 本地兜底文件告警阈值（MB）|

### 运维端口

| 服务 | 端口 | 管理地址 |
|------|------|---------|
| Spring Boot 应用 | 8080 | http://localhost:8080 |
| MySQL | 3306 | 虚拟机 IP |
| Redis | 6379 | 虚拟机 IP（密码: redis123）|
| RocketMQ Name Server | 9876 | 虚拟机 IP |
| RocketMQ Broker | 10911 | 虚拟机 IP |
| RocketMQ Dashboard | 4003 | http://<虚拟机IP>:4003 |
| SpringDoc OpenAPI | 8080 | http://localhost:8080/docs.html |

---

## 测试

### 单元测试

```bash
cd LinkCut-Backend
mvn test
```

测试内容：
- `testSnowflakeShortCodeGenerate`：验证短码生成唯一性
- `testBase62EncodeDecode`：验证 Base62 编解码正确性
- `testShortCodeLength`：验证短码长度在 6~9 位之间

### 压力测试

将 Profile 切换为 `test` 后，使用项目提供的 JMeter 脚本：

```bash
# 1. 修改 application.yml: spring.profiles.active = test
# 2. 启动后端
# 3. 使用 jmeter/LinkCut压测.jmx 执行压测
```

`jmeter/redirect_codes.csv` 提供预生成的短链码数据源，用于重定向接口压测。