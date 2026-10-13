# LinkCut - 高并发短链系统

## 项目简介

LinkCut 是一个面向求职展示的高并发短链服务，采用 **Caffeine L1 本地缓存 + Redis L2 分布式缓存 + Redisson 布隆过滤器 + 分布式锁 + RocketMQ 异步日志** 架构，系统性地解决了缓存穿透、击穿、雪崩三大经典问题，并补齐了多实例缓存一致性、布隆过滤器定时重建及可观测性。

- **短码算法**：Snowflake 雪花算法 + Base62 编码（7~8 位）
- **接口文档**：SpringDoc OpenAPI → http://localhost:8080/docs.html
- **联调验证**：MySQL 8.4 / Redis 8.2 / RocketMQ 5.3.0 全链路通过

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Spring Boot | 4.1.0 | 基础框架 |
| Java | 21 | 运行环境 |
| MyBatis-Plus | 3.5.16 | ORM 持久化，@TableLogic 逻辑删除 |
| MySQL | 8.4 | 短链数据与访问日志存储 |
| Redis | 8.2 | 缓存层（Redisson 3.43） |
| Redisson | 3.43.0 | 分布式锁 + 布隆过滤器 + Pub/Sub |
| RocketMQ | 5.3.0 | 异步访问日志采集 |
| Guava | 33.4.0 | RateLimiter 限流 |
| Caffeine | 3.2.0 | L1 本地缓存 + IP 限流器存储 |
| SpringDoc | 2.8.5 | OpenAPI 接口文档与在线调试 |
| Docker Compose | 3.3 | 容器编排 |

## 架构设计

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
  └───────────┬───────────┘          │  ├─ 命中 → 回填 L1 + 返回  │
              │                      │  └─ 未命中 → 布隆过滤器    │
  ┌───────────▼───────────┐          └────────────┬─────────────┘
  │ MySQL 持久化 (状态=1)   │                       │
  │ 写 Caffeine L1 + L2   │          ┌────────────▼─────────────┐
  │ 写入布隆过滤器          │          │  Redisson 布隆过滤器      │  ← 防穿透
  └───────────────────────┘          │  ├─ 不含 → 直接返回 404    │
                                     │  └─ 含 → 获取分布式锁      │
                                     └────────────┬─────────────┘
                                                  │
                                     ┌────────────▼─────────────┐
                                     │  Redisson 分布式锁         │  ← 防击穿
                                     │  双检 + 单线程查 DB + 回写  │
                                     └────────────┬─────────────┘
                                                  │
                                     ┌────────────▼─────────────┐
                                     │  MySQL 短链主表            │
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
                          │ 批量写 t_access_log  │
                          └────────────────────┘

          ┌─────────────────────────────────────────┐
          │  Redis Pub/Sub (linkcut:cache:invalidate) │  ← 多实例 Caffeine 一致性
          │  删除时广播 → 所有实例失效本地缓存         │
          └─────────────────────────────────────────┘

          ┌─────────────────────────────────────────┐
          │  布隆过滤器定时重建 (每日 03:00)           │
          │  扫描 DB status=1 → RENAME 原子切换       │
          └─────────────────────────────────────────┘
```

## 缓存策略全景

| 缓存层 | 技术 | 作用 | 关键设计 |
|--------|------|------|---------|
| L1 本地缓存 | Caffeine | 热点数据 0 网络延迟 | TTL 5min，`recordStats` 可观测 |
| L2 分布式缓存 | Redis RBucket | 跨实例共享 | TTL 对齐短链过期时间，±30% 随机偏移防雪崩 |
| 布隆过滤器 | Redisson RBloomFilter | 拦截不存在 key，防穿透 | 1kw 预期容量 / 0.1% 误判率；每日凌晨 3 点定时重建 |
| 分布式锁 | Redisson RLock | 单线程回源 DB，防击穿 | tryLock 3s 等待 / 10s 租约；DB 查询后回写 L1 + L2 |
| Pub/Sub 广播 | Redisson RTopic | 多实例 Caffeine 一致性 | 删除/更新时广播，所有实例毫秒级失效本地缓存 |

### 四大缓存问题的解决

| 问题 | 方案 |
|------|------|
| **缓存穿透**（查不存在的数据） | 布隆过滤器 + DB `status=1` 过滤兜底 |
| **缓存击穿**（热点 key 过期） | Redisson 分布式锁，锁内 double-check + 单线程回写 |
| **缓存雪崩**（大量 key 同时过期） | `calculateTtl()` 加 ±30% 随机偏移 |
| **多实例一致性**（Caffeine 过期窗口） | Redis Pub/Sub 广播失效 + 布隆过滤器定时重建 |

## 项目结构

```
Star-LinkCut-Demo/
├── LinkCut-Backend/
│   ├── pom.xml
│   └── src/main/java/.../linkcutbackend/
│       ├── cache/           # Caffeine + Redis 两级缓存管理器
│       ├── common/          # Base62 编解码 / 统一响应 Result / 全局异常处理
│       ├── config/          # Redisson / BloomFilter / RocketMQ / OpenAPI 配置
│       ├── controller/      # REST 控制器（7 个端点）
│       ├── dto/             # 请求/响应 DTO（含 Swagger @Schema 注解）
│       ├── entity/          # MyBatis-Plus 实体（@TableLogic 逻辑删除）
│       ├── filter/          # Guava RateLimiter 限流过滤器（Caffeine 防泄漏）
│       ├── generator/       # Snowflake 雪花算法 + Base62 编码
│       ├── mq/              # RocketMQ Producer / Consumer
│       ├── repository/      # MyBatis-Plus Mapper（含聚合统计查询）
│       ├── scheduler/       # 布隆过滤器定时重建任务
│       └── service/         # 短链业务逻辑（创建/跳转/详情/统计/删除/延期）
├── docker/                  # Docker 容器编排
│   ├── docker-compose.yml
│   ├── MySQL-8.4/           # init.sql + my.cnf
│   ├── Redis-8.2/           # redis.conf（AOF + RDB + allkeys-lru）
│   └── RocketMQ-5.3.0/      # broker.conf
└── postman/                 # Postman 测试集合
```

## 数据库设计

| 表名 | 说明 | 关键字段 |
|------|------|----------|
| `t_short_link` | 短链主表 | `short_code` (唯一索引), `origin_url`, `expire_time`, `status` (@TableLogic) |
| `t_short_code_snapshot` | 短链码快照 | `short_code`（布隆过滤器重建数据源） |
| `t_access_log` | 访问日志 | `short_code`, `access_ip`, `referer`, `access_time`（RocketMQ 异步写入）|

## 快速开始

### 1. 启动 Docker 容器

将 `docker/` 目录复制到虚拟机 `/XiyueTsuki-Star-LinkCut-Demo/`，创建必要目录并启动：

```bash
mkdir -p /XiyueTsuki-Star-LinkCut-Demo/{MySQL-8.4/data,Redis-8.2/data,RocketMQ-5.3.0/{namesrv/logs,broker/{logs,store}}}
docker-compose up -d
```

验证容器状态：

```bash
docker ps
# linkcut-mysql-8.4         → 3306
# linkcut-redis-8.2          → 6379
# linkcut-rmq-namesrv-5.3.0  → 9876
# linkcut-rmq-broker-5.3.0   → 10911
# linkcut-rmq-dashboard      → 4003
```

### 2. 配置后端连接

编辑 `LinkCut-Backend/src/main/resources/application.yml` 第 3 行，改为虚拟机实际 IP：

```yaml
app:
  host: 192.168.100.128    # ← 改成你的虚拟机 IP
```

### 3. 启动后端

```bash
cd LinkCut-Backend
mvn spring-boot:run

# 接口文档：http://localhost:8080/docs.html
```

## API 接口

> 所有接口可在 http://localhost:8080/docs.html 在线调试

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/api/link/create` | 创建短链 |
| `GET` | `/{shortCode}` | 短链跳转（302 重定向） |
| `GET` | `/api/link/{shortCode}/info` | 查询短链详情 |
| `GET` | `/api/link/{shortCode}/stats` | 查询访问统计 |
| `DELETE` | `/api/link/{shortCode}` | 删除短链（逻辑删除 + 失效缓存） |
| `PUT` | `/api/link/{shortCode}/expire` | 延期过期时间 |
| `GET` | `/api/cache/stats` | 缓存命中率与布隆过滤器状态 |

### 创建短链

```http
POST /api/link/create
Content-Type: application/json

{
  "originUrl": "https://www.baidu.com",
  "expireSeconds": 86400
}
```

| 参数 | 必填 | 说明 |
|------|:---:|------|
| `originUrl` | ✅ | 原始长 URL，必须以 `http://` 或 `https://` 开头 |
| `expireSeconds` | ❌ | 过期秒数，不传则永不过期（默认 7 天缓存 TTL） |

**响应示例：**

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "shortCode": "3xP2kL9a",
    "shortUrl": "http://192.168.100.128:8080/3xP2kL9a",
    "originUrl": "https://www.baidu.com",
    "expireTime": "2026-10-10T21:50:00"
  }
}
```

### 短链跳转

```http
GET /{shortCode}
```

返回 **HTTP 302** 重定向至原始 URL。短链不存在或已过期时返回 404。

### 查询短链详情

```http
GET /api/link/{shortCode}/info

# 响应
{
  "code": 200,
  "data": {
    "shortCode": "3xP2kL9a",
    "shortUrl": "http://192.168.100.128:8080/3xP2kL9a",
    "originUrl": "https://www.baidu.com",
    "createTime": "2026-10-09T21:50:00",
    "expireTime": "2026-10-10T21:50:00",
    "accessCount": 1234,
    "status": 1
  }
}
```

### 查询访问统计

```http
GET /api/link/{shortCode}/stats

# 响应
{
  "code": 200,
  "data": {
    "shortCode": "3xP2kL9a",
    "totalPV": 12345,
    "todayPV": 567,
    "weekPV": 8901,
    "topReferers": [
      { "referer": "https://weibo.com", "count": 3200 },
      { "referer": "https://zhihu.com", "count": 1500 }
    ]
  }
}
```

### 删除短链

```http
DELETE /api/link/{shortCode}

# 逻辑删除：DB status → 0 + Caffeine L1 失效 + Redis L2 删除 + Pub/Sub 广播
```

### 延期过期时间

```http
PUT /api/link/{shortCode}/expire
Content-Type: application/json

{
  "extendSeconds": 86400    # 在原有过期时间上增加 1 天（最少 60s）
}
```

若原链路已过期，则从当前时间开始计算。延期后同步刷新 Redis 缓存 TTL。

### 缓存统计

```http
GET /api/cache/stats

# 响应
{
  "code": 200,
  "data": {
    "hitCount": 98765,
    "missCount": 1234,
    "hitRate": 0.9876,
    "evictionCount": 500,
    "estimatedSize": 3200,
    "bloomFilterCount": 150000
  }
}
```

## 限流配置

环境通过 `spring.profiles.active` 切换（默认 `dev`）：

| Profile | 创建全局限流 | 创建 IP 限流 | 跳转全局限流 | 跳转 IP 限流 |
|---------|:-----------:|:-----------:|:-----------:|:-----------:|
| `dev` | 1000 QPS | 10 QPS | 10000 QPS | 100 QPS |
| `test` | 100000 QPS | 10000 QPS | 100000 QPS | 10000 QPS |

超限返回 HTTP 429 Too Many Requests。IP 限流器使用 Caffeine 存储（10 分钟无访问自动淘汰），防止恶意换 IP 导致内存泄漏。

## 压测建议

**推荐工具：JMeter / wrk**

```bash
# 生成 100 个短链作为测试数据
for i in $(seq 1 100); do
  curl -s -X POST http://localhost:8080/api/link/create \
    -H "Content-Type: application/json" \
    -d "{\"originUrl\":\"https://www.baidu.com/s?wd=$i\"}" \
    | grep -o '"shortCode":"[^"]*"'
done > short_codes.txt

# JMeter 跳转压测（取消勾选 Follow Redirects）
# 200 线程 × 100 循环 → 预期 QPS 受限于 test profile 限流值

# wrk 混合脚本压测
wrk -t8 -c200 -d60s -s mix_test.lua http://localhost:8080
```

**JMeter 重要提示：** 跳转测试务必取消勾选 `HTTP Request` → `Follow Redirects`，否则每个请求都会追到目标网站，浪费带宽并干扰压测结果。

## 容器账号

| 服务 | 端口 | 账号 | 密码 |
|------|:---:|------|------|
| MySQL | 3306 | root / linkcut | root123 / linkcut123 |
| Redis | 6379 | — | redis123 |
| RocketMQ Dashboard | 4003 | — | — |