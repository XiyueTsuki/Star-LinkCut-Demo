# LinkCut - 高并发短链系统

## 项目简介

LinkCut 是一个高并发短链服务，采用 **多级缓存 + 分布式锁 + 异步消息队列** 架构，有效应对缓存穿透、击穿、雪崩三大经典问题。

- **短码算法**：Snowflake 雪花算法 + Base62 编码（7~8 位）
- **联调验证**：MySQL 8.4 / Redis 8.2 / RocketMQ 5.3.0 全链路通过

## 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Spring Boot | 4.1.0 | 基础框架 |
| Java | 21 | 运行环境 |
| MyBatis-Plus | 3.5.16 | ORM 持久化 |
| MySQL | 8.4 | 短链数据与访问日志存储 |
| Redis | 8.2 | 缓存层 (Redisson 3.43) |
| Redisson | 3.43.0 | 分布式锁 + 布隆过滤器 |
| RocketMQ | 5.3.0 | 异步访问日志 |
| Guava | 33.4.0 | RateLimiter 限流 |
| Docker Compose | 3.3 | 容器编排 |

## 架构设计

```
┌──────────────────────────────────────────────────┐
│                   客户端请求                       │
└────────────┬──────────────┬──────────────────────┘
             │ 创建短链     │ 短链跳转
             ▼              ▼
┌────────────────────┐  ┌──────────────────────────────────┐
│  Guava RateLimiter │  │  布隆过滤器 (Redisson RBloomFilter)│
│  全局限流 + IP限流  │  │  ├─ 命中 → 放行                   │
└────────┬───────────┘  │  └─ 未命中 → 直接返回 404          │
         │              └────────┬─────────────────────────┘
         ▼                       │ 命中
┌────────────────────┐          ▼
│ Snowflake + Base62│  ┌────────────────────┐
│ 短码生成 + 冲突检测 │  │ Redis 缓存层        │
└────────┬───────────┘  │ (Redisson RBucket) │
         │              │ 随机 TTL 防雪崩     │
         ▼              └──────┬─────┬───────┘
┌────────────────────┐        │命中  │未命中
│    MySQL 持久化     │        │      ▼
│ (MyBatis-Plus)     │  返回原URL  ┌──────────────┐
└────────────────────┘             │ 分布式锁       │
                                   │ (Redisson RLock)│
        ┌──────────────────────────┤ 防缓存击穿      │
        │                          └──────┬─────────┘
        ▼                                 │ 获锁后查 MySQL
┌────────────────────┐                    │ 回写 Redis 缓存
│ RocketMQ Producer  │◄───────────────────┘
│ 异步发送访问日志     │
└────────┬───────────┘
         │
         ▼
┌────────────────────┐
│ RocketMQ Consumer  │
│ 批量写入访问日志表   │
└────────────────────┘
```

## 项目结构

```
Star-LinkCut-Demo/
├── LinkCut-Backend/                     # Spring Boot 后端
│   ├── pom.xml
│   └── src/main/java/.../
│       ├── cache/                       # Redis 缓存管理器
│       ├── common/                      # 通用工具 (Base62, Result, 全局异常)
│       ├── config/                      # Redisson / BloomFilter / RocketMQ 配置
│       ├── controller/                  # REST 控制器
│       ├── dto/                         # 请求/响应 DTO
│       ├── entity/                      # 实体 (ShortLink, AccessLog, ShortCodeSnapshot)
│       ├── filter/                      # Guava RateLimiter 限流过滤器
│       ├── generator/                   # Snowflake + Base62 短码生成器
│       ├── mq/                          # RocketMQ Producer / Consumer
│       ├── repository/                  # MyBatis-Plus Mapper
│       └── service/                     # 业务逻辑层
├── docker/                              # Docker 容器编排
│   ├── docker-compose.yml
│   ├── MySQL-8.4/
│   │   ├── init.sql                     # 建库建表脚本
│   │   └── conf/my.cnf                  # MySQL 自定义配置
│   ├── Redis-8.2/
│   │   └── conf/redis.conf              # Redis 配置
│   └── RocketMQ-5.3.0/
│       └── broker.conf                  # Broker 配置
└── postman/                             # Postman 测试集合
```

## 数据库设计

| 表名 | 说明 | 关键字段 |
|------|------|----------|
| `t_short_link` | 短链主表 | short_code (唯一索引), origin_url, expire_time |
| `t_short_code_snapshot` | 短链码快照 | short_code (布隆过滤器重建数据源) |
| `t_access_log` | 访问日志 | short_code, access_ip, user_agent, access_time |

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

编辑 `application.yml` 第 3 行，改为虚拟机实际 IP：

```yaml
app:
  host: 192.168.100.128    # ← 改成你的虚拟机 IP
```

### 3. 启动后端

```bash
cd LinkCut-Backend
mvn spring-boot:run
```

## API 接口

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
| `expireSeconds` | ❌ | 过期秒数，不传则永不过期 |

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

返回 **HTTP 302** 重定向至原始 URL。短链不存在时返回 404。

## 压测建议

**推荐工具：JMeter / wrk**

```bash
# 创建 100 个短链作为测试数据
for i in $(seq 1 100); do
  curl -s -X POST http://localhost:8080/api/link/create \
    -H "Content-Type: application/json" \
    -d "{\"originUrl\":\"https://www.baidu.com/s?wd=$i\"}" \
    | grep -o '"shortCode":"[^"]*"'
done > short_codes.txt

# wrk 跳转压测
wrk -t4 -c100 -d30s http://localhost:8080/3xP2kL9a

# wrk 混合脚本压测
wrk -t8 -c200 -d60s -s mix_test.lua http://localhost:8080
```

**JMeter 重点配置：** 跳转测试需取消勾选 `Follow Redirects`，否则会追百度服务器浪费带宽。

### 限流配置

| 接口 | 全局限流 | IP 限流 |
|------|---------|--------|
| 创建 | 1000 QPS | 10 QPS |
| 跳转 | 10000 QPS | 100 QPS |

超限返回 HTTP 429 Too Many Requests。

## 容器账号

| 服务 | 端口 | 账号 | 密码 |
|------|:---:|------|------|
| MySQL | 3306 | root / linkcut | root123 / linkcut123 |
| Redis | 6379 | — | redis123 |
| RocketMQ Dashboard | 4003 | — | — |