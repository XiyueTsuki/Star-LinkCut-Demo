-- LinkCut 短链系统 - MySQL 初始化脚本
-- 该脚本在 Docker 容器首次启动时自动执行

CREATE DATABASE IF NOT EXISTS linkcut
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE linkcut;

-- 短链主表
CREATE TABLE IF NOT EXISTS t_short_link (
    id            BIGINT        PRIMARY KEY       COMMENT 'Snowflake 雪花ID',
    short_code    VARCHAR(10)   NOT NULL           COMMENT 'Base62 短链码(7~8位)',
    origin_url    TEXT          NOT NULL           COMMENT '原始长URL',
    expire_time   DATETIME                         COMMENT '过期时间(NULL表示永不过期)',
    create_time   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    creator_ip    VARCHAR(45)                       COMMENT '创建者IP',
    access_count  BIGINT        DEFAULT 0          COMMENT '总访问次数',
    status        TINYINT       DEFAULT 1          COMMENT '状态: 1-有效 0-已删除',
    UNIQUE KEY uk_short_code (short_code),
    INDEX idx_expire_time (expire_time),
    INDEX idx_create_time (create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='短链主表';

-- 短链码快照表（用于重建布隆过滤器）
CREATE TABLE IF NOT EXISTS t_short_code_snapshot (
    id            BIGINT        AUTO_INCREMENT PRIMARY KEY,
    short_code    VARCHAR(10)   NOT NULL,
    UNIQUE KEY uk_short_code (short_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='短链码快照表(布隆过滤器重建数据源)';

-- 访问日志表（RocketMQ Consumer 异步批量写入）
CREATE TABLE IF NOT EXISTS t_access_log (
    id            BIGINT        PRIMARY KEY AUTO_INCREMENT COMMENT '自增主键',
    short_code    VARCHAR(10)   NOT NULL               COMMENT '短链码',
    access_ip     VARCHAR(45)                           COMMENT '访问者IP',
    user_agent    VARCHAR(500)                          COMMENT '浏览器UA',
    referer       VARCHAR(500)                          COMMENT '来源URL',
    access_time   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '访问时间',
    INDEX idx_short_code_time (short_code, access_time),
    INDEX idx_access_time (access_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='访问日志表';