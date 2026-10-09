package com.xiyuetsuki.linkcutbackend.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 访问日志实体（RocketMQ Consumer 异步批量写入）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("t_access_log")
public class AccessLog {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String shortCode;

    private String accessIp;

    private String userAgent;

    private String referer;

    private LocalDateTime accessTime;
}