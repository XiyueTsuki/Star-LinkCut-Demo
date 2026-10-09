package com.xiyuetsuki.linkcutbackend.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableLogic;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 短链主表实体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("t_short_link")
public class ShortLink {

    @TableId(type = IdType.ASSIGN_ID)
    private Long id;

    private String shortCode;

    private String originUrl;

    private LocalDateTime expireTime;

    private LocalDateTime createTime;

    private String creatorIp;

    private Long accessCount;

    @TableLogic
    private Integer status;
}