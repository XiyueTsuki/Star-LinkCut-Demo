package com.xiyuetsuki.linkcutbackend.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 短链码快照表实体（布隆过滤器重建数据源）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("t_short_code_snapshot")
public class ShortCodeSnapshot {

    @TableId(type = IdType.AUTO)
    private Long id;

    private String shortCode;
}