package com.xiyuetsuki.linkcutbackend.repository;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.xiyuetsuki.linkcutbackend.entity.AccessLog;
import org.apache.ibatis.annotations.Mapper;

/**
 * 访问日志表 Mapper
 */
@Mapper
public interface AccessLogRepository extends BaseMapper<AccessLog> {
}