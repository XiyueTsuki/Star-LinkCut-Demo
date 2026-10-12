package com.xiyuetsuki.linkcutbackend.repository;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.xiyuetsuki.linkcutbackend.entity.AccessLog;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * 访问日志表 Mapper
 */
@Mapper
public interface AccessLogRepository extends BaseMapper<AccessLog> {

    @Select("SELECT COUNT(*) FROM t_access_log WHERE short_code = #{shortCode}")
    Long countTotalByShortCode(@Param("shortCode") String shortCode);

    @Select("SELECT COUNT(*) FROM t_access_log WHERE short_code = #{shortCode} AND access_time >= #{since}")
    Long countByShortCodeSince(@Param("shortCode") String shortCode, @Param("since") LocalDateTime since);

    @Select("SELECT referer, COUNT(*) AS cnt FROM t_access_log " +
            "WHERE short_code = #{shortCode} AND referer IS NOT NULL AND referer != '' " +
            "GROUP BY referer ORDER BY cnt DESC LIMIT #{limit}")
    List<Map<String, Object>> topReferers(@Param("shortCode") String shortCode, @Param("limit") int limit);
}