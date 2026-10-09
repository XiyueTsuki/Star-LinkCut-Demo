package com.xiyuetsuki.linkcutbackend.repository;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.xiyuetsuki.linkcutbackend.entity.ShortLink;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * 短链主表 Mapper
 */
@Mapper
public interface ShortLinkRepository extends BaseMapper<ShortLink> {

    @Select("SELECT * FROM t_short_link WHERE short_code = #{shortCode} AND status = 1")
    ShortLink findByShortCode(@Param("shortCode") String shortCode);

    @Select("SELECT short_code FROM t_short_link WHERE status = 1")
    java.util.List<String> findAllActiveShortCodes();
}