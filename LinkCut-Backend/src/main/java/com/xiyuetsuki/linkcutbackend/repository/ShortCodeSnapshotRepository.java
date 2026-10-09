package com.xiyuetsuki.linkcutbackend.repository;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.xiyuetsuki.linkcutbackend.entity.ShortCodeSnapshot;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * 短链码快照表 Mapper
 */
@Mapper
public interface ShortCodeSnapshotRepository extends BaseMapper<ShortCodeSnapshot> {

    @Select("SELECT short_code FROM t_short_code_snapshot")
    java.util.List<String> findAllShortCodes();
}