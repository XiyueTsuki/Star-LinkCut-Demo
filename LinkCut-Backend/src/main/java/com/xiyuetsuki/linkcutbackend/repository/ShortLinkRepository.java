package com.xiyuetsuki.linkcutbackend.repository;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.xiyuetsuki.linkcutbackend.entity.ShortLink;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

/**
 * 短链主表 Mapper
 */
@Mapper
public interface ShortLinkRepository extends BaseMapper<ShortLink> {

    @Select("SELECT * FROM t_short_link WHERE short_code = #{shortCode} AND status = 1")
    ShortLink findByShortCode(@Param("shortCode") String shortCode);

    @Select("SELECT short_code FROM t_short_link WHERE status = 1")
    java.util.List<String> findAllActiveShortCodes();

    /**
     * 原子递增短链访问计数
     * <p>
     * 使用 {@code SET access_count = access_count + #{delta}} 保证并发安全。
     * 由 {@link com.xiyuetsuki.linkcutbackend.mq.consumer.IncrementConsumer} 批量合并后调用。
     *
     * @param shortCode 短链码
     * @param delta     增量（合并后的累计次数，如某批次出现 8 次则 delta=8）
     * @return 受影响行数（0 表示短链不存在或已删除）
     */
    @Update("UPDATE t_short_link SET access_count = access_count + #{delta} WHERE short_code = #{shortCode} AND status = 1")
    int incrementAccessCount(@Param("shortCode") String shortCode, @Param("delta") int delta);
}