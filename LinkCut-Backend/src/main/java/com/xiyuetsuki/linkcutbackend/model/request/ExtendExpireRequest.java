package com.xiyuetsuki.linkcutbackend.model.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
@Schema(description = "过期时间延期请求")
public class ExtendExpireRequest {

    @NotNull(message = "延期秒数不能为空")
    @Min(value = 60, message = "延期最少60秒")
    @Schema(description = "延期秒数，在原有过期时间基础上增加（若原已过期则从当前时间开始计算）", example = "86400")
    private Long extendSeconds;
}