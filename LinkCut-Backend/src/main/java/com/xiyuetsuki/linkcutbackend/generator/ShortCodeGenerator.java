package com.xiyuetsuki.linkcutbackend.generator;

/**
 * 短链码生成器接口
 */
public interface ShortCodeGenerator {

    /**
     * 生成一个短链码
     *
     * @return Base62 短链码
     */
    String generate();
}