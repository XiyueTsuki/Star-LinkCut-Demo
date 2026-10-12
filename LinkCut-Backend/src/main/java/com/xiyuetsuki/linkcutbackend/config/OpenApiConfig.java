package com.xiyuetsuki.linkcutbackend.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI linkCutOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("LinkCut 短链系统接口文档")
                        .description("高并发短链服务 - 支持短链创建、重定向跳转、详情查询、访问统计")
                        .version("1.0.0")
                        .contact(new Contact()
                                .name("xiyuetsuki")));
    }
}