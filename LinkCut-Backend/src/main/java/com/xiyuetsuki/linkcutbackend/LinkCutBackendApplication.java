package com.xiyuetsuki.linkcutbackend;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@MapperScan("com.xiyuetsuki.linkcutbackend.repository")
public class LinkCutBackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(LinkCutBackendApplication.class, args);
    }

}