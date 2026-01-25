package com.dbpopulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class DbPopulatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(DbPopulatorApplication.class, args);
    }
}
