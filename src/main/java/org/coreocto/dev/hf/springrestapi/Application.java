package org.coreocto.dev.hf.springrestapi;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Arrays;
import java.util.Map;

@SpringBootApplication
public class Application {

    private Logger LOGGER = Logger.getLogger(this.getClass());

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {

        Map<String, Object> dummy = jdbcTemplate.queryForMap("select 1 as dummy");
        for (Map.Entry<String, Object> entry : dummy.entrySet()) {
            LOGGER.debug(entry.getKey() + "=>" + entry.getValue());
        }

        return args -> {

            System.out.println("Let's inspect the beans provided by Spring Boot:");

            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                System.out.println(beanName);
            }

        };
    }
}
