package com.springkafka.demo;


import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppBeans {

    @Bean
    public Producer initProducer() {
        return new Producer();
    }

    @Bean
    public Consumer initConsumer(){ return new Consumer(); }

}