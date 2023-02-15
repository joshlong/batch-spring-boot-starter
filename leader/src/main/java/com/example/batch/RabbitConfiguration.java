package com.example.batch;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
class RabbitConfiguration {

    @Bean
    org.springframework.amqp.core.Queue requestQueue() {
        return new org.springframework.amqp.core.Queue("requests", false);
    }

    @Bean
    org.springframework.amqp.core.Queue repliesQueue() {
        return new Queue("replies", false);
    }

    @Bean
    TopicExchange exchange() {
        return new TopicExchange("remote-chunking-exchange");
    }

    @Bean
    Binding repliesBinding(TopicExchange exchange) {
        return BindingBuilder.bind(repliesQueue()).to(exchange).with("replies");
    }

    @Bean
    Binding requestBinding(TopicExchange exchange) {
        return BindingBuilder.bind(requestQueue()).to(exchange).with("requests");
    }
}
