package com.example.batch;

import com.joshlong.batch.remotechunking.InboundChunkChannel;
import com.joshlong.batch.remotechunking.OutboundChunkChannel;
import com.joshlong.batch.remotechunking.worker.WorkerChunkItemProcessor;
import com.joshlong.batch.remotechunking.worker.WorkerChunkItemWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.MessageChannel;

import java.util.List;

@Slf4j
@RequiredArgsConstructor
@SpringBootApplication
public class WorkerNodeApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerNodeApplication.class, args);
    }

    @Bean
    @WorkerChunkItemProcessor
    ItemProcessor<Object, Object> itemProcessor() {
        return item -> item;
    }

    @Bean
    @WorkerChunkItemWriter
    ItemWriter<Object> itemWriter() {
        return chunk -> {
            //
            log.info("doing the long-running writing thing");
            List<?> items = chunk.getItems();
            for (var i : items)
                log.info("i={}", i + "");
        };
    }

    @Bean
    IntegrationFlow inboundAmqpIntegrationFlow(
            @InboundChunkChannel MessageChannel inboundMessageChannel,
            ConnectionFactory connectionFactory) {
        return IntegrationFlow
                .from(Amqp.inboundAdapter(connectionFactory, "requests"))
                .channel(inboundMessageChannel)
                .get();
    }

    @Bean
    IntegrationFlow outboundAmqpIntegrationFlow(
            @OutboundChunkChannel MessageChannel outboundMessageChannel,
            AmqpTemplate template) {
        return IntegrationFlow //
                .from(outboundMessageChannel)
                .handle(Amqp.outboundAdapter(template).routingKey("replies"))
                .get();
    }


}
