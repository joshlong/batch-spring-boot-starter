package com.example.batch;

import com.joshlong.batch.remotechunking.worker.WorkerItemProcessor;
import com.joshlong.batch.remotechunking.worker.WorkerItemWriter;
import com.joshlong.batch.remotechunking.worker.WorkerInboundChunkChannel;
import com.joshlong.batch.remotechunking.worker.WorkerOutboundChunkChannel;
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
	@WorkerItemProcessor
	ItemProcessor<Object, Object> itemProcessor() {
		return item -> item;
	}

	@Bean
	@WorkerItemWriter
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
	IntegrationFlow inboundAmqpIntegrationFlow(@WorkerInboundChunkChannel MessageChannel workerRequestsMessageChannel,
			ConnectionFactory connectionFactory) {
		return IntegrationFlow//
				.from(Amqp.inboundAdapter(connectionFactory, "requests"))//
				.channel(workerRequestsMessageChannel)//
				.get();
	}

	@Bean
	IntegrationFlow outboundAmqpIntegrationFlow(@WorkerOutboundChunkChannel MessageChannel workerRepliesMessageChannel,
			AmqpTemplate template) {
		return IntegrationFlow //
				.from(workerRepliesMessageChannel)//
				.handle(Amqp.outboundAdapter(template).routingKey("replies"))//
				.get();
	}

}
