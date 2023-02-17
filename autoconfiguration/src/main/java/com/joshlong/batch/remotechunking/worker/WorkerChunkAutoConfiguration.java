package com.joshlong.batch.remotechunking.worker;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.step.item.SimpleChunkProcessor;
import org.springframework.batch.integration.chunk.ChunkProcessorChunkHandler;
import org.springframework.batch.integration.chunk.ChunkRequest;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

@Slf4j
@Configuration
@ConditionalOnProperty(value = "bootiful.batch.chunk.worker", havingValue = "true")
class WorkerChunkAutoConfiguration {

	@Bean
	@WorkerInboundChunkChannel
	DirectChannel workerRequestsMessageChannel() {
		return MessageChannels.direct().get();
	}

	@Bean
	@WorkerOutboundChunkChannel
	DirectChannel workerRepliesMessageChannel() {
		return MessageChannels.direct().get();
	}

	@Bean
	@ConditionalOnMissingBean
	ChunkProcessorChunkHandler<?> workerChunkProcessorChunkHandler(
			// todo make this optional
			// @WorkerChunkingItemProcessor ObjectProvider<ItemProcessor<Object, Object>>
			// processor,
			@WorkerChunkItemProcessor ItemProcessor<Object, Object> processor,
			@WorkerChunkItemWriter ItemWriter<Object> writer) {
		var chunkProcessorChunkHandler = new ChunkProcessorChunkHandler<>();
		chunkProcessorChunkHandler.setChunkProcessor(new SimpleChunkProcessor<>(processor, writer));
		return chunkProcessorChunkHandler;
	}

	@Bean
	@SuppressWarnings("unchecked")
	IntegrationFlow chunkProcessorChunkHandlerIntegrationFlow(
			ChunkProcessorChunkHandler<Object> chunkProcessorChunkHandler,
			@WorkerInboundChunkChannel MessageChannel inbound, @WorkerOutboundChunkChannel MessageChannel outbound) {
		return IntegrationFlow//
				.from(inbound)//
				.handle(message -> {
					try {
						var payload = message.getPayload();
						if (payload instanceof ChunkRequest<?> cr) {
							var chunkResponse = chunkProcessorChunkHandler.handleChunk((ChunkRequest<Object>) cr);
							outbound.send(MessageBuilder.withPayload(chunkResponse).build());
						}
						Assert.state(payload instanceof ChunkRequest<?>,
								"the payload must be an instance of ChunkRequest!");
					} //
					catch (Exception e) {
						throw new RuntimeException(e);
					}
				})//
				.get();
	}

}
