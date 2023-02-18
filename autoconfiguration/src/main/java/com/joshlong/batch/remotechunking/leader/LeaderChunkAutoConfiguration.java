package com.joshlong.batch.remotechunking.leader;

import lombok.extern.slf4j.Slf4j;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.integration.chunk.ChunkRequest;
import org.springframework.batch.integration.chunk.ChunkResponse;
import org.springframework.batch.integration.chunk.RemoteChunkHandlerFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.MessageChannels;

import java.util.Set;

@Slf4j
@Configuration
@ConditionalOnProperty(value = "bootiful.batch.chunk.leader", havingValue = "true")
@ImportRuntimeHints(LeaderChunkAutoConfiguration.Hints.class)
class LeaderChunkAutoConfiguration {

	static class Hints implements RuntimeHintsRegistrar {

		@Override
		public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
			Set.of(ChunkMessageChannelItemWriter.class)
					.forEach(c -> hints.reflection().registerType(c, MemberCategory.values()));
		}

	}

	private final static String MESSAGING_TEMPLATE_BEAN_NAME = "leaderChunkMessagingTemplate";

	@Bean
	@LeaderOutboundChunkChannel
	DirectChannel leaderRequestsMessageChannel() {
		return MessageChannels.direct().get();
	}

	@Bean
	@LeaderInboundChunkChannel
	QueueChannel leaderRepliesMessageChannel() {
		return MessageChannels.queue().get();
	}

	@Bean
	@ConditionalOnMissingBean
	@Qualifier(MESSAGING_TEMPLATE_BEAN_NAME)
	MessagingTemplate leaderChunkMessagingTemplate() {
		var template = new MessagingTemplate();
		template.setDefaultChannel(leaderRequestsMessageChannel());
		template.setReceiveTimeout(2000);
		return template;
	}

	@Bean
	@ConditionalOnMissingBean
	RemoteChunkHandlerFactoryBean<Object> leaderChunkHandler(
			ChunkMessageChannelItemWriter<Object> chunkMessageChannelItemWriterProxy,
			@LeaderChunkStep TaskletStep step) {
		var remoteChunkHandlerFactoryBean = new RemoteChunkHandlerFactoryBean<>();
		remoteChunkHandlerFactoryBean.setChunkWriter(chunkMessageChannelItemWriterProxy);
		remoteChunkHandlerFactoryBean.setStep(step);
		return remoteChunkHandlerFactoryBean;
	}

	@Bean
	@LeaderItemWriter
	// @StepScope
	ChunkMessageChannelItemWriter<?> leaderChunkMessageChannelItemWriter(
			@Qualifier(MESSAGING_TEMPLATE_BEAN_NAME) MessagingTemplate template) {
		var chunkMessageChannelItemWriter = new ChunkMessageChannelItemWriter<>();
		chunkMessageChannelItemWriter.setMessagingOperations(template);
		chunkMessageChannelItemWriter.setReplyChannel(leaderRepliesMessageChannel());
		return chunkMessageChannelItemWriter;
	}

}
