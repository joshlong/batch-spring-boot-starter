package com.joshlong.batch.remotechunking;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.integration.chunk.RemoteChunkHandlerFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.messaging.MessageChannel;

@Slf4j
@Configuration
class RemoteChunkAutoConfiguration {


    RemoteChunkAutoConfiguration() {
        log.info("remote chunking auto configuration started");
    }

    @Slf4j
    @Configuration
    @ConditionalOnProperty(value = "bootiful.batch.chunk.leader", havingValue = "true")
    static class LeaderAutoConfiguration {

        LeaderAutoConfiguration() {
            log.info("starting the leader node");
        }

        @Bean
        @ConditionalOnMissingBean
        @Qualifier("remoteChunkMessagingTemplate")
        MessagingTemplate remoteChunkMessagingTemplate(@ChunkRequestsChannel MessageChannel channel) {
            var template = new MessagingTemplate();
            template.setDefaultChannel(channel);
            template.setReceiveTimeout(2000);
            return template;
        }

        @Bean
        @ConditionalOnMissingBean
        RemoteChunkHandlerFactoryBean<Object> remoteChunkHandler(
                ChunkMessageChannelItemWriter<Object> chunkMessageChannelItemWriterProxy,
                @ChunkingStep TaskletStep step) throws Exception {
            var remoteChunkHandlerFactoryBean = new RemoteChunkHandlerFactoryBean<>();
            remoteChunkHandlerFactoryBean.setChunkWriter(chunkMessageChannelItemWriterProxy);
            remoteChunkHandlerFactoryBean.setStep(step);
            return remoteChunkHandlerFactoryBean;
        }

        @Bean
        @ChunkingItemWriter
        @StepScope
        @ConditionalOnMissingBean
        ChunkMessageChannelItemWriter<?> chunkMessageChannelItemWriter(
                @Qualifier("remoteChunkMessagingTemplate") MessagingTemplate template) {
            var chunkMessageChannelItemWriter = new ChunkMessageChannelItemWriter<>();
            chunkMessageChannelItemWriter.setMessagingOperations(template);
            chunkMessageChannelItemWriter.setReplyChannel(remoteChunkRepliesMessageChannel());
            return chunkMessageChannelItemWriter;
        }

        @Bean
        @ConditionalOnMissingBean
        @ChunkRequestsChannel
        DirectChannel remoteChunkRequestsMessageChannel() {
            return MessageChannels.direct().get();
        }

        @Bean
        @ConditionalOnMissingBean
        @ChunkRepliesChannel
        QueueChannel remoteChunkRepliesMessageChannel() {
            return MessageChannels.queue().get();
        }
    }
}


