package com.joshlong.batch.remotechunking.leader;

import com.joshlong.batch.remotechunking.InboundChunkChannel;
import com.joshlong.batch.remotechunking.OutboundChunkChannel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.integration.chunk.RemoteChunkHandlerFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
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
@ConditionalOnProperty(value = "bootiful.batch.chunk.leader", havingValue = "true")
class LeaderChunkAutoConfiguration {

    private final static String MESSAGING_TEMPLATE_BEAN_NAME = "leaderChunkMessagingTemplate";

    @Bean
    @ConditionalOnMissingBean
    @Qualifier(MESSAGING_TEMPLATE_BEAN_NAME)
    MessagingTemplate leaderChunkMessagingTemplate(@OutboundChunkChannel MessageChannel channel) {
        var template = new MessagingTemplate();
        template.setDefaultChannel(channel);
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
    @StepScope
    @ConditionalOnMissingBean
    ChunkMessageChannelItemWriter<?> leaderChunkMessageChannelItemWriter(
            @Qualifier(MESSAGING_TEMPLATE_BEAN_NAME) MessagingTemplate template) {
        var chunkMessageChannelItemWriter = new ChunkMessageChannelItemWriter<>();
        chunkMessageChannelItemWriter.setMessagingOperations(template);
        chunkMessageChannelItemWriter.setReplyChannel(leaderRepliesMessageChannel());
        return chunkMessageChannelItemWriter;
    }

    @Bean
    @ConditionalOnMissingBean
    @OutboundChunkChannel
    DirectChannel leaderRequestsMessageChannel() {
        return MessageChannels.direct().get();
    }

    @Bean
    @ConditionalOnMissingBean
    @InboundChunkChannel
    QueueChannel leaderRepliesMessageChannel() {
        return MessageChannels.queue().get();
    }
}



