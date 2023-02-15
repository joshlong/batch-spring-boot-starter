package com.example.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.batch.remotechunking.ChunkingItemWriter;
import com.joshlong.batch.remotechunking.ChunkingStep;
import com.joshlong.batch.remotechunking.InboundChunkChannel;
import com.joshlong.batch.remotechunking.OutboundChunkChannel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.integration.chunk.ChunkRequest;
import org.springframework.batch.integration.chunk.ChunkResponse;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

@RequiredArgsConstructor
@SpringBootApplication
public class BatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

    private final ObjectMapper objectMapper;

    public record Customer(String name) {
    }

    @Bean
    @ChunkingStep
    TaskletStep step(JobRepository repository,
                     PlatformTransactionManager transactionManager,
                     @ChunkingItemWriter ItemWriter<String> itemWriter) {
        var listItemReader = new ListItemReader<>(List.of(new Customer("Dr. Syer"), new Customer("Michael"), new Customer("Mahmoud")));
        return new StepBuilder("step", repository)
                .<Customer, String>chunk(100, transactionManager)
                .reader(listItemReader)
                .processor(this::jsonFor)
                .writer(itemWriter)
                .build();
    }

    //todo connect this with rabbitmq or kafka or something real so i can setup a worker node
    @Bean
    IntegrationFlow chunkIntegrationFlow(
            @OutboundChunkChannel MessageChannel outbound,
            @InboundChunkChannel MessageChannel inbound) {
        return IntegrationFlow
                .from(outbound)
                .handle(message -> {
                    if (message.getPayload() instanceof ChunkRequest<?> chunkRequest) {
                        var chunkResponse = new ChunkResponse(chunkRequest.getSequence(),
                                chunkRequest.getJobId(),
                                chunkRequest.getStepContribution());
                        inbound.send(MessageBuilder.withPayload(chunkResponse).build());
                    }
                })
                .get();
    }

    @Bean
    Job job(JobRepository repository, Step step) {
        return new JobBuilder("job", repository)
                .start(step)
                .build();
    }

    @SneakyThrows
    private String jsonFor(Object o) {
        return objectMapper.writeValueAsString(o);
    }

}
