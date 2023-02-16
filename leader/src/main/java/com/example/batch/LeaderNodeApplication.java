package com.example.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.batch.remotechunking.InboundChunkChannel;
import com.joshlong.batch.remotechunking.OutboundChunkChannel;
import com.joshlong.batch.remotechunking.leader.LeaderChunkStep;
import com.joshlong.batch.remotechunking.leader.LeaderItemWriter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.MessageChannel;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.List;

@RequiredArgsConstructor
@SpringBootApplication
public class LeaderNodeApplication {

    public static void main(String[] args) {
        SpringApplication.run(LeaderNodeApplication.class, args);
    }

    private final ObjectMapper objectMapper;

    public record Customer(String name) {
    }

    @Bean
    @LeaderChunkStep
    TaskletStep step(JobRepository repository,
                     PlatformTransactionManager transactionManager,
                     @LeaderItemWriter ItemWriter<String> itemWriter) {
        var listItemReader = new ListItemReader<>(List.of(new Customer("Dr. Syer"), new Customer("Michael"), new Customer("Mahmoud")));
        return new StepBuilder("step", repository)
                .<Customer, String>chunk(100, transactionManager)
                .reader(listItemReader)
                .processor(this::jsonFor)
                .writer(itemWriter)
                .build();
    }

    @Bean
    IntegrationFlow outboundIntegrationFlow(
            @OutboundChunkChannel MessageChannel out,
            AmqpTemplate amqpTemplate) {
        return IntegrationFlow //
                .from(out)
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey("requests"))
                .get();
    }

    @Bean
    IntegrationFlow inboundIntegrationFlow(ConnectionFactory cf, @InboundChunkChannel MessageChannel in) {
        return IntegrationFlow
                .from(Amqp.inboundAdapter(cf, "replies"))
                .channel(in)
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



