package com.example.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joshlong.batch.remotechunking.ChunkingItemWriter;
import com.joshlong.batch.remotechunking.ChunkingStep;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
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

    @SneakyThrows
    private String jsonFor(Object o) {
        return objectMapper.writeValueAsString(o);
    }

    @Bean
    @ChunkingStep
    TaskletStep step(JobRepository repository,
                     PlatformTransactionManager transactionManager,
                     @ChunkingItemWriter ItemWriter<String > itemWriter) {
        var listItemReader = new ListItemReader<>(List.of(new Customer("Dr. Syer"),
                new Customer("Michael"),
                new Customer("Mahmoud")));
        return new StepBuilder("step", repository)
                .<Customer, String>chunk(100, transactionManager)
                .reader(listItemReader)
                .processor(this::jsonFor)
                .writer(itemWriter)
                .build();
    }

    @Bean
    Job job(JobRepository repository, Step step) {
        return new JobBuilder("job", repository)
                .start(step)
                .build();
    }

}
