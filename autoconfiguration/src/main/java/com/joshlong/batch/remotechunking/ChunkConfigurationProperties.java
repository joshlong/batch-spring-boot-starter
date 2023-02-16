package com.joshlong.batch.remotechunking;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "bootiful.batch.chunk")
public record ChunkConfigurationProperties(Leader leader, Worker worker) {

    public record Leader(boolean enabled) {
    }

    public record Worker(boolean enabled) {
    }
}
