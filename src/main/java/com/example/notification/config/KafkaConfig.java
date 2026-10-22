package com.example.notification.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

// What happens when handling a message fails: retry a few times, then park it on the dead-letter topic
@Configuration
public class KafkaConfig {

    // The dead-letter topic (DLT): messages that still fail after all retries end up here, so they aren't lost.
    // Created at startup. Same number of partitions as order-events, because a failed message
    // is put into the DLT partition with the same number as the partition it came from.
    @Bean
    public NewTopic orderEventsDeadLetterTopic(@Value("${notification.kafka.topic.order-events-dlt}") String dltName,
                                               @Value("${notification.kafka.topic.order-events-partitions}") int partitions) {
        return TopicBuilder.name(dltName)
                .partitions(partitions)
                // Only one Kafka server, so only one copy of each message
                .replicas(1)
                .build();
    }

    // Spring Boot plugs this into our @KafkaListener automatically
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                            @Value("${notification.kafka.topic.order-events-dlt}") String dltName,
                                            @Value("${notification.kafka.retry.interval-ms}") long retryIntervalMs,
                                            @Value("${notification.kafka.retry.max-retries}") long maxRetries) {
        // After the last retry: copy the message (same key, body and headers, plus headers saying why it failed)
        // to the DLT, into the partition with the same number it came from
        DeadLetterPublishingRecoverer sendToDeadLetterTopic = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (failedRecord, exception) -> new TopicPartition(dltName, failedRecord.partition()));

        // Wait retryIntervalMs between tries; maxRetries = 3 means 4 tries in total
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(sendToDeadLetterTopic,
                new FixedBackOff(retryIntervalMs, maxRetries));

        // JSON we can't read will never get better by retrying: send it to the DLT straight away
        errorHandler.addNotRetryableExceptions(JacksonException.class);
        return errorHandler;
    }
}
