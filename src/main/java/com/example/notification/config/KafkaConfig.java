package com.example.notification.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

import java.nio.charset.StandardCharsets;

// What happens when handling a message fails: retry a few times, then park it on the dead-letter topic
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    // Same MDC key as OrderEventListener, so these lines show [reqId=...] too
    private static final String MDC_KEY = "reqId";

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

        // Log every failed try and every message sent to the DLT (Spring's own log lines don't say why)
        errorHandler.setRetryListeners(new RetryListener() {

            // Called after each failed try (also the single try of a JSON error, which is then not retried)
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                withRequestId(record, () -> log.warn("Try {} failed for event {} (order {}): {}",
                        deliveryAttempt, headerValue(record, "eventId"), record.key(), reason(ex)));
            }

            // Called after the message was written to the DLT
            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                withRequestId(record, () -> log.error("Gave up on event {} (order {}), sent to {}: {}",
                        headerValue(record, "eventId"), record.key(), dltName, reason(ex)));
            }
        });
        return errorHandler;
    }

    // These log lines run after the listener has finished (and cleared the request ID),
    // so put the message's request ID back for the duration of the log call
    private static void withRequestId(ConsumerRecord<?, ?> record, Runnable logCall) {
        String requestId = headerValue(record, "reqId");
        if (requestId != null) {
            MDC.put(MDC_KEY, requestId);
        }
        try {
            logCall.run();
        } finally {
            MDC.remove(MDC_KEY);
        }
    }

    // The real error, e.g. "IllegalStateException: Fake SMS failure ...", not Spring's wrapper around it
    private static String reason(Exception ex) {
        Throwable rootCause = NestedExceptionUtils.getMostSpecificCause(ex);
        return rootCause.getClass().getSimpleName() + ": " + rootCause.getMessage();
    }

    // A header's text, or null if the message doesn't have it
    private static String headerValue(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
