package com.example.notification.consumer;

import com.example.notification.sms.SmsSender;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;

// Listens to the order-events topic and sends an SMS for every ORDER_PLACED event
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    // Same MDC key as shop-api, so both apps' log lines show [reqId=...]
    private static final String MDC_KEY = "reqId";

    private final JsonMapper jsonMapper;
    private final SmsSender smsSender;
    private final ProcessedEventRepository processedEventRepository;

    public OrderEventListener(JsonMapper jsonMapper, SmsSender smsSender,
                              ProcessedEventRepository processedEventRepository) {
        this.jsonMapper = jsonMapper;
        this.smsSender = smsSender;
        this.processedEventRepository = processedEventRepository;
    }

    // Called by Spring Kafka once for every message on the topic.
    // @Transactional: the "already handled" row is only kept if the SMS was sent;
    // if sending fails, the row is rolled back so a retry tries the SMS again.
    // concurrency = 3: this app starts 3 consumers (one thread each), one per partition of order-events.
    // More than the number of partitions would be idle.
    @KafkaListener(topics = "${notification.kafka.topic.order-events}", concurrency = "3")
    @Transactional
    public void onOrderEvent(ConsumerRecord<String, String> record) {
        // Request ID of the original POST /api/orders call in shop-api, so one order can be traced across both apps
        String requestId = headerValue(record, "reqId");
        if (requestId != null) {
            MDC.put(MDC_KEY, requestId);
        }
        try {
            String eventId = headerValue(record, "eventId");
            String eventType = headerValue(record, "eventType");
            log.info("Received {} event {} for order {} (partition {}, offset {})",
                    eventType, eventId, record.key(), record.partition(), record.offset());

            // Only "order placed" events send an SMS; other event types are skipped
            if (!"ORDER_PLACED".equals(eventType)) {
                log.info("Skipping event {}: type {} needs no SMS", eventId, eventType);
                return;
            }

            OrderPlacedEvent event = jsonMapper.readValue(record.value(), OrderPlacedEvent.class);

            // Already handled before (Kafka delivered it again): don't send a second SMS
            if (!processedEventRepository.markProcessed(event.eventId(), event.eventType(), event.orderId())) {
                log.info("Skipping duplicate event {} for order {}: already handled", event.eventId(), event.orderId());
                return;
            }

            smsSender.send(event.customerPhone(), buildOrderPlacedText(event));
        } finally {
            MDC.remove(MDC_KEY);
        }
    }

    // e.g. "Hi Bob, your order #12 (5 items, USD 4188.00) has been placed."
    static String buildOrderPlacedText(OrderPlacedEvent event) {
        String items = event.itemCount() == 1 ? "1 item" : event.itemCount() + " items";
        return "Hi " + event.customerFirstName() + ", your order #" + event.orderId()
                + " (" + items + ", " + event.currency() + " " + event.totalAmount().toPlainString()
                + ") has been placed.";
    }

    // A header's text, or null if the message doesn't have it
    private static String headerValue(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
