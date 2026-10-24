package com.example.notification.consumer;

import com.example.notification.sms.SmsSender;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

// Unit test: the SMS sender and the database are fakes (mocks); a real JsonMapper reads the message body
@ExtendWith(MockitoExtension.class)
class OrderEventListenerTest {

    private static final UUID EVENT_ID = UUID.fromString("6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b");

    // The JSON body shop-api sends for order 26 (Bob, 3 items, USD 2119.00)
    private static final String ORDER_26_JSON = """
            {
              "eventId": "6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b",
              "eventType": "ORDER_PLACED",
              "schemaVersion": 1,
              "occurredAt": "2026-10-21T10:15:30Z",
              "orderId": 26,
              "customerId": 6,
              "customerFirstName": "Bob",
              "customerPhone": "5550101",
              "totalAmount": 2119.00,
              "currency": "USD",
              "itemCount": 3
            }
            """;

    @Mock
    private SmsSender smsSender;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    private OrderEventListener listener;

    @BeforeEach
    void setUp() {
        listener = new OrderEventListener(JsonMapper.builder().build(), smsSender, processedEventRepository);
    }

    @Test
    void orderPlaced_newEvent_marksProcessedAndSendsSms() {
        when(processedEventRepository.markProcessed(EVENT_ID, "ORDER_PLACED", 26L)).thenReturn(true);

        listener.onOrderEvent(buildRecord("ORDER_PLACED", ORDER_26_JSON));

        verify(smsSender).send("5550101", "Hi Bob, your order #26 (3 items, USD 2119.00) has been placed.");
    }

    @Test
    void orderPlaced_alreadyHandled_sendsNoSecondSms() {
        // The event ID is already in processed_event: Kafka delivered the same message again
        when(processedEventRepository.markProcessed(EVENT_ID, "ORDER_PLACED", 26L)).thenReturn(false);

        listener.onOrderEvent(buildRecord("ORDER_PLACED", ORDER_26_JSON));

        verify(smsSender, never()).send(anyString(), anyString());
    }

    @Test
    void otherEventType_isSkippedWithoutTouchingTheDatabase() {
        listener.onOrderEvent(buildRecord("ORDER_CANCELLED", ORDER_26_JSON));

        verifyNoInteractions(processedEventRepository, smsSender);
    }

    @Test
    void unknownFieldsInTheJson_areIgnored() {
        // shop-api may add fields later; that must not break this service
        String jsonWithNewField = ORDER_26_JSON.replace("\"itemCount\": 3", "\"itemCount\": 3, \"newField\": \"x\"");
        when(processedEventRepository.markProcessed(EVENT_ID, "ORDER_PLACED", 26L)).thenReturn(true);

        listener.onOrderEvent(buildRecord("ORDER_PLACED", jsonWithNewField));

        verify(smsSender).send("5550101", "Hi Bob, your order #26 (3 items, USD 2119.00) has been placed.");
    }

    @Test
    void unreadableJson_throwsAndSendsNothing() {
        // KafkaConfig sends JacksonException straight to the dead-letter topic (no retries)
        assertThatThrownBy(() -> listener.onOrderEvent(buildRecord("ORDER_PLACED", "{not json")))
                .isInstanceOf(JacksonException.class);

        verifyNoInteractions(processedEventRepository, smsSender);
    }

    @Test
    void smsFails_exceptionIsPassedOn() {
        // The exception must reach Spring: it rolls back the processed_event row and the error handler retries
        when(processedEventRepository.markProcessed(EVENT_ID, "ORDER_PLACED", 26L)).thenReturn(true);
        doThrow(new IllegalStateException("SMS provider down")).when(smsSender).send(anyString(), anyString());

        assertThatThrownBy(() -> listener.onOrderEvent(buildRecord("ORDER_PLACED", ORDER_26_JSON)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("SMS provider down");
    }

    @Test
    void requestId_isInTheLogWhileHandlingAndRemovedAfter() {
        when(processedEventRepository.markProcessed(EVENT_ID, "ORDER_PLACED", 26L)).thenReturn(true);
        // Remember what the request ID was at the moment the SMS was sent
        String[] requestIdDuringSend = new String[1];
        doAnswer(call -> {
            requestIdDuringSend[0] = MDC.get("reqId");
            return null;
        }).when(smsSender).send(anyString(), anyString());

        listener.onOrderEvent(buildRecord("ORDER_PLACED", ORDER_26_JSON));

        assertThat(requestIdDuringSend[0]).isEqualTo("my-request-1");
        // Removed afterwards, so the next message on this thread doesn't show the wrong request ID
        assertThat(MDC.get("reqId")).isNull();
    }

    @Test
    void buildOrderPlacedText_oneItem_saysItemNotItems() {
        OrderPlacedEvent event = new OrderPlacedEvent(EVENT_ID, "ORDER_PLACED", 1, Instant.now(),
                12L, 6L, "Bob", "5550101", new BigDecimal("50.00"), "USD", 1);

        assertThat(OrderEventListener.buildOrderPlacedText(event))
                .isEqualTo("Hi Bob, your order #12 (1 item, USD 50.00) has been placed.");
    }

    // A Kafka message like the ones shop-api sends: key = order ID, headers eventId/eventType/reqId
    private static ConsumerRecord<String, String> buildRecord(String eventType, String json) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("order-events", 1, 7L, "26", json);
        record.headers().add("eventId", EVENT_ID.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("eventType", eventType.getBytes(StandardCharsets.UTF_8));
        record.headers().add("reqId", "my-request-1".getBytes(StandardCharsets.UTF_8));
        return record;
    }
}
