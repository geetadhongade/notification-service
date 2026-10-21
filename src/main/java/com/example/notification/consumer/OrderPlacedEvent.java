package com.example.notification.consumer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

// Our own copy of the "order placed" message that shop-api sends (services don't share Java classes).
// Fields shop-api adds later are simply ignored, so they can't break this service.
public record OrderPlacedEvent(
        UUID eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        Long orderId,
        Long customerId,
        String customerFirstName,
        String customerPhone,
        BigDecimal totalAmount,
        String currency,
        // Total quantity of all items
        int itemCount
) {
}
