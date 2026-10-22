package com.example.notification.consumer;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.UUID;

// Remembers which events were already handled (table processed_event), so a redelivered event sends no second SMS
@Repository
public class ProcessedEventRepository {

    private final JdbcTemplate jdbcTemplate;

    public ProcessedEventRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // Saves the event ID. Returns true if it is new, false if it was already there (a duplicate).
    // Check and save happen in ONE statement, so two copies of this service can't both think the event is new.
    public boolean markProcessed(UUID eventId, String eventType, Long orderId) {
        int rowsInserted = jdbcTemplate.update(
                "INSERT INTO processed_event (event_id, event_type, order_id) VALUES (?, ?, ?) "
                        + "ON CONFLICT (event_id) DO NOTHING",
                eventId, eventType, orderId);
        return rowsInserted == 1;
    }
}
