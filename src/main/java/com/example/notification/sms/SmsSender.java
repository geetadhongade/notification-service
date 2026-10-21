package com.example.notification.sms;

// Sends one SMS. Today only LoggingSmsSender exists; a real provider (Twilio, AWS SNS, ...)
// can be added as another implementation without changing the Kafka listener.
public interface SmsSender {

    void send(String phoneNumber, String text);
}
