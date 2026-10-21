package com.example.notification.sms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

// Fake SMS sender: writes the SMS to the log instead of sending it (no account, no cost)
@Component
public class LoggingSmsSender implements SmsSender {

    private static final Logger log = LoggerFactory.getLogger(LoggingSmsSender.class);

    @Override
    public void send(String phoneNumber, String text) {
        log.info("SMS to {}: {}", phoneNumber, text);
    }
}
