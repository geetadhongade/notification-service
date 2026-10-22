package com.example.notification.sms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

// Fake SMS sender: writes the SMS to the log instead of sending it (no account, no cost)
@Component
public class LoggingSmsSender implements SmsSender {

    private static final Logger log = LoggerFactory.getLogger(LoggingSmsSender.class);

    // Testing only: when true, every send fails, so retries and the dead-letter topic can be seen
    private final boolean fakeFailure;

    public LoggingSmsSender(@Value("${notification.sms.fake-failure}") boolean fakeFailure) {
        this.fakeFailure = fakeFailure;
    }

    @Override
    public void send(String phoneNumber, String text) {
        if (fakeFailure) {
            throw new IllegalStateException("Fake SMS failure (notification.sms.fake-failure=true)");
        }
        log.info("SMS to {}: {}", phoneNumber, text);
    }
}
