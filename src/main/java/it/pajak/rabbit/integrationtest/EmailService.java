package it.pajak.rabbit.integrationtest;

import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.stereotype.Service;

@Service
public class EmailService {
    private CounterService counterService;
    public static final String METRIC_EMAIL_SENT = "email.%s.sent";

    public EmailService(CounterService counterService) {
        this.counterService = counterService;
    }

    public void send(String email, String message) {
        // implementation of sending email

        // metrics notification
        counterService.increment(String.format(METRIC_EMAIL_SENT, email));
    }
}
