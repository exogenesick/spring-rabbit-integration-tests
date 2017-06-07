package it.pajak.rabbit.integrationtest.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.pajak.rabbit.integrationtest.EmailService;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class Consumer implements MessageListener {
    private EmailService emailService;
    private ObjectMapper objectMapper;
    private CounterService counterService;
    public static final String METRIC_EMAIL_FAIL = "email.fail";

    public Consumer(
        EmailService emailService, 
        ObjectMapper objectMapper,
        CounterService counterService
    ) {
        this.emailService = emailService;
        this.objectMapper = objectMapper;
        this.counterService = counterService;
    }

    @Override
    public void onMessage(Message message) {
        EmailRequestAmqpMessage emailRequest = null;

        try {
            emailRequest = objectMapper.readValue(message.getBody(), EmailRequestAmqpMessage.class);
        } catch (IOException e) {
            counterService.increment(METRIC_EMAIL_FAIL);
            return;
        }

        emailService.send(emailRequest.email, emailRequest.message);
    }
}