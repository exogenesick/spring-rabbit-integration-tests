package it.pajak.rabbit.integrationtest.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import it.pajak.rabbit.integrationtest.Application;
import it.pajak.rabbit.integrationtest.EmailService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = Application.class, initializers = ConfigFileApplicationContextInitializer.class)
public class ListenerTest {
    private static String RABBIT_SERVICE_NAME = "rmq";
    private static int TEST_TIMEOUT_SECONDS = 5;

    @ClassRule
    public static DockerComposeRule docker = DockerComposeRule.builder()
        .file("src/test/resources/docker-compose.yml")
        .waitingForService(RABBIT_SERVICE_NAME, HealthChecks.toHaveAllPortsOpen())
        .build();

    private static ConnectionFactory connectionFactory;

    @BeforeClass
    public static void initialize() throws InterruptedException, URISyntaxException {
        URI connectionString = new URI(String.format("amqp://localhost:%s", 45672));
        connectionFactory = new CachingConnectionFactory(connectionString);
    }

    @Autowired
    private ListenerProperties listenerProperties;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private CounterService counterService;

    private RabbitTemplate rabbitTemplate;

    @Before
    public void setup() {
        rabbitTemplate = new RabbitTemplate(connectionFactory);
    }

    @Test
    public void should_send_email_and_notify_when_message_is_valid() throws Exception {
        // given
        EmailRequestAmqpMessage emailRequest = new EmailRequestAmqpMessage();
        emailRequest.email = "example@email.com";
        emailRequest.message = "Some message content";

        CountDownLatch latch = new CountDownLatch(1);

        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                latch.countDown();
                return null;
            }
        }).when(counterService).increment(String.format(EmailService.METRIC_EMAIL_SENT, emailRequest.email));

        // when
        rabbitTemplate.convertAndSend(
            listenerProperties.getExchangeName(),
            listenerProperties.getQueueName(),
            prepareMessage(objectMapper.writeValueAsString(emailRequest))
        );

        // then
        latch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertEquals(0, latch.getCount());
    }

    @Test
    public void should_notify_when_message_is_invalid() throws Exception {
        // given
        EmailRequestAmqpMessage emailRequest = new EmailRequestAmqpMessage();
        emailRequest.email = "example@email.com";
        emailRequest.message = "Some message content";

        CountDownLatch latch = new CountDownLatch(1);

        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                latch.countDown();
                return null;
            }
        }).when(counterService).increment(String.format(Consumer.METRIC_EMAIL_FAIL));

        // when
        rabbitTemplate.convertAndSend(
            listenerProperties.getExchangeName(),
            listenerProperties.getQueueName(),
            prepareMessage("MaLfo67325RmE%^Fd-Me76ss-a6@")
        );

        // then
        latch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertEquals(0, latch.getCount());
    }

    private Message prepareMessage(String rawMessage) throws JsonProcessingException {
        return MessageBuilder
            .withBody(rawMessage.getBytes())
            .andProperties(
                MessagePropertiesBuilder
                    .newInstance()
                    .setContentType("application/json")
                    .build()
            )
            .build();
    }
}