package it.pajak.rabbit.integrationtest.listener;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(ListenerProperties.class)
public class Listener {
    @Autowired
    ListenerProperties listenerProperties;

    @Bean
    public SimpleMessageListenerContainer listenerContainer(
        ConnectionFactory connectionFactory,
        Consumer consumer
    ) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(listenerProperties.getQueueName());
        container.setMessageListener(consumer);
        return container;
    }

    @Bean
    Queue queue() {
        return new Queue(listenerProperties.getQueueName(), false);
    }

    @Bean
    TopicExchange exchange() {
        return new TopicExchange(listenerProperties.getExchangeName());
    }

    @Bean
    Binding binding(Queue queue, TopicExchange exchange) {
        return BindingBuilder.bind(queue)
            .to(exchange)
            .with(listenerProperties.getQueueName());
    }
}