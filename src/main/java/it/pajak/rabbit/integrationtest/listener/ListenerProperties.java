package it.pajak.rabbit.integrationtest.listener;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "listener")
public class ListenerProperties {
    private String exchangeName;
    private String queueName;

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }
}
