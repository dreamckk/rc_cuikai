package com.example.notify.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class NotifyProducer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RocketMQTemplate rocketMQTemplate;
    private final String topic;

    public NotifyProducer(RocketMQTemplate rocketMQTemplate,
                          @Value("${notify.topic}") String topic) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.topic = topic;
    }

    public void publish(String notificationId, String notificationLogId, String supplierId, Map<String, Object> payload) {
        Map<String, Object> message = new HashMap<>();
        message.put("notificationId", notificationId);
        message.put("supplierId", supplierId);
        message.put("payload", payload);

        try {
            String json = MAPPER.writeValueAsString(message);
            rocketMQTemplate.send(topic, MessageBuilder.withPayload(json)
                    .setHeader("notificationId", notificationId)
                    .build());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize notification message", e);
        }
    }
}
