package com.example.notify.mq;

import com.example.notify.service.DispatcherService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RocketMQMessageListener(
        topic = "${notify.topic}",
        consumerGroup = "${rocketmq.consumer.group}",
        maxReconsumeTimes = 16
)
public class NotifyConsumer implements RocketMQListener<String> {

    private static final Logger log = LoggerFactory.getLogger(NotifyConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DispatcherService dispatcherService;

    public NotifyConsumer(DispatcherService dispatcherService) {
        this.dispatcherService = dispatcherService;
    }

    @Override
    public void onMessage(String messageJson) {
        try {
            Map<String, Object> message = MAPPER.readValue(messageJson,
                    new TypeReference<Map<String, Object>>() {});

            String notificationId = (String) message.get("notificationId");
            String supplierId = (String) message.get("supplierId");
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) message.get("payload");

            log.info("Dispatching notification {} to supplier {}", notificationId, supplierId);
            dispatcherService.dispatch(notificationId, supplierId, payload);
            log.info("Notification {} delivered successfully", notificationId);

        } catch (Exception e) {
            log.error("Failed to process notification message: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
