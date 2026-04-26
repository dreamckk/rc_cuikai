package com.example.notify.service;

import com.example.notify.config.SupplierConfigCache;
import com.example.notify.model.NotificationLog;
import com.example.notify.model.NotificationStatus;
import com.example.notify.mq.NotifyProducer;
import com.example.notify.repository.NotificationLogRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

@Service
public class NotifyService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SupplierConfigCache supplierConfigCache;
    private final NotificationLogRepository logRepository;
    private final NotifyProducer producer;

    public NotifyService(SupplierConfigCache supplierConfigCache,
                         NotificationLogRepository logRepository,
                         NotifyProducer producer) {
        this.supplierConfigCache = supplierConfigCache;
        this.logRepository = logRepository;
        this.producer = producer;
    }

    public String submit(String supplierId, Map<String, Object> payload) {
        if (!supplierConfigCache.exists(supplierId)) {
            throw new IllegalArgumentException("Unknown supplier: " + supplierId);
        }

        String id = UUID.randomUUID().toString();

        NotificationLog log = new NotificationLog();
        log.setId(id);
        log.setSupplierId(supplierId);
        log.setPayload(toJson(payload));
        log.setStatus(NotificationStatus.PENDING);
        log.setAttempts(0);
        logRepository.save(log);

        producer.publish(id, id, supplierId, payload);
        return id;
    }

    private String toJson(Map<String, Object> payload) {
        try {
            return MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize payload", e);
        }
    }
}
