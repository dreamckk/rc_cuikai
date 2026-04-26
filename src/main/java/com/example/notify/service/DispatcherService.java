package com.example.notify.service;

import com.example.notify.config.SupplierConfigCache;
import com.example.notify.http.ExternalApiClient;
import com.example.notify.model.NotificationLog;
import com.example.notify.model.NotificationStatus;
import com.example.notify.model.SupplierConfig;
import com.example.notify.repository.NotificationLogRepository;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class DispatcherService {

    private final SupplierConfigCache supplierConfigCache;
    private final ExternalApiClient apiClient;
    private final NotificationLogRepository logRepository;

    public DispatcherService(SupplierConfigCache supplierConfigCache,
                             ExternalApiClient apiClient,
                             NotificationLogRepository logRepository) {
        this.supplierConfigCache = supplierConfigCache;
        this.apiClient = apiClient;
        this.logRepository = logRepository;
    }

    public void dispatch(String notificationId, String supplierId, Map<String, Object> payload) {
        SupplierConfig config = supplierConfigCache.get(supplierId)
                .orElseThrow(() -> new IllegalStateException("Supplier config not found: " + supplierId));

        NotificationLog log = logRepository.findById(notificationId)
                .orElseThrow(() -> new IllegalStateException("Notification log not found: " + notificationId));

        try {
            apiClient.send(config, payload);
            log.setStatus(NotificationStatus.DELIVERED);
            log.setAttempts(log.getAttempts() + 1);
            logRepository.save(log);
        } catch (Exception e) {
            log.setStatus(NotificationStatus.FAILED);
            log.setAttempts(log.getAttempts() + 1);
            log.setLastError(e.getMessage());
            logRepository.save(log);
            throw e;
        }
    }
}
