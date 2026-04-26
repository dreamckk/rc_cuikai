package com.example.notify.service;

import com.example.notify.config.SupplierConfigCache;
import com.example.notify.http.ExternalApiClient;
import com.example.notify.model.NotificationLog;
import com.example.notify.model.NotificationStatus;
import com.example.notify.model.SupplierConfig;
import com.example.notify.repository.NotificationLogRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DispatcherServiceTest {

    @Mock private SupplierConfigCache supplierConfigCache;
    @Mock private ExternalApiClient apiClient;
    @Mock private NotificationLogRepository logRepository;
    @InjectMocks private DispatcherService service;

    private SupplierConfig buildSupplier() {
        SupplierConfig config = new SupplierConfig();
        config.setId("ad_system_a");
        config.setUrl("https://example.com/notify");
        config.setHeaders("{}");
        config.setBodyTemplate("{\"uid\":\"${userId}\"}");
        config.setTimeoutMs(5000);
        return config;
    }

    @Test
    void dispatchSuccessUpdatesStatusToDelivered() {
        SupplierConfig config = buildSupplier();
        when(supplierConfigCache.get("ad_system_a")).thenReturn(Optional.of(config));

        NotificationLog log = new NotificationLog();
        log.setId("notif-001");
        log.setSupplierId("ad_system_a");
        log.setStatus(NotificationStatus.PENDING);
        log.setAttempts(0);
        when(logRepository.findById("notif-001")).thenReturn(Optional.of(log));

        doNothing().when(apiClient).send(any(), any());

        service.dispatch("notif-001", "ad_system_a", Map.of("userId", "u123"));

        ArgumentCaptor<NotificationLog> captor = ArgumentCaptor.forClass(NotificationLog.class);
        verify(logRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(NotificationStatus.DELIVERED);
        assertThat(captor.getValue().getAttempts()).isEqualTo(1);
    }

    @Test
    void dispatchFailureThrowsAndUpdatesStatusToFailed() {
        SupplierConfig config = buildSupplier();
        when(supplierConfigCache.get("ad_system_a")).thenReturn(Optional.of(config));

        NotificationLog log = new NotificationLog();
        log.setId("notif-002");
        log.setSupplierId("ad_system_a");
        log.setStatus(NotificationStatus.PENDING);
        log.setAttempts(0);
        when(logRepository.findById("notif-002")).thenReturn(Optional.of(log));

        doThrow(new RuntimeException("500 Internal Server Error"))
                .when(apiClient).send(eq(config), any());

        assertThatThrownBy(() -> service.dispatch("notif-002", "ad_system_a", Map.of("userId", "u456")))
                .isInstanceOf(RuntimeException.class);

        ArgumentCaptor<NotificationLog> captor = ArgumentCaptor.forClass(NotificationLog.class);
        verify(logRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(NotificationStatus.FAILED);
        assertThat(captor.getValue().getLastError()).contains("500");
    }
}
