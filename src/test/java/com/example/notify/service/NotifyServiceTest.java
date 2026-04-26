package com.example.notify.service;

import com.example.notify.config.SupplierConfigCache;
import com.example.notify.model.NotificationLog;
import com.example.notify.model.NotificationStatus;
import com.example.notify.mq.NotifyProducer;
import com.example.notify.repository.NotificationLogRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NotifyServiceTest {

    @Mock private SupplierConfigCache supplierConfigCache;
    @Mock private NotificationLogRepository logRepository;
    @Mock private NotifyProducer producer;
    @InjectMocks private NotifyService service;

    @Test
    void submitCreatesLogAndPublishes() {
        when(supplierConfigCache.exists("ad_system_a")).thenReturn(true);
        when(logRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        Map<String, Object> payload = Map.of("userId", "u123");
        String id = service.submit("ad_system_a", payload);

        assertThat(id).isNotBlank();

        ArgumentCaptor<NotificationLog> logCaptor = ArgumentCaptor.forClass(NotificationLog.class);
        verify(logRepository).save(logCaptor.capture());
        NotificationLog saved = logCaptor.getValue();
        assertThat(saved.getSupplierId()).isEqualTo("ad_system_a");
        assertThat(saved.getStatus()).isEqualTo(NotificationStatus.PENDING);

        verify(producer).publish(anyString(), eq(id), eq("ad_system_a"), any());
    }

    @Test
    void submitThrowsForUnknownSupplier() {
        when(supplierConfigCache.exists("unknown")).thenReturn(false);

        assertThatThrownBy(() -> service.submit("unknown", Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown");
    }
}
