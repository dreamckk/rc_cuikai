package com.example.notify.repository;

import com.example.notify.model.NotificationLog;
import com.example.notify.model.NotificationStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@ActiveProfiles("test")
class NotificationLogRepositoryTest {

    @Autowired
    private NotificationLogRepository repository;

    @Test
    void saveAndFindById() {
        NotificationLog log = new NotificationLog();
        log.setId("test-uuid-001");
        log.setSupplierId("ad_system_a");
        log.setPayload("{\"userId\":\"u123\"}");
        log.setStatus(NotificationStatus.PENDING);
        log.setAttempts(0);
        repository.save(log);

        Optional<NotificationLog> found = repository.findById("test-uuid-001");
        assertThat(found).isPresent();
        assertThat(found.get().getSupplierId()).isEqualTo("ad_system_a");
        assertThat(found.get().getStatus()).isEqualTo(NotificationStatus.PENDING);
    }

    @Test
    void updateStatus() {
        NotificationLog log = new NotificationLog();
        log.setId("test-uuid-002");
        log.setSupplierId("ad_system_a");
        log.setPayload("{\"userId\":\"u456\"}");
        log.setStatus(NotificationStatus.PENDING);
        log.setAttempts(0);
        repository.save(log);

        log.setStatus(NotificationStatus.DELIVERED);
        log.setAttempts(1);
        repository.save(log);

        NotificationLog updated = repository.findById("test-uuid-002").orElseThrow();
        assertThat(updated.getStatus()).isEqualTo(NotificationStatus.DELIVERED);
        assertThat(updated.getAttempts()).isEqualTo(1);
    }
}
