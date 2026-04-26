package com.example.notify.model;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "notification_log")
public class NotificationLog {

    @Id
    private String id;

    @Column(name = "supplier_id")
    private String supplierId;

    @Column(columnDefinition = "JSON")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "VARCHAR(20)")
    private NotificationStatus status;

    private int attempts;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    void prePersist() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
