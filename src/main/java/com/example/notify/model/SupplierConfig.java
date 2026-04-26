package com.example.notify.model;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "supplier_config")
public class SupplierConfig {

    @Id
    private String id;

    private String name;
    private String url;

    @Column(columnDefinition = "JSON")
    private String headers;

    @Column(name = "body_template", columnDefinition = "TEXT")
    private String bodyTemplate;

    @Column(name = "timeout_ms")
    private int timeoutMs = 5000;

    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
}
