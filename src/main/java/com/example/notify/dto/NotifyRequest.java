package com.example.notify.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import java.util.Map;

@Data
public class NotifyRequest {

    @NotBlank(message = "supplier_id is required")
    private String supplierId;

    @NotNull(message = "payload is required")
    private Map<String, Object> payload;
}
