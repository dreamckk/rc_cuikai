package com.example.notify.controller;

import com.example.notify.repository.NotificationLogRepository;
import com.example.notify.service.NotifyService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(NotifyController.class)
class NotifyControllerTest {

    @Autowired private MockMvc mockMvc;
    @MockBean private NotifyService notifyService;
    @MockBean private NotificationLogRepository logRepository;
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void postNotifyReturns202WithNotificationId() throws Exception {
        when(notifyService.submit(eq("ad_system_a"), any())).thenReturn("uuid-001");

        Map<String, Object> request = Map.of(
                "supplierId", "ad_system_a",
                "payload", Map.of("userId", "u123", "event", "REGISTERED")
        );

        mockMvc.perform(post("/api/v1/notify")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.notificationId").value("uuid-001"));
    }

    @Test
    void postNotifyReturns400ForUnknownSupplier() throws Exception {
        doThrow(new IllegalArgumentException("Unknown supplier: bad_supplier"))
                .when(notifyService).submit(eq("bad_supplier"), any());

        Map<String, Object> request = Map.of(
                "supplierId", "bad_supplier",
                "payload", Map.of("userId", "u123")
        );

        mockMvc.perform(post("/api/v1/notify")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void postNotifyReturns400ForMissingSupplierId() throws Exception {
        Map<String, Object> request = Map.of("payload", Map.of("userId", "u123"));

        mockMvc.perform(post("/api/v1/notify")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }
}
