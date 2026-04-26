package com.example.notify.controller;

import com.example.notify.dto.NotifyRequest;
import com.example.notify.dto.NotifyResponse;
import com.example.notify.model.NotificationLog;
import com.example.notify.repository.NotificationLogRepository;
import com.example.notify.service.NotifyService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/notify")
public class NotifyController {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final NotifyService notifyService;
    private final NotificationLogRepository logRepository;

    public NotifyController(NotifyService notifyService,
                            NotificationLogRepository logRepository) {
        this.notifyService = notifyService;
        this.logRepository = logRepository;
    }

    @PostMapping
    public ResponseEntity<NotifyResponse> submit(@Valid @RequestBody NotifyRequest request) {
        String id = notifyService.submit(request.getSupplierId(), request.getPayload());
        return ResponseEntity.accepted().body(new NotifyResponse(id));
    }

    @GetMapping("/{id}")
    public ResponseEntity<NotificationLog> getStatus(@PathVariable String id) {
        return logRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{id}/retry")
    public ResponseEntity<Void> retry(@PathVariable String id) {
        NotificationLog log = logRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Notification not found: " + id));
        Map<String, Object> payload = MAPPER.convertValue(log.getPayload(),
                new TypeReference<Map<String, Object>>() {});
        notifyService.submit(log.getSupplierId(), payload);
        return ResponseEntity.accepted().build();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(e.getMessage());
    }
}
