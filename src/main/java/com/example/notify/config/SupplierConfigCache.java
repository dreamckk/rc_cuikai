package com.example.notify.config;

import com.example.notify.model.SupplierConfig;
import com.example.notify.repository.SupplierConfigRepository;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SupplierConfigCache {

    private final SupplierConfigRepository repository;
    private final Map<String, SupplierConfig> cache = new ConcurrentHashMap<>();

    public SupplierConfigCache(SupplierConfigRepository repository) {
        this.repository = repository;
    }

    @PostConstruct
    public void load() {
        repository.findAll().forEach(c -> cache.put(c.getId(), c));
    }

    public Optional<SupplierConfig> get(String supplierId) {
        return Optional.ofNullable(cache.get(supplierId));
    }

    public boolean exists(String supplierId) {
        return cache.containsKey(supplierId);
    }
}
