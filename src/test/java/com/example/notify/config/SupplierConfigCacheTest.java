package com.example.notify.config;

import com.example.notify.model.SupplierConfig;
import com.example.notify.repository.SupplierConfigRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SupplierConfigCacheTest {

    @Mock
    private SupplierConfigRepository repository;

    private SupplierConfigCache cache;

    @BeforeEach
    void setUp() {
        SupplierConfig config = new SupplierConfig();
        config.setId("ad_system_a");
        config.setName("广告系统A");
        config.setUrl("https://example.com/notify");
        config.setHeaders("{\"X-Api-Key\":\"key\"}");
        config.setBodyTemplate("{\"uid\":\"${userId}\"}");
        config.setTimeoutMs(5000);

        when(repository.findAll()).thenReturn(List.of(config));
        cache = new SupplierConfigCache(repository);
        cache.load();
    }

    @Test
    void getExistingSupplier() {
        Optional<SupplierConfig> result = cache.get("ad_system_a");
        assertThat(result).isPresent();
        assertThat(result.get().getUrl()).isEqualTo("https://example.com/notify");
    }

    @Test
    void getMissingSupplier() {
        Optional<SupplierConfig> result = cache.get("unknown");
        assertThat(result).isEmpty();
    }

    @Test
    void existsReturnsTrueForKnownSupplier() {
        assertThat(cache.exists("ad_system_a")).isTrue();
    }

    @Test
    void existsReturnsFalseForUnknownSupplier() {
        assertThat(cache.exists("unknown")).isFalse();
    }
}
