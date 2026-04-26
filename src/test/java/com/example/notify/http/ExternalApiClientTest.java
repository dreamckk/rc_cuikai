package com.example.notify.http;

import com.example.notify.model.SupplierConfig;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExternalApiClientTest {

    private MockWebServer server;
    private ExternalApiClient client;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        client = new ExternalApiClient(3000, 5000);
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    private SupplierConfig buildConfig(String bodyTemplate) {
        SupplierConfig config = new SupplierConfig();
        config.setId("test_supplier");
        config.setUrl(server.url("/notify").toString());
        config.setHeaders("{\"X-Api-Key\":\"secret\",\"Content-Type\":\"application/json\"}");
        config.setBodyTemplate(bodyTemplate);
        config.setTimeoutMs(3000);
        return config;
    }

    @Test
    void sendsRequestWithRenderedBody() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));

        SupplierConfig config = buildConfig("{\"uid\":\"${userId}\",\"event\":\"${event}\"}");
        Map<String, Object> payload = Map.of("userId", "u123", "event", "REGISTERED");

        client.send(config, payload);

        RecordedRequest request = server.takeRequest();
        assertThat(request.getHeader("X-Api-Key")).isEqualTo("secret");
        assertThat(request.getBody().readUtf8()).contains("u123").contains("REGISTERED");
    }

    @Test
    void throwsOnNon2xxResponse() {
        server.enqueue(new MockResponse().setResponseCode(500).setBody("error"));

        SupplierConfig config = buildConfig("{\"uid\":\"${userId}\"}");
        Map<String, Object> payload = Map.of("userId", "u123");

        assertThatThrownBy(() -> client.send(config, payload))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("500");
    }
}
