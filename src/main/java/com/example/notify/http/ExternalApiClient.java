package com.example.notify.http;

import com.example.notify.model.SupplierConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class ExternalApiClient {

    private static final MediaType JSON = MediaType.get("application/json");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final OkHttpClient httpClient;

    public ExternalApiClient(
            @Value("${notify.http.connect-timeout-ms:3000}") int connectTimeoutMs,
            @Value("${notify.http.read-timeout-ms:5000}") int readTimeoutMs) {
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                .readTimeout(readTimeoutMs, TimeUnit.MILLISECONDS)
                .build();
    }

    public void send(SupplierConfig config, Map<String, Object> payload) {
        String body = renderBody(config.getBodyTemplate(), payload);

        Request.Builder requestBuilder = new Request.Builder()
                .url(config.getUrl())
                .post(RequestBody.create(body, JSON));

        parseHeaders(config.getHeaders()).forEach(requestBuilder::addHeader);

        try (Response response = httpClient.newCall(requestBuilder.build()).execute()) {
            if (!response.isSuccessful()) {
                throw new RuntimeException("External API returned " + response.code() + " for supplier " + config.getId());
            }
        } catch (IOException e) {
            throw new RuntimeException("HTTP call failed for supplier " + config.getId() + ": " + e.getMessage(), e);
        }
    }

    private String renderBody(String template, Map<String, Object> payload) {
        String result = template;
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            result = result.replace("${" + entry.getKey() + "}", String.valueOf(entry.getValue()));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> parseHeaders(String headersJson) {
        try {
            return MAPPER.readValue(headersJson, Map.class);
        } catch (IOException e) {
            return Map.of();
        }
    }
}
