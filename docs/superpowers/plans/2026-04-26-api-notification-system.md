# API 通知系统 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现一个内部通知投递服务，接收业务系统的 HTTP 通知请求，通过 RocketMQ 异步可靠地投递到外部供应商 API。

**Architecture:** Gateway API 接收请求并写入 RocketMQ（返回 202），Dispatcher Worker 消费消息并调用外部 HTTP API，失败时不 ACK 触发 RocketMQ 内置重试（16次），超限进 DLQ。供应商配置存 MySQL，启动时缓存。每次投递写 notification_log 审计。

**Tech Stack:** Java 17, Spring Boot 3, RocketMQ 5.x (rocketmq-spring-boot-starter), MySQL 8, Flyway, OkHttp3, JUnit 5, Mockito, H2（测试用）

---

## File Structure

```
src/main/java/com/example/notify/
├── NotifyApplication.java
├── controller/NotifyController.java
├── service/NotifyService.java
├── service/DispatcherService.java
├── mq/NotifyProducer.java
├── mq/NotifyConsumer.java
├── config/SupplierConfigCache.java
├── http/ExternalApiClient.java
├── repository/SupplierConfigRepository.java
├── repository/NotificationLogRepository.java
├── model/SupplierConfig.java
├── model/NotificationLog.java
├── model/NotificationStatus.java
└── dto/NotifyRequest.java, NotifyResponse.java

src/main/resources/
├── application.yml
└── db/migration/V1__init.sql

src/test/java/com/example/notify/
├── controller/NotifyControllerTest.java
├── service/NotifyServiceTest.java
├── service/DispatcherServiceTest.java
├── config/SupplierConfigCacheTest.java
└── http/ExternalApiClientTest.java
```

---

## Task 1: 初始化 Spring Boot 项目

**Files:**
- Create: `pom.xml`
- Create: `src/main/java/com/example/notify/NotifyApplication.java`
- Create: `src/main/resources/application.yml`
- Create: `src/main/resources/db/migration/V1__init.sql`

- [ ] **Step 1: 生成 pom.xml**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>notify</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <packaging>jar</packaging>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.2.4</version>
    </parent>

    <properties>
        <java.version>17</java.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>org.apache.rocketmq</groupId>
            <artifactId>rocketmq-spring-boot-starter</artifactId>
            <version>2.3.0</version>
        </dependency>
        <dependency>
            <groupId>com.squareup.okhttp3</groupId>
            <artifactId>okhttp</artifactId>
            <version>4.12.0</version>
        </dependency>
        <dependency>
            <groupId>com.mysql</groupId>
            <artifactId>mysql-connector-j</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-mysql</artifactId>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <optional>true</optional>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.squareup.okhttp3</groupId>
            <artifactId>mockwebserver</artifactId>
            <version>4.12.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: 创建 NotifyApplication.java**

```java
package com.example.notify;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class NotifyApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotifyApplication.class, args);
    }
}
```

- [ ] **Step 3: 创建 application.yml**

```yaml
spring:
  datasource:
    url: jdbc:mysql://localhost:3306/notify_db?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
    username: root
    password: root
    driver-class-name: com.mysql.cj.jdbc.Driver
  jpa:
    hibernate:
      ddl-auto: validate
    show-sql: false
  flyway:
    enabled: true
    locations: classpath:db/migration

rocketmq:
  name-server: localhost:9876
  producer:
    group: notify-producer-group
    send-message-timeout: 3000
  consumer:
    group: notify-consumer-group

notify:
  topic: notify-topic
  max-retry: 16
  http:
    connect-timeout-ms: 3000
    read-timeout-ms: 5000

---
spring:
  config:
    activate:
      on-profile: test
  datasource:
    url: jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=MySQL
    driver-class-name: org.h2.Driver
    username: sa
    password:
  jpa:
    hibernate:
      ddl-auto: create-drop
    database-platform: org.hibernate.dialect.H2Dialect
  flyway:
    enabled: false
```

- [ ] **Step 4: 创建 V1__init.sql**

```sql
CREATE TABLE supplier_config (
    id          VARCHAR(64)  NOT NULL PRIMARY KEY,
    name        VARCHAR(128) NOT NULL,
    url         VARCHAR(512) NOT NULL,
    headers     JSON         NOT NULL DEFAULT ('{}'),
    body_template TEXT        NOT NULL DEFAULT ('{}'),
    timeout_ms  INT          NOT NULL DEFAULT 5000,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE notification_log (
    id          VARCHAR(64)  NOT NULL PRIMARY KEY,
    supplier_id VARCHAR(64)  NOT NULL,
    payload     JSON         NOT NULL,
    status      ENUM('PENDING','DELIVERED','FAILED','DEAD') NOT NULL DEFAULT 'PENDING',
    attempts    INT          NOT NULL DEFAULT 0,
    last_error  TEXT,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_supplier_id (supplier_id),
    INDEX idx_status (status)
);

INSERT INTO supplier_config (id, name, url, headers, body_template, timeout_ms) VALUES
('ad_system_a', '广告系统A', 'https://httpbin.org/post',
 '{"Content-Type":"application/json","X-Api-Key":"test-key"}',
 '{"uid":"${userId}","event":"${event}"}',
 5000);
```

- [ ] **Step 5: 验证项目结构可编译**

```bash
mvn compile -q
```

Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
git init
git add pom.xml src/main/java/com/example/notify/NotifyApplication.java \
        src/main/resources/application.yml src/main/resources/db/migration/V1__init.sql
git commit -m "feat: initialize Spring Boot project with RocketMQ, MySQL, Flyway"
```

---

## Task 2: 数据模型与 Repository

**Files:**
- Create: `src/main/java/com/example/notify/model/NotificationStatus.java`
- Create: `src/main/java/com/example/notify/model/SupplierConfig.java`
- Create: `src/main/java/com/example/notify/model/NotificationLog.java`
- Create: `src/main/java/com/example/notify/repository/SupplierConfigRepository.java`
- Create: `src/main/java/com/example/notify/repository/NotificationLogRepository.java`
- Create: `src/test/java/com/example/notify/repository/NotificationLogRepositoryTest.java`

- [ ] **Step 1: 写失败测试**

新建 `src/test/java/com/example/notify/repository/NotificationLogRepositoryTest.java`：

```java
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
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn test -pl . -Dtest=NotificationLogRepositoryTest -q 2>&1 | tail -5
```

Expected: FAIL — 类不存在

- [ ] **Step 3: 创建 NotificationStatus.java**

```java
package com.example.notify.model;

public enum NotificationStatus {
    PENDING, DELIVERED, FAILED, DEAD
}
```

- [ ] **Step 4: 创建 SupplierConfig.java**

```java
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
```

- [ ] **Step 5: 创建 NotificationLog.java**

```java
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
```

- [ ] **Step 6: 创建 SupplierConfigRepository.java**

```java
package com.example.notify.repository;

import com.example.notify.model.SupplierConfig;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SupplierConfigRepository extends JpaRepository<SupplierConfig, String> {
}
```

- [ ] **Step 7: 创建 NotificationLogRepository.java**

```java
package com.example.notify.repository;

import com.example.notify.model.NotificationLog;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NotificationLogRepository extends JpaRepository<NotificationLog, String> {
}
```

- [ ] **Step 8: 运行测试，确认通过**

```bash
mvn test -Dtest=NotificationLogRepositoryTest -q 2>&1 | tail -5
```

Expected: BUILD SUCCESS, Tests run: 2, Failures: 0

- [ ] **Step 9: Commit**

```bash
git add src/main/java/com/example/notify/model/ \
        src/main/java/com/example/notify/repository/ \
        src/test/java/com/example/notify/repository/
git commit -m "feat: add JPA entities and repositories for supplier_config and notification_log"
```

---

## Task 3: 供应商配置缓存

**Files:**
- Create: `src/main/java/com/example/notify/config/SupplierConfigCache.java`
- Create: `src/test/java/com/example/notify/config/SupplierConfigCacheTest.java`

- [ ] **Step 1: 写失败测试**

```java
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
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn test -Dtest=SupplierConfigCacheTest -q 2>&1 | tail -5
```

Expected: FAIL — SupplierConfigCache 不存在

- [ ] **Step 3: 实现 SupplierConfigCache.java**

```java
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
```

- [ ] **Step 4: 运行测试，确认通过**

```bash
mvn test -Dtest=SupplierConfigCacheTest -q 2>&1 | tail -5
```

Expected: BUILD SUCCESS, Tests run: 4, Failures: 0

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/example/notify/config/SupplierConfigCache.java \
        src/test/java/com/example/notify/config/SupplierConfigCacheTest.java
git commit -m "feat: add SupplierConfigCache with PostConstruct load"
```

---

## Task 4: DTO 与 ExternalApiClient

**Files:**
- Create: `src/main/java/com/example/notify/dto/NotifyRequest.java`
- Create: `src/main/java/com/example/notify/dto/NotifyResponse.java`
- Create: `src/main/java/com/example/notify/http/ExternalApiClient.java`
- Create: `src/test/java/com/example/notify/http/ExternalApiClientTest.java`

- [ ] **Step 1: 创建 DTO**

`NotifyRequest.java`:

```java
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
```

`NotifyResponse.java`:

```java
package com.example.notify.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class NotifyResponse {
    private String notificationId;
}
```

- [ ] **Step 2: 写失败测试（ExternalApiClient）**

```java
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
```

- [ ] **Step 3: 运行测试，确认失败**

```bash
mvn test -Dtest=ExternalApiClientTest -q 2>&1 | tail -5
```

Expected: FAIL — ExternalApiClient 不存在

- [ ] **Step 4: 实现 ExternalApiClient.java**

```java
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
```

- [ ] **Step 5: 运行测试，确认通过**

```bash
mvn test -Dtest=ExternalApiClientTest -q 2>&1 | tail -5
```

Expected: BUILD SUCCESS, Tests run: 2, Failures: 0

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/example/notify/dto/ \
        src/main/java/com/example/notify/http/ExternalApiClient.java \
        src/test/java/com/example/notify/http/ExternalApiClientTest.java
git commit -m "feat: add DTOs and ExternalApiClient with OkHttp and body template rendering"
```

---

## Task 5: RocketMQ Producer 与 NotifyService

**Files:**
- Create: `src/main/java/com/example/notify/mq/NotifyProducer.java`
- Create: `src/main/java/com/example/notify/service/NotifyService.java`
- Create: `src/test/java/com/example/notify/service/NotifyServiceTest.java`

- [ ] **Step 1: 写失败测试**

```java
package com.example.notify.service;

import com.example.notify.config.SupplierConfigCache;
import com.example.notify.model.NotificationLog;
import com.example.notify.model.NotificationStatus;
import com.example.notify.mq.NotifyProducer;
import com.example.notify.repository.NotificationLogRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NotifyServiceTest {

    @Mock private SupplierConfigCache supplierConfigCache;
    @Mock private NotificationLogRepository logRepository;
    @Mock private NotifyProducer producer;
    @InjectMocks private NotifyService service;

    @Test
    void submitCreatesLogAndPublishes() {
        when(supplierConfigCache.exists("ad_system_a")).thenReturn(true);
        when(logRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        Map<String, Object> payload = Map.of("userId", "u123");
        String id = service.submit("ad_system_a", payload);

        assertThat(id).isNotBlank();

        ArgumentCaptor<NotificationLog> logCaptor = ArgumentCaptor.forClass(NotificationLog.class);
        verify(logRepository).save(logCaptor.capture());
        NotificationLog saved = logCaptor.getValue();
        assertThat(saved.getSupplierId()).isEqualTo("ad_system_a");
        assertThat(saved.getStatus()).isEqualTo(NotificationStatus.PENDING);

        verify(producer).publish(anyString(), eq(id), eq("ad_system_a"), any());
    }

    @Test
    void submitThrowsForUnknownSupplier() {
        when(supplierConfigCache.exists("unknown")).thenReturn(false);

        assertThatThrownBy(() -> service.submit("unknown", Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown");
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn test -Dtest=NotifyServiceTest -q 2>&1 | tail -5
```

Expected: FAIL

- [ ] **Step 3: 创建 NotifyProducer.java**

```java
package com.example.notify.mq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class NotifyProducer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RocketMQTemplate rocketMQTemplate;
    private final String topic;

    public NotifyProducer(RocketMQTemplate rocketMQTemplate,
                          @Value("${notify.topic}") String topic) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.topic = topic;
    }

    public void publish(String notificationId, String notificationLogId, String supplierId, Map<String, Object> payload) {
        Map<String, Object> message = new HashMap<>();
        message.put("notificationId", notificationId);
        message.put("supplierId", supplierId);
        message.put("payload", payload);

        try {
            String json = MAPPER.writeValueAsString(message);
            rocketMQTemplate.send(topic, MessageBuilder.withPayload(json)
                    .setHeader("notificationId", notificationId)
                    .build());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize notification message", e);
        }
    }
}
```

- [ ] **Step 4: 创建 NotifyService.java**

```java
package com.example.notify.service;

import com.example.notify.config.SupplierConfigCache;
import com.example.notify.model.NotificationLog;
import com.example.notify.model.NotificationStatus;
import com.example.notify.mq.NotifyProducer;
import com.example.notify.repository.NotificationLogRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;

@Service
public class NotifyService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SupplierConfigCache supplierConfigCache;
    private final NotificationLogRepository logRepository;
    private final NotifyProducer producer;

    public NotifyService(SupplierConfigCache supplierConfigCache,
                         NotificationLogRepository logRepository,
                         NotifyProducer producer) {
        this.supplierConfigCache = supplierConfigCache;
        this.logRepository = logRepository;
        this.producer = producer;
    }

    public String submit(String supplierId, Map<String, Object> payload) {
        if (!supplierConfigCache.exists(supplierId)) {
            throw new IllegalArgumentException("Unknown supplier: " + supplierId);
        }

        String id = UUID.randomUUID().toString();

        NotificationLog log = new NotificationLog();
        log.setId(id);
        log.setSupplierId(supplierId);
        log.setPayload(toJson(payload));
        log.setStatus(NotificationStatus.PENDING);
        log.setAttempts(0);
        logRepository.save(log);

        producer.publish(id, id, supplierId, payload);
        return id;
    }

    private String toJson(Map<String, Object> payload) {
        try {
            return MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize payload", e);
        }
    }
}
```

- [ ] **Step 5: 运行测试，确认通过**

```bash
mvn test -Dtest=NotifyServiceTest -q 2>&1 | tail -5
```

Expected: BUILD SUCCESS, Tests run: 2, Failures: 0

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/example/notify/mq/NotifyProducer.java \
        src/main/java/com/example/notify/service/NotifyService.java \
        src/test/java/com/example/notify/service/NotifyServiceTest.java
git commit -m "feat: add NotifyService and NotifyProducer for receiving and queuing notifications"
```

---

## Task 6: RocketMQ Consumer 与 DispatcherService

**Files:**
- Create: `src/main/java/com/example/notify/service/DispatcherService.java`
- Create: `src/main/java/com/example/notify/mq/NotifyConsumer.java`
- Create: `src/test/java/com/example/notify/service/DispatcherServiceTest.java`

- [ ] **Step 1: 写失败测试**

```java
package com.example.notify.service;

import com.example.notify.config.SupplierConfigCache;
import com.example.notify.http.ExternalApiClient;
import com.example.notify.model.NotificationLog;
import com.example.notify.model.NotificationStatus;
import com.example.notify.model.SupplierConfig;
import com.example.notify.repository.NotificationLogRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DispatcherServiceTest {

    @Mock private SupplierConfigCache supplierConfigCache;
    @Mock private ExternalApiClient apiClient;
    @Mock private NotificationLogRepository logRepository;
    @InjectMocks private DispatcherService service;

    private SupplierConfig buildSupplier() {
        SupplierConfig config = new SupplierConfig();
        config.setId("ad_system_a");
        config.setUrl("https://example.com/notify");
        config.setHeaders("{}");
        config.setBodyTemplate("{\"uid\":\"${userId}\"}");
        config.setTimeoutMs(5000);
        return config;
    }

    @Test
    void dispatchSuccessUpdatesStatusToDelivered() {
        SupplierConfig config = buildSupplier();
        when(supplierConfigCache.get("ad_system_a")).thenReturn(Optional.of(config));

        NotificationLog log = new NotificationLog();
        log.setId("notif-001");
        log.setSupplierId("ad_system_a");
        log.setStatus(NotificationStatus.PENDING);
        log.setAttempts(0);
        when(logRepository.findById("notif-001")).thenReturn(Optional.of(log));

        doNothing().when(apiClient).send(any(), any());

        service.dispatch("notif-001", "ad_system_a", Map.of("userId", "u123"));

        ArgumentCaptor<NotificationLog> captor = ArgumentCaptor.forClass(NotificationLog.class);
        verify(logRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(NotificationStatus.DELIVERED);
        assertThat(captor.getValue().getAttempts()).isEqualTo(1);
    }

    @Test
    void dispatchFailureThrowsAndUpdatesStatusToFailed() {
        SupplierConfig config = buildSupplier();
        when(supplierConfigCache.get("ad_system_a")).thenReturn(Optional.of(config));

        NotificationLog log = new NotificationLog();
        log.setId("notif-002");
        log.setSupplierId("ad_system_a");
        log.setStatus(NotificationStatus.PENDING);
        log.setAttempts(0);
        when(logRepository.findById("notif-002")).thenReturn(Optional.of(log));

        doThrow(new RuntimeException("500 Internal Server Error"))
                .when(apiClient).send(eq(config), any());

        assertThatThrownBy(() -> service.dispatch("notif-002", "ad_system_a", Map.of("userId", "u456")))
                .isInstanceOf(RuntimeException.class);

        ArgumentCaptor<NotificationLog> captor = ArgumentCaptor.forClass(NotificationLog.class);
        verify(logRepository).save(captor.capture());
        assertThat(captor.getValue().getStatus()).isEqualTo(NotificationStatus.FAILED);
        assertThat(captor.getValue().getLastError()).contains("500");
    }
}
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn test -Dtest=DispatcherServiceTest -q 2>&1 | tail -5
```

Expected: FAIL

- [ ] **Step 3: 实现 DispatcherService.java**

```java
package com.example.notify.service;

import com.example.notify.config.SupplierConfigCache;
import com.example.notify.http.ExternalApiClient;
import com.example.notify.model.NotificationLog;
import com.example.notify.model.NotificationStatus;
import com.example.notify.model.SupplierConfig;
import com.example.notify.repository.NotificationLogRepository;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class DispatcherService {

    private final SupplierConfigCache supplierConfigCache;
    private final ExternalApiClient apiClient;
    private final NotificationLogRepository logRepository;

    public DispatcherService(SupplierConfigCache supplierConfigCache,
                             ExternalApiClient apiClient,
                             NotificationLogRepository logRepository) {
        this.supplierConfigCache = supplierConfigCache;
        this.apiClient = apiClient;
        this.logRepository = logRepository;
    }

    public void dispatch(String notificationId, String supplierId, Map<String, Object> payload) {
        SupplierConfig config = supplierConfigCache.get(supplierId)
                .orElseThrow(() -> new IllegalStateException("Supplier config not found: " + supplierId));

        NotificationLog log = logRepository.findById(notificationId)
                .orElseThrow(() -> new IllegalStateException("Notification log not found: " + notificationId));

        try {
            apiClient.send(config, payload);
            log.setStatus(NotificationStatus.DELIVERED);
            log.setAttempts(log.getAttempts() + 1);
            logRepository.save(log);
        } catch (Exception e) {
            log.setStatus(NotificationStatus.FAILED);
            log.setAttempts(log.getAttempts() + 1);
            log.setLastError(e.getMessage());
            logRepository.save(log);
            throw e;
        }
    }
}
```

- [ ] **Step 4: 创建 NotifyConsumer.java**

```java
package com.example.notify.mq;

import com.example.notify.service.DispatcherService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@RocketMQMessageListener(
        topic = "${notify.topic}",
        consumerGroup = "${rocketmq.consumer.group}",
        maxReconsumeTimes = 16
)
public class NotifyConsumer implements RocketMQListener<String> {

    private static final Logger log = LoggerFactory.getLogger(NotifyConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DispatcherService dispatcherService;

    public NotifyConsumer(DispatcherService dispatcherService) {
        this.dispatcherService = dispatcherService;
    }

    @Override
    public void onMessage(String messageJson) {
        try {
            Map<String, Object> message = MAPPER.readValue(messageJson,
                    new TypeReference<Map<String, Object>>() {});

            String notificationId = (String) message.get("notificationId");
            String supplierId = (String) message.get("supplierId");
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) message.get("payload");

            log.info("Dispatching notification {} to supplier {}", notificationId, supplierId);
            dispatcherService.dispatch(notificationId, supplierId, payload);
            log.info("Notification {} delivered successfully", notificationId);

        } catch (Exception e) {
            log.error("Failed to process notification message: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
```

- [ ] **Step 5: 运行测试，确认通过**

```bash
mvn test -Dtest=DispatcherServiceTest -q 2>&1 | tail -5
```

Expected: BUILD SUCCESS, Tests run: 2, Failures: 0

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/example/notify/service/DispatcherService.java \
        src/main/java/com/example/notify/mq/NotifyConsumer.java \
        src/test/java/com/example/notify/service/DispatcherServiceTest.java
git commit -m "feat: add DispatcherService and NotifyConsumer for reliable delivery with retry"
```

---

## Task 7: REST Controller

**Files:**
- Create: `src/main/java/com/example/notify/controller/NotifyController.java`
- Create: `src/test/java/com/example/notify/controller/NotifyControllerTest.java`

- [ ] **Step 1: 写失败测试**

```java
package com.example.notify.controller;

import com.example.notify.service.NotifyService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(NotifyController.class)
class NotifyControllerTest {

    @Autowired private MockMvc mockMvc;
    @MockBean private NotifyService notifyService;
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
```

- [ ] **Step 2: 运行测试，确认失败**

```bash
mvn test -Dtest=NotifyControllerTest -q 2>&1 | tail -5
```

Expected: FAIL

- [ ] **Step 3: 实现 NotifyController.java**

```java
package com.example.notify.controller;

import com.example.notify.dto.NotifyRequest;
import com.example.notify.dto.NotifyResponse;
import com.example.notify.model.NotificationLog;
import com.example.notify.repository.NotificationLogRepository;
import com.example.notify.service.NotifyService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/notify")
public class NotifyController {

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
        notifyService.submit(log.getSupplierId(),
                new com.fasterxml.jackson.databind.ObjectMapper()
                        .convertValue(log.getPayload(), new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String, Object>>() {}));
        return ResponseEntity.accepted().build();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(e.getMessage());
    }
}
```

- [ ] **Step 4: 在 application.yml 中启用 Bean Validation**

在 `pom.xml` 的 `<dependencies>` 中追加（Bean Validation）：

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-validation</artifactId>
</dependency>
```

- [ ] **Step 5: 运行测试，确认通过**

```bash
mvn test -Dtest=NotifyControllerTest -q 2>&1 | tail -5
```

Expected: BUILD SUCCESS, Tests run: 3, Failures: 0

- [ ] **Step 6: 运行全部测试**

```bash
mvn test -q 2>&1 | tail -10
```

Expected: BUILD SUCCESS，所有测试通过

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/example/notify/controller/NotifyController.java \
        src/test/java/com/example/notify/controller/NotifyControllerTest.java \
        pom.xml
git commit -m "feat: add REST controller with POST /notify, GET /notify/{id}, POST /notify/{id}/retry"
```

---

## Task 8: README 与 AI 使用说明

**Files:**
- Create: `README.md`

- [ ] **Step 1: 创建 README.md**

内容包含：
1. 项目简介
2. 快速启动（本地运行 MySQL + RocketMQ 的 docker-compose 命令，以及 mvn spring-boot:run）
3. 设计文档指向（链接 `docs/superpowers/specs/2026-04-26-api-notification-system-design.md`）
4. AI 使用说明（从设计文档第九节复制）

```markdown
# API 通知系统

企业内部通知投递服务：接收业务系统的 HTTP 通知请求，通过 RocketMQ 异步可靠地投递到外部供应商 API。

## 快速启动

**1. 启动依赖（MySQL + RocketMQ）：**

```bash
docker run -d --name mysql \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=notify_db \
  -p 3306:3306 mysql:8.0

docker run -d --name rocketmq-namesrv \
  -p 9876:9876 apache/rocketmq:5.1.4 sh mqnamesrv

docker run -d --name rocketmq-broker \
  --link rocketmq-namesrv \
  -e "NAMESRV_ADDR=rocketmq-namesrv:9876" \
  -p 10911:10911 -p 10909:10909 \
  apache/rocketmq:5.1.4 sh mqbroker
```

**2. 启动服务：**

```bash
mvn spring-boot:run
```

**3. 发送通知：**

```bash
curl -X POST http://localhost:8080/api/v1/notify \
  -H "Content-Type: application/json" \
  -d '{"supplierId":"ad_system_a","payload":{"userId":"u123","event":"REGISTERED"}}'
```

## 设计文档

见 [docs/superpowers/specs/2026-04-26-api-notification-system-design.md](docs/superpowers/specs/2026-04-26-api-notification-system-design.md)

## AI 使用说明

### AI 提供了帮助的地方
- 快速梳理通知系统的常见失败模式和重试策略
- 提供 RocketMQ DLQ 机制的具体配置参数（重试次数、间隔策略）
- 生成 DB 表结构初稿和 API 接口定义

### AI 给出但未采纳的建议
- **事务消息（本地消息表）**：复杂度过高，不适合 MVP。网关写入 MQ 失败概率极低，业务系统可重试。
- **per-supplier 独立 Topic**：运维成本高，过早优化。
- **per-supplier 限流**：MVP 阶段通知量可控，暂不需要。

### 关键决策由自己做出
- 选择 RocketMQ 而非 Kafka：基于任务调度场景对延迟重试和 DLQ 的原生支持需求
- At-least-once 语义：基于业务损失权衡（丢通知 > 重复通知）
- 系统边界的划定：幂等性、鉴权、限流均不在本系统内解决，基于职责单一原则
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add README with quick start and AI usage notes"
```

---

## Self-Review Checklist

- [x] **Spec coverage**: 架构（Task 1）、数据模型（Task 2）、供应商配置（Task 3）、ExternalApiClient（Task 4）、NotifyService + Producer（Task 5）、DispatcherService + Consumer（Task 6）、REST API（Task 7）、README + AI 说明（Task 8）均已覆盖
- [x] **Placeholder scan**: 无 TBD/TODO，所有步骤均含完整代码
- [x] **Type consistency**: `NotifyService.submit()` 在 Task 5 定义，Task 7 中一致使用；`DispatcherService.dispatch()` 在 Task 6 定义，Consumer 一致调用；`SupplierConfigCache.get()`/`exists()` 在 Task 3 定义，Task 5/6 一致使用
- [x] **Scope check**: 8 个 Task，单次实现计划足够
