# API 通知系统设计文档

**日期**：2026-04-26  
**作者**：崔凯  
**状态**：已确认，待实现

---

## 一、问题理解

企业内部多个业务系统（广告引流、CRM、库存等）在关键事件发生时，需要调用不同外部供应商的 HTTP API 发送通知。各供应商 API 的请求地址、Header、Body 格式各不相同。业务系统只关心通知"能被送达"，不关心外部 API 的返回值。

核心挑战：**如何在外部系统不稳定（超时、5xx、宕机）的情况下，仍然可靠地完成通知投递？**

---

## 二、整体架构

**选型：同步网关 + RocketMQ 异步投递**

```
业务系统 ──POST /notify──▶ Gateway API ──publish──▶ RocketMQ ──consume──▶ Dispatcher Worker ──HTTP──▶ 外部供应商 API
                                                                                    │
                                                                              失败不 ACK
                                                                                    │
                                                                         RocketMQ 自动重试（16次）
                                                                                    │
                                                                              超限 → DLQ → 告警
```

**技术栈**：Java 17 + Spring Boot 3 + RocketMQ + MySQL

### 组件职责

| 组件 | 职责 |
|------|------|
| Gateway API | 接收业务系统请求，验证参数，生成 notification_id，发布到 RocketMQ，返回 202 |
| Dispatcher Worker | 消费 RocketMQ 消息，查询供应商配置，组装并发送 HTTP 请求，ACK 或触发重试 |
| Supplier Config | DB 表存储供应商 URL、Header、Body 模板，应用启动时加载缓存 |
| Notification Log | DB 表记录每条通知的投递状态、尝试次数、失败原因，用于审计和排查 |

---

## 三、数据模型

### supplier_config（供应商配置）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | VARCHAR(64) | 供应商唯一标识，如 `ad_system_a` |
| name | VARCHAR(128) | 可读名称 |
| url | VARCHAR(512) | 目标 HTTP 地址 |
| headers | JSON | 固定请求头，如 `{"Authorization":"Bearer xxx"}` |
| body_template | TEXT | Body 模板，支持占位符，如 `{"uid":"${userId}"}` |
| timeout_ms | INT | HTTP 调用超时，默认 5000ms |

### notification_log（投递日志）

| 字段 | 类型 | 说明 |
|------|------|------|
| id | VARCHAR(64) | notification_id（UUID） |
| supplier_id | VARCHAR(64) | 目标供应商 |
| payload | JSON | 业务系统传入的原始数据 |
| status | ENUM | PENDING / DELIVERED / FAILED / DEAD |
| attempts | INT | 已尝试投递次数 |
| last_error | TEXT | 最近一次失败原因 |
| created_at | DATETIME | 创建时间 |
| updated_at | DATETIME | 最后更新时间 |

---

## 四、API 接口

### POST /api/v1/notify
业务系统调用，提交一条通知请求。

**Request Body：**
```json
{
  "supplier_id": "ad_system_a",
  "payload": {
    "userId": "u123",
    "event": "REGISTERED"
  }
}
```

**Response 202 Accepted：**
```json
{
  "notification_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Response 400**：supplier_id 不存在或参数格式错误。

### GET /api/v1/notify/{id}
查询单条通知的投递状态（运维排查用）。

**Response 200：**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "supplier_id": "ad_system_a",
  "status": "DELIVERED",
  "attempts": 1,
  "created_at": "2026-04-26T10:00:00Z"
}
```

### POST /api/v1/notify/{id}/retry
手动重新投递 DLQ 中的死信（运维接口，不对业务系统暴露）。

---

## 五、可靠性设计

### 投递语义：At-least-once

宁可重复投递，不可丢失。原因：外部系统（广告/CRM/库存）通常能设计幂等接口，而丢通知的业务损失更难接受和追溯。

### 重试策略

使用 RocketMQ 内置重试机制：
- 最大重试次数：16 次
- 重试间隔：指数退避（10s → 30s → 1min → 2min → ... → 2h），跨越约 4 小时
- Dispatcher 消费失败时不 ACK，RocketMQ 自动延迟重新投递

### 外部系统长期不可用的处理

1. RocketMQ 完成 16 次重试（约 4 小时内）
2. 超限后消息自动进入 DLQ（`%DLQ%notify`）
3. 监控告警触发，通知运维人员
4. 等供应商恢复后，通过 `/api/v1/notify/{id}/retry` 接口手动补发

---

## 六、系统边界

### 在系统内解决
- 接收业务系统的通知请求
- 异步可靠投递（at-least-once）
- 自动重试 + 指数退避
- 死信队列兜底 + 告警
- 投递日志记录（审计）
- 手动重试运维接口
- 多供应商配置管理

### 明确不解决

| 问题 | 不解决的原因 |
|------|------------|
| **幂等性** | 外部系统需自己保证；我们 at-least-once，可能重复投递，这是已知语义 |
| **请求鉴权** | 调用方身份认证由 API 网关层（如 Kong）统一处理，职责分离 |
| **消息顺序** | 不保证同一供应商消息有序；通知场景通常不依赖顺序 |
| **回调通知** | 不处理外部 API 的异步回调；超出系统定位 |
| **per-supplier 限流** | MVP 阶段通知量可控，过早优化 |

---

## 七、关键工程决策与取舍

### AI 建议中未采纳的方案

**1. RocketMQ 事务消息（本地消息表）**
- AI 建议：用事务消息保证"写 DB 和发 MQ"的原子性，彻底解决网关宕机丢消息。
- 未采纳原因：网关写入 MQ 失败概率极低，且业务系统可重试调用网关。事务消息引入额外 DB 表、事务回查接口，实现复杂度不匹配 MVP 价值。

**2. per-supplier 独立 Topic**
- AI 建议：每个供应商一个 RocketMQ Topic，实现完全隔离。
- 未采纳原因：增加运维复杂度（Topic 数量随供应商增长），MVP 阶段单 Topic + 并发消费足够。

**3. per-supplier 限流**
- AI 建议：对每个供应商设置调用频率上限，防止打垮外部系统。
- 未采纳原因：MVP 通知量有限，过早优化。

### 自己做出的关键决策

1. **选 RocketMQ 而非 Kafka**：Kafka 吞吐高但 DLQ、延迟重试需要自行实现；RocketMQ 原生支持，更贴合任务调度场景。
2. **供应商配置存 DB 而非配置文件**：运行时可动态新增/修改供应商，无需重启服务。
3. **通知日志独立于 MQ**：MQ 消息有 TTL，日志需要长期留存用于审计，两者职责不同。

---

## 八、未来演进路径

| 触发条件 | 演进方向 |
|---------|---------|
| 流量显著增长 | Dispatcher Worker 水平扩展，增加 RocketMQ 消费并发数 |
| 供应商数量增多 | 引入 per-supplier 限流（Resilience4j）+ 独立 Topic 隔离 |
| 可靠性要求提升 | 引入本地消息表 + 事务消息，消除网关单点故障 |
| 可观测性需求 | 接入 Prometheus + Grafana，监控投递成功率、延迟、DLQ 积压量 |
| 供应商配置频繁变更 | 配置中心（如 Nacos）+ 热更新，避免重启 |

---

## 九、AI 使用说明

### AI 提供了帮助的地方
- 快速梳理通知系统的常见失败模式和重试策略
- 提供 RocketMQ DLQ 机制的具体配置参数（重试次数、间隔策略）
- 生成 DB 表结构初稿和 API 接口定义

### AI 给出但未采纳的建议
- 事务消息（本地消息表）：复杂度过高，不适合 MVP
- per-supplier 独立 Topic：运维成本高，过早优化
- per-supplier 限流：MVP 阶段通知量可控，暂不需要

### 关键决策由自己做出
- 选择 RocketMQ 而非 Kafka：基于任务调度场景对延迟重试和 DLQ 的原生支持需求
- at-least-once 语义：基于业务损失权衡（丢通知 > 重复通知）
- 系统边界的划定：幂等性、鉴权、限流均不在本系统内解决，基于职责单一原则
