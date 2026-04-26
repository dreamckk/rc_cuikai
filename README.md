# API 通知系统

企业内部通知投递服务：接收业务系统的 HTTP 通知请求，通过 RocketMQ 异步可靠地投递到外部供应商 API。

## 架构概览

```
业务系统 ──POST /notify──▶ Gateway API ──publish──▶ RocketMQ ──consume──▶ Dispatcher Worker ──HTTP──▶ 外部供应商 API
                                                                                  │
                                                                         失败不 ACK → 自动重试（16次）
                                                                                  │
                                                                           超限 → DLQ → 告警
```

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
# Response: {"notificationId":"<uuid>"}
```

**4. 查询投递状态：**

```bash
curl http://localhost:8080/api/v1/notify/<notificationId>
```

**5. 手动重试（DLQ 死信补发）：**

```bash
curl -X POST http://localhost:8080/api/v1/notify/<notificationId>/retry
```

## 运行测试

```bash
mvn test
```

全套测试使用 H2 内存数据库，无需启动 MySQL 或 RocketMQ。

## 设计文档

见 [docs/superpowers/specs/2026-04-26-api-notification-system-design.md](docs/superpowers/specs/2026-04-26-api-notification-system-design.md)

## AI 使用说明

### AI 提供了帮助的地方
- 快速梳理通知系统的常见失败模式和重试策略
- 提供 RocketMQ DLQ 机制的具体配置参数（重试次数、间隔策略）
- 生成 DB 表结构初稿、API 接口定义和单元测试代码

### AI 给出但未采纳的建议
- **事务消息（本地消息表）**：复杂度过高，不适合 MVP。网关写入 MQ 失败概率极低，业务系统可重试调用网关。
- **per-supplier 独立 Topic**：运维成本高，过早优化。单 Topic + 并发消费已满足需求。
- **per-supplier 限流**：MVP 阶段通知量可控，暂不需要。

### 关键决策由自己做出
- **选择 RocketMQ 而非 Kafka**：Kafka 吞吐高但 DLQ 和延迟重试需要自行实现；RocketMQ 原生支持，更贴合任务调度场景。
- **At-least-once 语义**：基于业务损失权衡——丢通知的损失远大于重复投递的代价，外部系统通常能设计幂等接口。
- **系统边界的划定**：幂等性、鉴权、限流均不在本系统内解决，职责单一，各层各司其职。
- **供应商配置存 DB 而非配置文件**：运行时可动态新增/修改供应商，无需重启服务。
- **通知日志独立于 MQ**：MQ 消息有 TTL，日志需要长期留存用于审计，两者职责不同。
