# API 通知系统

企业内部通知投递服务：接收业务系统的 HTTP 通知请求，通过 RocketMQ 异步可靠地投递到外部供应商 API。


## 问题理解
1. 实现一个消息中台，可接收不同业务系统的消息调用，然后针对不同类型发出对应的消息。因此，对外应封装统一调用API，对内则根据不同消息模板发送消息。
2. 需关注系统可扩展性，以后可接入更多业务消息投递。
3. 需关注消息投递的可靠性和可回溯性，应具备重复投递和日志记录功能。

## 架构概览
![](https://github.com/dreamckk/rc_cuikai/blob/main/img/arch.png)

## 设计文档
见 [docs/superpowers/specs/2026-04-26-api-notification-system-design.md](docs/superpowers/specs/2026-04-26-api-notification-system-design.md)

### 核心可靠性设计

####  投递语义：At-least-once

宁可重复投递，不可丢失。原因：外部系统（广告/CRM/库存）通常能设计幂等接口，而丢通知的业务损失更难接受和追溯。

####  重试策略
使用 RocketMQ 内置重试机制：
- 最大重试次数：16 次
- 重试间隔：指数退避（10s → 30s → 1min → 2min → ... → 2h），跨越约 4 小时
- Dispatcher 消费失败时不 ACK，RocketMQ 自动延迟重新投递

####  外部系统长期不可用的处理
1. RocketMQ 完成 16 次重试（约 4 小时内）
2. 超限后消息自动进入死信队列
3. 监控告警触发，通知运维人员
4. 等供应商恢复后，通过 `/api/v1/notify/{id}/retry` 接口手动补发


## 关键工程决策与取舍说明
1. 使用Springboot实现简单的MVC三层架构设计，只实现api接口及消息投递，避免过度设计
2. 只存储不同业务http模板及投递日志，不设计过多DB存储
3. 消息投递依赖RocketMq保证可靠投递，暂不设计失败打点监控及兜底重试任务

## AI 使用说明
### AI 提供了帮助的地方
- 利用Claude Code辅助编码，利用Superpowers SDD开发范式，技术方案、执行方案、代码编写完全由AI实现
- 梳理通知系统的常见失败模式和重试策略
- 提供消息队列技术选型对比
- 生成 DB 表结构初稿、API 接口定义和单元测试代码

### AI 给出但未采纳的建议
- **事务消息（本地消息表）**：复杂度过高，不适合MVP。网关写入 MQ 失败概率极低，业务系统可重试调用网关。且本系统重点关注于消息的投递。
- **多Topic消费**：不同消息类型投递不同的topic，增加了维护工作。此处采用单topic+消息模板，可满足需求。
- **消息限流**：消息限流需基于业务量大小设置，此处暂不考虑。

### 关键决策由自己做出
- **选择 RocketMQ 而非 Kafka**：付费通知、商品库存等场景需要保证更强的一致性，RocketMQ更适合；且如果需要订单和库存的事务操作，RocketMQ更适合；且RocketMQ
- **At-least-once 语义**：可重复投递，外部系统做消息幂等处理。
- **系统边界的划定**：幂等性、鉴权、限流均不在本系统内解决，职责单一。
- **供应商配置存 DB 而非配置文件**：运行时可动态新增/修改供应商，无需重启服务。
- **投递日志记录**：增加日志记录环节，系统内应记录投递日志，用于审计、问题排查等。


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

