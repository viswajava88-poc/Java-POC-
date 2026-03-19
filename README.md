# 🛒 Event-Driven Order & Notification System

A production-quality event-driven microservices assignment demonstrating real-time order processing with Kafka, WebSocket push notifications, the Outbox pattern, idempotency, and full observability.

---

## 📋 Table of Contents

- [Architecture Diagram](#-architecture-diagram)
- [Services Overview](#-services-overview)
- [Project Structure](#-project-structure)
- [Quick Start](#-quick-start)
- [API Reference](#-api-reference)
- [Design Decisions](#-design-decisions)
- [Resilience Concepts](#-resilience-concepts)
- [Observability](#-observability)
- [Failure Handling](#-failure-handling)
- [Deployment](#-deployment)

---

## 🏗 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          CLIENT / BROWSER                               │
│                                                                         │
│   ┌──────────────────────────────────────────────────────────────┐     │
│   │                  Frontend (index.html)                        │     │
│   │   - POST /orders  ──────────────────────► Order Service       │     │
│   │   - WebSocket /ws  ◄────────────────────  Notification Svc    │     │
│   └──────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────┘
         │  REST (HTTP)                          │  STOMP / WebSocket
         ▼                                       ▲
┌─────────────────────┐              ┌───────────────────────┐
│    Order Service    │              │  Notification Service  │
│    (port 8080)      │              │  (port 8081)           │
│                     │              │                        │
│  ┌───────────────┐  │              │  ┌──────────────────┐  │
│  │  Controller   │  │              │  │  Kafka Consumer  │  │
│  │  POST /orders │  │              │  │  (order-events)  │  │
│  │  GET  /orders │  │              │  └────────┬─────────┘  │
│  └──────┬────────┘  │              │           │            │
│         │           │              │  ┌────────▼─────────┐  │
│  ┌──────▼────────┐  │              │  │  WS Broadcaster  │  │
│  │    Service    │  │              │  │  /topic/orders   │  │
│  │  (Business    │  │              │  └──────────────────┘  │
│  │   Logic +     │  │              └───────────────────────┘
│  │   Idempotency)│  │                         ▲
│  └──────┬────────┘  │                         │
│         │           │    ┌────────────────┐   │
│  ┌──────▼────────┐  │    │     Kafka      │   │
│  │  JPA / DB     ├──┼───►│  order-events  ├───┘
│  │  Repository   │  │    │   (topic)      │
│  └───────────────┘  │    └────────────────┘
│         │           │              ▲
│  ┌──────▼────────┐  │              │
│  │  Outbox Table │  │    ┌─────────┴──────┐
│  │  (outbox_     ├──┼───►│ Outbox Relay   │
│  │   events)     │  │    │ (Scheduler,    │
│  └───────────────┘  │    │  every 5s)     │
│                     │    └────────────────┘
└──────────┬──────────┘
           │
    ┌──────▼──────┐
    │  PostgreSQL  │
    │  - orders    │
    │  - order_    │
    │    items     │
    │  - outbox_   │
    │    events    │
    └─────────────┘


┌──────────────────────────────────────────┐
│              Observability               │
│                                          │
│  Prometheus ◄── /actuator/prometheus     │
│      │           (both services)         │
│      ▼                                   │
│  Grafana (dashboards, port 3001)         │
│                                          │
│  Structured Logs → stdout (Loki-ready)   │
│  Spring Actuator → /health, /metrics     │
└──────────────────────────────────────────┘
```

---

## 🧩 Services Overview

| Service | Port | Tech | Responsibility |
|---|---|---|---|
| **Order Service** | 8080 | Spring Boot, JPA, Kafka | Create & retrieve orders, publish events |
| **Notification Service** | 8081 | Spring Boot, Kafka, WebSocket | Consume events, push to browser clients |
| **Frontend** | 3000 | HTML/JS (SockJS + STOMP) | Create orders, display live notifications |
| **PostgreSQL** | 5432 | Postgres 15 | Persist orders, items, outbox events |
| **Kafka** | 9092 | Confluent Kafka 7.5 | Message broker |
| **Kafka UI** | 8090 | Provectus UI | Dev tool — browse topics & messages |
| **Prometheus** | 9090 | Prometheus | Scrape metrics from services |
| **Grafana** | 3001 | Grafana | Visualise metrics dashboards |

---

## 📁 Project Structure

```
assignment/
│
├── docker-compose.yml                  # Full stack orchestration
├── monitoring/
│   └── prometheus.yml                  # Prometheus scrape config
│
├── frontend/
│   └── index.html                      # WebSocket UI client
│
├── order-service/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/main/java/com/example/orderservice/
│       ├── OrderServiceApplication.java
│       ├── controller/
│       │   └── OrderController.java     # POST /orders, GET /orders/{id}
│       ├── service/
│       │   └── OrderService.java        # Business logic, idempotency, outbox
│       ├── repository/
│       │   ├── OrderRepository.java
│       │   └── OutboxEventRepository.java
│       ├── model/
│       │   ├── Order.java               # JPA entity
│       │   ├── OrderItem.java           # JPA entity
│       │   └── OutboxEvent.java         # Outbox table entity
│       ├── dto/
│       │   └── OrderDto.java            # Request / Response / Event DTOs
│       ├── kafka/
│       │   └── OrderEventPublisher.java # Outbox relay scheduler → Kafka
│       ├── config/
│       │   └── KafkaProducerConfig.java
│       └── exception/
│           ├── OrderNotFoundException.java
│           ├── DuplicateOrderException.java
│           └── GlobalExceptionHandler.java
│
└── notification-service/
    ├── Dockerfile
    ├── pom.xml
    └── src/main/java/com/example/notificationservice/
        ├── NotificationServiceApplication.java
        ├── kafka/
        │   └── OrderEventConsumer.java  # Kafka listener
        ├── websocket/
        │   └── NotificationBroadcaster.java  # STOMP broadcaster
        ├── config/
        │   ├── KafkaConsumerConfig.java
        │   └── WebSocketConfig.java
        └── model/
            └── OrderEvent.java
```

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose installed
- Ports 3000, 5432, 8080, 8081, 8090, 9090, 9092 free

### Run Everything

```bash
# Clone / unzip project
cd assignment

# Build and start all services
docker compose up --build

# To run in background
docker compose up --build -d
```

### Service URLs

| URL | Description |
|---|---|
| http://localhost:3000 | Frontend UI |
| http://localhost:8080/orders | Order Service API |
| http://localhost:8081/actuator/health | Notification Service health |
| http://localhost:8090 | Kafka UI (browse topics) |
| http://localhost:9090 | Prometheus |
| http://localhost:3001 | Grafana (admin / admin) |

### Stop

```bash
docker compose down
docker compose down -v   # also removes volumes
```

---

## 📡 API Reference

### POST `/orders` — Create Order

```http
POST http://localhost:8080/orders
Content-Type: application/json
X-Idempotency-Key: my-unique-key-123   (optional)
```

```json
{
  "customerName": "Jane Doe",
  "customerEmail": "jane@example.com",
  "items": [
    {
      "productId": "PROD-001",
      "productName": "Laptop",
      "quantity": 1,
      "unitPrice": 999.99
    }
  ]
}
```

**Response 201:**
```json
{
  "success": true,
  "message": "Order created successfully",
  "data": {
    "id": 1,
    "orderNumber": "ORD-A1B2C3D4",
    "customerName": "Jane Doe",
    "customerEmail": "jane@example.com",
    "status": "PENDING",
    "totalAmount": 999.99,
    "items": [...],
    "createdAt": "2024-03-19T10:00:00",
    "updatedAt": "2024-03-19T10:00:00"
  }
}
```

---

### GET `/orders/{id}` — Get Order by ID

```http
GET http://localhost:8080/orders/1
```

**Response 200:**
```json
{
  "success": true,
  "message": "Order retrieved successfully",
  "data": { ... }
}
```

**Response 404 (not found):**
```json
{
  "success": false,
  "message": "Order not found: 99"
}
```

---

## 🧠 Design Decisions

### 1. Messaging Technology — Apache Kafka

**Why Kafka over RabbitMQ or SQS?**

Kafka was chosen because:
- **Log retention** — events are durably stored and replayable. If the Notification Service goes down and restarts, it can replay from its last committed offset and not miss a single event.
- **High throughput** — Kafka handles millions of messages per second, making it suitable for order-at-scale scenarios.
- **Consumer groups** — multiple notification consumers can be added with no code change, enabling horizontal scaling.
- **Ordering guarantees** — messages within a partition are strictly ordered by insertion time.

RabbitMQ would be simpler for lower scale. SQS is a managed alternative if deploying to AWS.

---

### 2. Outbox Pattern for Reliable Event Publishing

**Problem:** If the Order Service saves the order to PostgreSQL and then tries to publish to Kafka separately, a crash between those two steps causes the event to be silently lost — the order exists but no notification is ever sent.

**Solution — Outbox Pattern:**
1. When creating an order, write both the `orders` row and an `outbox_events` row **in the same database transaction**.
2. A separate scheduler (`OrderEventPublisher`) polls `outbox_events` every 5 seconds, publishes PENDING events to Kafka, then marks them PUBLISHED.
3. Because the outbox write is atomic with the order write, there is **no window for data loss**.

```
DB Transaction:
  INSERT INTO orders ...
  INSERT INTO outbox_events (status=PENDING) ...
  COMMIT

Scheduler (every 5s):
  SELECT * FROM outbox_events WHERE status='PENDING'
  → kafka.send(event)
  → UPDATE outbox_events SET status='PUBLISHED'
```

---

### 3. Communication Patterns

| Pattern | Used For |
|---|---|
| **Synchronous REST** | Client → Order Service (create, retrieve) |
| **Async Event (Kafka)** | Order Service → Notification Service |
| **WebSocket (STOMP)** | Notification Service → Browser client |

The REST layer is synchronous because clients need an immediate response with the order ID. Downstream processing (notifications) is asynchronous — the client doesn't need to wait for the notification system.

---

### 4. WebSocket vs Server-Sent Events

**WebSocket (STOMP over SockJS)** was chosen over SSE because:
- STOMP allows topic-based subscriptions (`/topic/orders`) without custom routing logic.
- SockJS provides a transparent fallback for browsers or proxies that block WebSocket.
- Two-way communication is available if needed in future (e.g. client sends acknowledgement).

SSE would have been simpler for strictly one-way push but less extensible.

---

### 5. Database — PostgreSQL

PostgreSQL was chosen over H2 or MySQL for:
- **ACID compliance** — critical for the outbox pattern's transactional guarantee.
- **Production readiness** — same DB runs locally (Docker) and in production.
- **JSON support** — the `payload` column in `outbox_events` stores JSON natively.

---

### 6. Scaling Strategy

**Order Service** is stateless (session-free REST). Scale horizontally behind a load balancer. The outbox relay uses DB-level row locking to prevent duplicate publishing when multiple instances run.

**Notification Service** scales by adding consumers to the Kafka consumer group `notification-service`. Kafka automatically rebalances partitions across instances. WebSocket state is in-process — for multi-instance WS, a shared broker like Redis Pub/Sub would be needed.

**Kafka** can scale by adding partitions and brokers. The `order-events` topic can be partitioned by `customerEmail` to ensure per-customer ordering.

---

## 🛡️ Resilience Concepts

### Idempotency

Every `POST /orders` request accepts an optional `X-Idempotency-Key` header. The key is stored with a unique constraint in the database. If the same key is submitted again (e.g. a retry after network timeout), the existing order is returned immediately without re-processing or re-publishing the event.

```
Client → POST /orders  (X-Idempotency-Key: abc123)
  → DB check: key exists? → return existing order
  → DB check: key missing? → create new order
```

Without an idempotency key, the caller is responsible for not duplicating requests.

---

### Retry Strategy

**Outbox relay retries** — the scheduler retries failed Kafka publishes up to 3 times, with exponential back-off implicit in the 5-second polling interval. After 3 failures the event is marked `FAILED` and can be routed to a dead-letter queue or alerted on.

**Kafka producer retries** — the producer is configured with `retries=3` and `acks=all`, meaning a message is only confirmed sent when all in-sync replicas have acknowledged it.

**Kafka consumer retries** — the consumer uses manual acknowledgment. If processing fails, the offset is still committed (to avoid a poison-pill loop) and the message is logged for dead-letter handling. In production, a Dead Letter Topic (DLT) would receive these failed messages.

---

### Circuit Breaker

Not implemented in code, but the recommended approach is **Resilience4j**:

```java
// Example — wrap Kafka send with circuit breaker
@CircuitBreaker(name = "kafka", fallbackMethod = "fallbackPublish")
public void publishEvent(String payload) {
    kafkaTemplate.send(ORDER_TOPIC, payload);
}

public void fallbackPublish(String payload, Exception e) {
    // Log alert — outbox relay will retry from DB
    log.error("Circuit open: Kafka unavailable, event will be retried from outbox");
}
```

The outbox pattern itself acts as a natural circuit breaker buffer — events accumulate in the DB and flush when Kafka recovers.

---

## 📊 Observability

### Metrics (Prometheus + Grafana)

Both services expose a `/actuator/prometheus` endpoint scraped every 15 seconds.

Custom application metrics tracked:

| Metric | Description |
|---|---|
| `orders.created` | Total orders successfully created |
| `orders.duplicate` | Idempotent duplicate requests detected |
| `notifications.sent` | Events successfully pushed via WebSocket |
| `notifications.failed` | Events that failed to process |

Standard Spring Boot metrics also auto-exposed: JVM heap, GC, HTTP request latency, Kafka consumer lag, database connection pool.

**Grafana** dashboards available at http://localhost:3001 (admin / admin). Import dashboard ID `4701` from Grafana.com for a Spring Boot overview.

---

### Logging

Both services use structured console logging formatted for log aggregators (Loki, Elasticsearch):

```
2024-03-19 10:30:00 [main] INFO  c.e.o.service.OrderService - Order created orderNumber=ORD-A1B2C3D4
2024-03-19 10:30:05 [scheduler] INFO  c.e.o.kafka.OrderEventPublisher - Published outbox event id=1 type=ORDER_CREATED
2024-03-19 10:30:05 [consumer] INFO  c.e.n.kafka.OrderEventConsumer - Processing event type=ORDER_CREATED orderNumber=ORD-A1B2C3D4
```

In production, ship logs to **Grafana Loki** or **ELK Stack** by adding the appropriate Logback appender.

---

### Tracing

Not implemented in code but the recommended approach is **Micrometer Tracing + Zipkin**:

```xml
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-tracing-bridge-brave</artifactId>
</dependency>
```

This auto-propagates `traceId` and `spanId` headers across REST calls and Kafka messages, enabling end-to-end trace visualisation in Zipkin or Jaeger.

---

## ⚠️ Failure Handling

### Scenario 1 — Kafka is unavailable

**During order creation:**
The Order Service writes the order to PostgreSQL and writes the event to the `outbox_events` table — both in one transaction. The API responds successfully to the client. The Kafka event is **not lost**.

When Kafka recovers, the outbox relay scheduler detects the PENDING event and publishes it. The notification arrives delayed but is never dropped.

**During consumption:**
The Notification Service's Kafka consumer will fail to connect. Spring Kafka retries the connection automatically. Once Kafka is back, the consumer reads from its last committed offset — no events are skipped.

---

### Scenario 2 — A service crashes mid-request

**Order Service crash after DB write but before outbox write:**
The transaction is rolled back. The order does not exist. The client receives an error and can safely retry (with the same idempotency key if provided).

**Order Service crash after outbox write:**
The order and outbox event both exist. The outbox relay will publish the event when the service restarts. The client may have received an error but the order is persisted — idempotency key prevents duplicate creation on retry.

**Notification Service crash:**
The Kafka offset was not committed for any in-flight message. On restart, the consumer re-reads from the last committed offset and reprocesses. WebSocket connections are dropped and clients reconnect automatically (SockJS auto-reconnect is implemented in the frontend, 3-second retry).

---

### Scenario 3 — Duplicate orders submitted

**With `X-Idempotency-Key` header:**
The second request hits the unique-constraint check in the database, finds the existing order, and returns it immediately. No duplicate is created, no duplicate event is published, no duplicate notification is sent.

**Without `X-Idempotency-Key` header:**
Two separate orders are created with different order numbers. The system treats them as independent orders. Clients should always send an idempotency key when implementing retry logic.

**Kafka consumer deduplication:**
The `orderNumber` is carried in the event payload and is globally unique (UUID-based). If a duplicate event somehow arrives on the Kafka topic, the Notification Service broadcasts it — but the client UI can deduplicate by `orderNumber`.

---

## 🐳 Deployment

### Docker Compose (included)

```bash
docker compose up --build
```

All 8 containers start with correct health-check ordering (Postgres and Kafka must be healthy before the services start).

### Basic Kubernetes (extension)

Create the following resources per service: `Deployment`, `Service`, `ConfigMap` for environment variables, and a `Secret` for database credentials. Example for the Order Service:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: order-service
  template:
    metadata:
      labels:
        app: order-service
    spec:
      containers:
        - name: order-service
          image: order-service:1.0.0
          ports:
            - containerPort: 8080
          env:
            - name: SPRING_DATASOURCE_URL
              value: jdbc:postgresql://postgres-service:5432/orderdb
            - name: SPRING_KAFKA_BOOTSTRAP_SERVERS
              value: kafka-service:9092
          readinessProbe:
            httpGet:
              path: /actuator/health
              port: 8080
            initialDelaySeconds: 20
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /actuator/health
              port: 8080
            initialDelaySeconds: 30
            periodSeconds: 15
---
apiVersion: v1
kind: Service
metadata:
  name: order-service
spec:
  selector:
    app: order-service
  ports:
    - port: 8080
      targetPort: 8080
```

For Kafka in Kubernetes, use the **Strimzi Kafka Operator**. For PostgreSQL, use a StatefulSet with persistent volume claims or a managed service (RDS, Cloud SQL).

---

*Built with Java 17 · Spring Boot 3.2 · Apache Kafka · PostgreSQL · WebSocket (STOMP/SockJS) · Docker Compose · Prometheus · Grafana*
