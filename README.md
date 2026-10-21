# Notification Service

A small Spring Boot service that listens to order events from
[shop-api](https://github.com/geetadhongade/shop-api) on Kafka and sends the
customer an SMS when an order is placed.

The SMS is **fake** for now: it is written to the log instead of being sent.

```
shop-api ──(outbox)──▶ Kafka topic "order-events" ──▶ notification-service ──▶ SMS
```

## How it works

1. A customer places an order in shop-api.
2. shop-api saves the order **and** an `ORDER_PLACED` event in the same database
   transaction (transactional outbox), then publishes the event to Kafka. If Kafka
   is down, the event waits in the database and is sent later.
3. This service reads the event from the `order-events` topic and sends an SMS like:

   ```
   Hi Bob, your order #26 (3 items, USD 2119.00) has been placed.
   ```

Each Kafka message carries these headers:

| Header | Meaning |
|---|---|
| `eventId` | Unique ID of the event (UUID), the same on every re-send |
| `eventType` | `ORDER_PLACED` (other types are skipped) |
| `reqId` | Request ID of the original shop-api call, shown on every log line |

The message key is the order ID, so all events for one order stay in order.

Example message body:

```json
{
  "eventId": "6f1c...",
  "eventType": "ORDER_PLACED",
  "schemaVersion": 1,
  "occurredAt": "2026-10-21T10:15:30Z",
  "orderId": 26,
  "customerId": 3,
  "customerFirstName": "Bob",
  "customerPhone": "5550101",
  "totalAmount": 2119.00,
  "currency": "USD",
  "itemCount": 3
}
```

## Tech stack

| Part | Choice |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 4.1, Spring for Apache Kafka |
| Messaging | Apache Kafka 4.1 (KRaft mode, no ZooKeeper) |
| JSON | Jackson 3 |

## Running it

You need Java 21 and Docker Desktop. Kafka is started from shop-api's Docker Compose file.

```bash
# 1. Start Kafka (from the shop-api folder)
cd shop-api
docker compose up -d kafka

# 2. Start this service (from this folder)
cd ../notification-service
./mvnw spring-boot:run
```

Then place an order in shop-api, and the SMS line appears in this service's log.

On its very first start the service reads the topic from the beginning, so
events sent before it existed are not missed (`auto-offset-reset=earliest`).

## Configuration

Settings live in `src/main/resources/application.properties`:

| Property | Default | Meaning |
|---|---|---|
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Where Kafka runs (`kafka:29092` from inside Docker) |
| `spring.kafka.consumer.group-id` | `notification-service` | All copies of this service share one group, so each message is handled once |
| `notification.kafka.topic.order-events` | `order-events` | Topic to listen to |

## Logs

Logs go to the console and to `logs/notification-service.log` (rolled daily or at
10 MB, kept for 30 days). Every line shows the shop-api request ID, so one order
can be followed across both apps:

```bash
grep "reqId=my-request-1" ../shop-api/logs/shop-api.log logs/notification-service.log
```

## Project structure

```
src/main/java/com/example/notification
├── NotificationServiceApplication.java   # starts the app
├── consumer
│   ├── OrderEventListener.java           # reads Kafka messages, builds the SMS text
│   └── OrderPlacedEvent.java             # shape of the event's JSON body
└── sms
    ├── SmsSender.java                    # "send one SMS" interface
    └── LoggingSmsSender.java             # fake sender: writes the SMS to the log
```

## Planned

- Skip duplicate events (remember handled event IDs in its own database)
- Retries and a dead-letter topic (`order-events.DLT`) for messages that keep failing
- Tests, a Docker image and a Docker Compose entry
- A real SMS provider
