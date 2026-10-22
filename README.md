# Notification Service

A small Spring Boot service that listens to order events from
[shop-api](https://github.com/geetadhongade/shop-api) on Kafka and sends the
customer an SMS when an order is placed.

The SMS is **fake** for now: it is written to the log instead of being sent.

```
shop-api ──(outbox)──▶ Kafka topic "order-events" ──▶ notification-service ──▶ SMS
                                                          │        │
                                         notification DB ◀┘        └──▶ "order-events.DLT"
                                    (events already handled)            (messages that kept failing)
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

### Duplicates: at most one SMS per event

Kafka can deliver the same message more than once (for example after a crash or a
retry in shop-api). Before sending, the service saves the `eventId` in the
`processed_event` table of its own `notification` database:

```sql
INSERT INTO processed_event (event_id, event_type, order_id) VALUES (?, ?, ?)
ON CONFLICT (event_id) DO NOTHING
```

If the row already existed, the event was handled before and is skipped
(`Skipping duplicate event ...`). Saving the row and sending the SMS happen in one
database transaction: if the SMS fails, the row is rolled back, so a retry can still send it.

### Failures: retries and a dead-letter topic

| Problem | What happens |
|---|---|
| Sending fails (e.g. SMS provider down) | Tried again 2 seconds later, up to 3 retries (4 tries in total) |
| JSON that can't be read | Not retried (it would never work) |
| Still failing after that | The message is copied to **`order-events.DLT`** (dead-letter topic) and the service moves on to the next message |

A message in the DLT keeps its original key, body and headers, plus headers saying
why it failed (`kafka_dlt-exception-message`, `kafka_dlt-exception-cause-fqcn`, ...)
and where it came from (`kafka_dlt-original-topic`, `-partition`, `-offset`).
Nothing reads the DLT automatically: look at it by hand and resend if needed.

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
| Database | PostgreSQL 16 (plain SQL with `JdbcTemplate`), schema managed by Flyway |
| JSON | Jackson 3 |

## Running it

You need Java 21, PostgreSQL and Docker Desktop. Kafka is started from shop-api's Docker Compose file.

```bash
# 1. Create the database once (the table is created by Flyway when the app starts)
createdb notification

# 2. Start Kafka (from the shop-api folder)
cd shop-api
docker compose up -d kafka

# 3. Start this service (from this folder)
cd ../notification-service
./mvnw spring-boot:run
```

The database connection is in `application.properties`
(`spring.datasource.url=jdbc:postgresql://localhost:5432/notification`); change the
username/password there if yours differ. The dead-letter topic is created when the
service starts.

Then place an order in shop-api, and the SMS line appears in this service's log.

On its very first start the service reads the topic from the beginning, so
events sent before it existed are not missed (`auto-offset-reset=earliest`).

### Trying out failures

```bash
# Make every (fake) SMS fail, to see the retries and the dead-letter topic
./mvnw spring-boot:run -Dspring-boot.run.arguments=--notification.sms.fake-failure=true

# Look at the dead-letter topic (Ctrl+C to stop)
docker exec shop-api-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic order-events.DLT --from-beginning --property print.headers=true --property print.partition=true

# Replay every order event (the service must be stopped) - all should be skipped as duplicates
docker exec shop-api-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --group notification-service --topic order-events --reset-offsets --to-earliest --execute
```

## Configuration

Settings live in `src/main/resources/application.properties`:

| Property | Default | Meaning |
|---|---|---|
| `spring.kafka.bootstrap-servers` | `localhost:9092` | Where Kafka runs (`kafka:29092` from inside Docker) |
| `spring.kafka.consumer.group-id` | `notification-service` | All copies of this service share one group, so each message is handled once |
| `notification.kafka.topic.order-events` | `order-events` | Topic to listen to |
| `notification.kafka.topic.order-events-dlt` | `order-events.DLT` | Dead-letter topic |
| `notification.kafka.topic.order-events-partitions` | `3` | Partitions of the DLT; must match `order-events` |
| `notification.kafka.retry.interval-ms` | `2000` | Wait between tries |
| `notification.kafka.retry.max-retries` | `3` | Retries after the first try |
| `spring.datasource.url` | `jdbc:postgresql://localhost:5432/notification` | This service's own database |
| `notification.sms.fake-failure` | `false` | Testing only: make every SMS fail |

## Logs

Logs go to the console and to `logs/notification-service.log` (rolled daily or at
10 MB, kept for 30 days). Every line shows the shop-api request ID, so one order
can be followed across both apps:

```bash
grep "reqId=my-request-1" ../shop-api/logs/shop-api.log logs/notification-service.log
```

Failures are logged with the same request ID:

```
WARN  [reqId=my-request-1] KafkaConfig : Try 1 failed for event 6f1c... (order 26): IllegalStateException: ...
ERROR [reqId=my-request-1] KafkaConfig : Gave up on event 6f1c... (order 26), sent to order-events.DLT: ...
```

## Project structure

```
src/main/java/com/example/notification
├── NotificationServiceApplication.java   # starts the app
├── config
│   └── KafkaConfig.java                  # retries, dead-letter topic, failure logging
├── consumer
│   ├── OrderEventListener.java           # reads Kafka messages, skips duplicates, builds the SMS text
│   ├── OrderPlacedEvent.java             # shape of the event's JSON body
│   └── ProcessedEventRepository.java     # remembers handled event IDs (processed_event table)
└── sms
    ├── SmsSender.java                    # "send one SMS" interface
    └── LoggingSmsSender.java             # fake sender: writes the SMS to the log

src/main/resources
├── application.properties
└── db/migration
    └── V1__create_processed_event_table.sql
```

## Planned

- Tests, a Docker image and a Docker Compose entry
- A real SMS provider
- Clean-up of old `processed_event` rows
