# Connect a Kafka client

A stock Kafka client works with pg_topics with no code change. Change
four settings: the address, the security settings, the topic
names and, for librdkafka, the partitioner. This page gives working
settings for each tested client.

## What every client needs

| Setting | Value |
|---|---|
| Bootstrap server | `pg_topics.advertised_host` and the database's `pg_topics.port`, for example `kafka.example.internal:9092` |
| Security protocol | `SASL_SSL`. pg_topics does not offer plain text. |
| SASL mechanism | `PLAIN`. pg_topics does not offer SCRAM, OAUTHBEARER or GSSAPI. |
| User name and password | A PostgreSQL role that can log in, and its password |
| CA certificate | The listener's certificate, or the CA that signed it |
| Topic name | The queue table, `schema.table_q`, for example `shop.orders_q` |

Warning: a producer aimed at a topic named `orders` gets
`TOPIC_AUTHORIZATION_FAILED`, the same answer as for a topic that it may
not see. Use the full table name.

A value must be JSON. A key must be UTF-8 text of 40 characters or fewer.
See [Kafka compatibility](../README.md#kafka-compatibility) for the full
list of limits.

## Tested client versions

| Client | Version tested |
|---|---|
| Java client and tools | `confluentinc/cp-kafka:7.7.1` (Apache Kafka 3.7) |
| librdkafka | 2.15.1, through `confluent-kafka` for Python 2.15.1 |
| Confluent JavaScript | `@confluentinc/kafka-javascript` 1.10.1 |
| kafkajs | 2.2.4 |
| franz-go | 1.22.1 |

Each client produces 1000 keyed records with headers. It consumes them in a
group of two members, commits, and resumes from the committed offset after a
restart. The Java client also reads every batch with `check.crcs=true`.
The results of kafkajs and franz-go are for information only. Their
failures do not fail the test suite.

## Java

```properties
bootstrap.servers=kafka.example.internal:9092
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="orders_app" password="change-me";
ssl.truststore.type=PEM
ssl.truststore.location=/etc/pg_topics/server.crt
```

The same file works as `--command-config`, `--producer.config` and
`--consumer.config` for the Kafka command line tools.

The Java producer is idempotent by default since Kafka 3.0. Keep this
default.

## librdkafka and clients built on it

This covers `confluent-kafka` for Python, Go and .NET, and any other client
built on librdkafka.

```python
from confluent_kafka import Producer, Consumer

base = {
    "bootstrap.servers": "kafka.example.internal:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "orders_app",
    "sasl.password": "change-me",
    "ssl.ca.location": "/etc/pg_topics/server.crt",
}
producer = Producer({**base, "partitioner": "murmur2_random", "enable.idempotence": True})
consumer = Consumer({**base, "group.id": "shop.billing", "auto.offset.reset": "earliest"})
```

Warning: set `partitioner` to `murmur2_random`. The librdkafka default,
`consistent_random`, uses another hash. A keyed SQL `topic.publish` and a
librdkafka producer put the same key on different bands. The order per
key is lost.

Warning: turn on `enable.idempotence`. librdkafka retries a failed batch up
to 2147483647 times by default. A producer that is not idempotent can write
one record many times when the stamper is slow. See
[Produce waits for the offset](#produce-waits-for-the-offset).

## Confluent JavaScript

```js
const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

const base = {
  'bootstrap.servers': 'kafka.example.internal:9092',
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'PLAIN',
  'sasl.username': 'orders_app',
  'sasl.password': 'change-me',
  'ssl.ca.location': '/etc/pg_topics/server.crt',
};
const kafka = new Kafka();
const producer = kafka.producer({ ...base, partitioner: 'murmur2_random' });
const consumer = kafka.consumer({ ...base, 'group.id': 'shop.billing', 'auto.offset.reset': 'earliest' });
```

This client uses librdkafka, so the partitioner warning above applies.

## kafkajs

```js
const fs = require('fs');
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  brokers: ['kafka.example.internal:9092'],
  ssl: { ca: [fs.readFileSync('/etc/pg_topics/server.crt')] },
  sasl: { mechanism: 'plain', username: 'orders_app', password: 'change-me' },
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'shop.billing' });
```

The kafkajs 2.x default partitioner agrees with pg_topics.

## franz-go

```go
pem, _ := os.ReadFile("/etc/pg_topics/server.crt")
pool := x509.NewCertPool()
pool.AppendCertsFromPEM(pem)
cl, err := kgo.NewClient(
	kgo.SeedBrokers("kafka.example.internal:9092"),
	kgo.DialTLSConfig(&tls.Config{RootCAs: pool}),
	kgo.SASL(plain.Auth{User: "orders_app", Pass: "change-me"}.AsMechanism()),
	kgo.ConsumerGroup("shop.billing"),
	kgo.ConsumeTopics("shop.orders_q"),
)
```

The franz-go default partitioner agrees with pg_topics.

## Produce waits for the offset

A record gets its offset from a background worker, the stamper, a short
time after the commit. With `acks=1` or `acks=all`, `Produce` answers only
after the stamper gives the records their offsets. So the client sees the
true offset of each record. At a fixed 10,000 records a second, the median
answer took 31 to 34 ms in the benchmarks.

If the stamper does not stamp the records within the request timeout, the
answer is `REQUEST_TIMED_OUT`. The records are already stored. The client
then retries:

- An idempotent producer gets a `DUPLICATE` answer with the first offsets.
  No record is written twice.
- A producer that is not idempotent writes the records a second time.

Use an idempotent producer.

`acks=0` does not wait.

## Record contents

- Values: a value must be valid JSON. `Fetch` gives back the value as
  PostgreSQL `jsonb` prints it. Field order and white space can change.
  A signature check over the raw bytes fails.
- Keys: UTF-8, 40 characters or fewer. A longer key, or a key that is not
  UTF-8, fails the whole batch with `INVALID_RECORD`.
- Headers: every header comes back exactly, in order. Names can repeat.
  Binary values are kept.
- Timestamps: a consumer sees the server time of the insert, as
  `LogAppendTime`. The producer's own timestamp is stored in the column
  `producer_timestamp` but is not sent back over Kafka.
- Tombstones: a record with a key and a null value is kept and comes back
  with a null value.
- Compression: `Produce` accepts `gzip`, `snappy`, `lz4` and `zstd`.
  `Fetch` always sends uncompressed batches.

## Consumer groups

Groups use the classic Kafka group protocol. The client does the band
assignment, and pg_topics passes it on unchanged. The tests cover the
`range` and `cooperative-sticky` assignors.

- The new consumer protocol of KIP-848 (`group.protocol=consumer`) is not
  offered. Keep the default `group.protocol=classic`.
- Static membership (`group.instance.id`) is ignored. A restarted member
  joins as a new member and the group rebalances.
- `session.timeout.ms` must be between `pg_topics.group_min_session_ms`
  (6000) and `pg_topics.group_max_session_ms` (1800000).
- Group names are shared by the whole database. Start each name with your
  schema name, for example `shop.billing`.

## Admin tools

These Kafka tools work:

| Tool | Works for |
|---|---|
| `kafka-topics` | `--create`, `--describe`, `--delete` |
| `kafka-configs` | `--describe` and `--alter` on topics, for `retention.ms` and `pg_topics.min_durability` |
| `kafka-consumer-groups` | `--list`, `--describe`, `--delete`, `--reset-offsets` on a group with no members |
| `kafka-broker-api-versions` | Lists the 24 APIs that pg_topics offers |
| `kafka-console-producer`, `kafka-console-consumer` | Yes |
| `kafka-producer-perf-test`, `kafka-consumer-perf-test` | Yes |

These tools do not work, because pg_topics does not offer their APIs:
`kafka-acls`, `kafka-reassign-partitions`, `kafka-leader-election`,
`kafka-delete-records`, `kafka-log-dirs`, `kafka-transactions`,
`kafka-features` and `kafka-metadata-quorum`.

A `kafka-topics --create` with a config other than `retention.ms` and
`pg_topics.min_durability` fails with `INVALID_CONFIG`, and so does
`cleanup.policy=compact`. Kafka Connect in distributed mode, Kafka Streams
and Schema Registry create compacted topics for their own state. They fail
at that step. They are not tested.
