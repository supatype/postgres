# Install pg_topics

This guide takes you from a PostgreSQL 17 server to a working topic. A stock Kafka client can write to the topic and read from it. The guide takes about 15 minutes, and most of that is the first build. You need superuser access. You must restart PostgreSQL once.

## What you need

| Item | Detail |
|---|---|
| PostgreSQL | Version 17. Other versions are not built or tested, so they are not supported. |
| Server headers | The package that gives you `pg_config` and the server headers. On Debian and Ubuntu this is `postgresql-server-dev-17`. |
| Rust | A current stable toolchain from `rustup`. |
| `cargo-pgrx` | Version 0.12.9 exactly. Other versions do not build this extension. |
| `clang` | `clang`, `libclang-dev` and `llvm-dev`. The build uses them to read the PostgreSQL headers. |
| A TLS certificate | A PEM certificate and its private key for the Kafka port. The listener does not start without one. |

## 1. Build and install the extension

Install the build tools. This example is for Debian or Ubuntu with the PostgreSQL apt repository:

```bash
sudo apt-get install -y postgresql-17 postgresql-server-dev-17 clang libclang-dev llvm-dev
cargo install cargo-pgrx --version 0.12.9 --locked
cargo pgrx init --pg17 "$(which pg_config)"
```

If `which pg_config` does not find the PostgreSQL 17 copy, give the full path. For example, use `/usr/lib/postgresql/17/bin/pg_config`.

Build a release copy and install it into that PostgreSQL:

```bash
cd extensions/pg_topics/extension
cargo pgrx install --release --pg-config "$(which pg_config)"
```

This copies `pg_topics.so` and the SQL files into the PostgreSQL install. If your user cannot write there, use `sudo` with the full path to `cargo`.

## 2. Load the library and name your databases

Add these lines to `postgresql.conf`:

```ini
shared_preload_libraries = 'pg_topics'
pg_topics.databases = 'app'
pg_topics.failover_is_fenced = on
```

- `shared_preload_libraries` loads `pg_topics` when PostgreSQL starts. If you already load other libraries, add `pg_topics` to the same list, for example `'pg_stat_statements,pg_topics'`.
- `pg_topics.databases` is a comma list of the databases that hold topics. Replace `app` with your own database name. Name only databases that exist. A missing database makes its workers restart every 5 seconds.
- `pg_topics.failover_is_fenced = on` is your promise that the old primary stops before a standby takes over. The default durability tier needs it. Read [Durability and failover](configuration.md#durability-tiers) before you set it.

Each database in `pg_topics.databases` starts 5 background workers. They are the stamper, the replicated stamper, the partition worker, the sync worker and the Kafka listener. Check that `max_worker_processes` has room for them:

```ini
max_worker_processes = 16
```

The value must be at least 4 times the number of named databases, plus what the rest of the server already uses. The PostgreSQL default is 8.

Each Kafka client holds one PostgreSQL connection. Keep `max_connections` above `pg_topics.max_clients` (default 100) plus your other connections.

## 3. Let the workers connect

Two workers open a real connection to PostgreSQL. `pg_hba.conf` must let them in.

The partition worker connects over the local Unix socket as the bootstrap superuser, usually `postgres`. Most servers already have this line:

```
local   all    postgres                   peer
```

The Kafka listener connects to `127.0.0.1` over TCP, once for each Kafka client, as that client's own role and password. Add a password line for `127.0.0.1`:

```
host    all    all    127.0.0.1/32    scram-sha-256
```

Warning: a `trust` or `peer` line for `127.0.0.1` does not work. This is on purpose. The listener checks the login method that PostgreSQL used. It refuses any method that is not a password method. So a `trust` line cannot let a Kafka client log in as any role it names. The listener accepts only `scram-sha-256`, `md5`, `password`, `ldap`, `pam` and `radius`.

Put the `127.0.0.1` line above any broader line that would match first.

## 4. Give the listener a certificate

The Kafka port speaks TLS only. Set the certificate and key:

```ini
pg_topics.tls_cert_file = '/etc/pg_topics/server.crt'
pg_topics.tls_key_file  = '/etc/pg_topics/server.key'
```

A relative path starts at the PostgreSQL data directory. The `postgres` operating system user must be able to read the files.

If the server already has `ssl = on` with its own certificate, you can use that one instead:

```ini
pg_topics.tls_use_postgres_cert = on
```

For a test machine, this makes a self-signed certificate that is valid for 365 days:

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout server.key -out server.crt \
  -subj /CN=kafka.example.internal \
  -addext subjectAltName=DNS:kafka.example.internal
```

The name in `subjectAltName` must be the name that clients use to connect.

## 5. Tell clients where to reconnect

A Kafka client asks the server for its own address. Then it reconnects to that address. Set the address to a name that your clients can reach:

```ini
pg_topics.advertised_host = 'kafka.example.internal'
```

The default is `localhost`. That is wrong for any client on another machine or in a container.

The listener port is 9092 by default. To change it for one database, run this as a superuser:

```sql
ALTER DATABASE app SET pg_topics.port = 9093;
```

Each database needs its own port. Two databases on one port do not both work. The second listener cannot bind the port and does not start.

## 6. Restart PostgreSQL and create the extension

Restart PostgreSQL, for example:

```bash
sudo systemctl restart postgresql@17-main
```

Then, as a superuser, run this in each database that you named:

```sql
CREATE EXTENSION pg_topics;
```

The extension puts all its objects in a schema named `topic`. The install stops with an error if a `topic` schema already exists and a non-superuser owns it or has a grant on it. The one grant that it accepts is `USAGE` for `PUBLIC`, which a removed pg_topics leaves. In that case, drop the schema, or give it to a superuser with no grants. Then install again.

## 7. Check that it works

Check that the 5 workers run:

```sql
SELECT backend_type FROM pg_stat_activity WHERE backend_type LIKE 'pg_topics %';
```

You see `pg_topics stamper`, `pg_topics replicated stamper`, `pg_topics partition`, `pg_topics sync` and `pg_topics listener`.

Check that the listener has its port:

```sql
SELECT query FROM topic.listener_status;
```

You see `listening on port 9092`. Any other text tells you why it did not bind, for example a missing certificate.

## 8. Make a first topic

Make a role for your application and a topic for it. A topic name is always `schema.table`, and the table name ends in `_q`. A band is what Kafka calls a partition.

```sql
CREATE ROLE orders_app LOGIN PASSWORD 'change-me';
CREATE SCHEMA shop AUTHORIZATION orders_app;
SET ROLE orders_app;
SELECT topic.create_topic('shop.orders_q', band_count => 4);
SELECT topic.publish('shop.orders_q', '{"order_id": 1}', key => 'customer-42');
RESET ROLE;
```

Now read the record back in SQL:

```sql
SELECT * FROM topic.fetch('shop.orders_q', band => topic.band_for('customer-42', 4), from_offset => 0);
```

Then read it with a Kafka client. Save the client settings in `client.properties`:

```properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="orders_app" password="change-me";
ssl.truststore.type=PEM
ssl.truststore.location=/path/to/server.crt
```

```bash
kafka-console-consumer --bootstrap-server kafka.example.internal:9092 \
  --consumer.config client.properties --topic shop.orders_q --from-beginning
```

You see `{"order_id": 1}`. [Connect a Kafka client](kafka-clients.md) has settings for other client libraries.

## Upgrade or reinstall

The workers load when PostgreSQL starts. So a new build of `pg_topics` needs a PostgreSQL restart.

Warning: a restart takes longer than the session timeout of most Kafka consumers. Every consumer group in every named database rebalances once when its clients reconnect. Restart at a quiet time.

Build and install as in step 1, then restart.

Version 0.1.0 is the first version, so there are no upgrade scripts yet.

## Remove pg_topics

Warning: removing the extension deletes every offset, consumer group and topic setting. Export any data that you need first.

1. In each named database, drop each topic with `topic.drop_topic`, for example `SELECT topic.drop_topic('shop.orders_q');`. This drops the queue table and its control rows.
2. Run `DROP EXTENSION pg_topics CASCADE;`. The triggers on each queue table use functions of the extension, so the command fails without `CASCADE` while any queue table is left. `CASCADE` removes those triggers and leaves the queue tables as plain tables.
3. Optional: run `DROP SCHEMA topic;`. `DROP EXTENSION` leaves the empty schema. A later `CREATE EXTENSION pg_topics` accepts it.
4. Remove `pg_topics` from `shared_preload_libraries`.
5. Remove the `pg_topics.*` lines from `postgresql.conf`.
6. Restart PostgreSQL.
