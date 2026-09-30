import os
import socket
import ssl
import sys
import threading
import time

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition


def connect(host, port):
    deadline = time.monotonic() + 5
    while True:
        try:
            return socket.create_connection((host, port), timeout=5)
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.1)


def base():
    return {
        "bootstrap.servers": os.environ["BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": os.environ["KAFKA_USER"],
        "sasl.password": os.environ["KAFKA_PASSWORD"],
        "ssl.ca.location": "/w/server.crt",
        "error_cb": lambda e: print("cb", e.name(), e.str(), flush=True),
    }


def consumer(**extra):
    return Consumer({**base(), "group.id": "pg_topics_harness", "enable.auto.commit": False,
                     "enable.auto.offset.store": False, **extra})


def report(err, msg):
    if err:
        print("err", err.name(), flush=True)
    else:
        print("ok", msg.partition(), msg.offset(), msg.value().decode(), flush=True)


def produce(topic, count, mode="json", compression="none", keys="", partition=-1,
            timestamp=0, acks="all", request_timeout=30000):
    p = Producer({**base(), "partitioner": "murmur2_random", "compression.type": compression,
                  "acks": acks, "linger.ms": 50, "message.max.bytes": 64000000,
                  "batch.size": 64000000, "message.timeout.ms": 10000, "retries": 0,
                  "request.timeout.ms": int(request_timeout)})
    for i in range(int(count)):
        key = None
        if keys == "long":
            key = "k" * 41
        elif keys:
            key = f"{keys}-{i}"
        value = {"json": f'{{"i": {i}}}', "text": "not json",
                 "mid": '"' + "x" * 200000 + '"',
                 "big": '"' + "x" * 1050000 + '"',
                 "bomb": '"' + "0" * 20000000 + '"'}[mode]
        p.produce(topic, value=value.encode(), key=key, partition=int(partition),
                  timestamp=int(timestamp), headers=[("n", str(i).encode())], on_delivery=report)
    left = p.flush(60)
    print("left", left, flush=True)


def produce_headers(topic):
    p = Producer(base())
    p.produce(topic, value=b'{"h": 1}', partition=0, on_delivery=report,
              headers=[("a", b"1"), ("a", b"2"), ("bin", b"\xff\x00\x01"), ("n", None)])
    print("left", p.flush(10), flush=True)


def headers(topic):
    c = consumer()
    c.assign([TopicPartition(topic, 0, 0)])
    m = c.poll(20)
    print("headers", None if m is None or m.error() else m.headers(), flush=True)
    c.close()


def consume(topic, partition, offset, count, reset="earliest", cfg=""):
    c = consumer(**{"auto.offset.reset": reset, **{k: int(v) for k, v in
                    (item.split("=") for item in cfg.split(",") if item)}})
    c.assign([TopicPartition(topic, int(partition), int(offset))])
    got, end = 0, time.monotonic() + 20
    while got < int(count) and time.monotonic() < end:
        m = c.poll(1)
        if m is None:
            continue
        if m.error():
            print("err", m.error().name(), m.error().str(), flush=True)
            break
        got += 1
        key = m.key().decode() if m.key() is not None else "-"
        print("msg", m.partition(), m.offset(), key, m.value().decode(), m.timestamp()[0], flush=True)
    print("count", got, flush=True)
    c.close()


def metadata(topic):
    c = consumer()
    found = c.list_topics(topic, timeout=10).topics[topic]
    print("metadata", found.error.name() if found.error else "NONE", flush=True)
    c.close()


def offsets(topic, partition, ts):
    c = consumer()
    tp = TopicPartition(topic, int(partition))
    lo, hi = c.get_watermark_offsets(tp, timeout=10)
    print("watermarks", lo, hi, flush=True)
    found = c.offsets_for_times([TopicPartition(topic, int(partition), int(ts))], timeout=10)
    print("time", found[0].offset, flush=True)
    c.close()


def latency(topic):
    c = consumer(**{"fetch.wait.max.ms": 5000, "fetch.min.bytes": 1})
    tp = TopicPartition(topic, 0)
    _, hi = c.get_watermark_offsets(tp, timeout=10)
    c.assign([TopicPartition(topic, 0, hi)])
    c.poll(3)
    sent = {}
    p = Producer({**base(), "linger.ms": 0})
    p.produce(topic, value=b'{"late": 1}', partition=0,
              on_delivery=lambda e, m: sent.setdefault("at", time.monotonic()))
    p.flush(10)
    m = c.poll(10)
    got = time.monotonic()
    if m is None or m.error() or "at" not in sent:
        print("latency none", flush=True)
    else:
        print("latency", int((got - sent["at"]) * 1000), flush=True)
    c.close()


def raw(request):
    host, port = os.environ["BOOTSTRAP"].split(":")
    tls = ssl.create_default_context(cafile="/w/server.crt")
    data = b""
    closed = False
    with connect(host, int(port)) as s, tls.wrap_socket(s, server_hostname=host) as t:
        t.sendall(bytes.fromhex(request))
        t.settimeout(2)
        try:
            while True:
                chunk = t.recv(65536)
                if not chunk:
                    closed = True
                    break
                data += chunk
        except (TimeoutError, socket.timeout):
            pass
        except (ssl.SSLError, ConnectionError):
            closed = True
    if data:
        print("raw", data.hex(), flush=True)
    elif closed:
        print("raw closed", flush=True)
    else:
        print("raw timeout", flush=True)


def produce_partition(topic, partition):
    import struct

    def string(text):
        return struct.pack(">h", len(text.encode())) + text.encode()

    def read(t, n):
        data = b""
        while len(data) < n:
            chunk = t.recv(n - len(data))
            if not chunk:
                raise ConnectionError("the listener closed the connection")
            data += chunk
        return data

    def call(t, key, version, body):
        request = struct.pack(">hhi", key, version, 1) + string("raw") + body
        t.sendall(struct.pack(">i", len(request)) + request)
        return read(t, struct.unpack(">i", read(t, 4))[0])[4:]

    host, port = os.environ["BOOTSTRAP"].split(":")
    tls = ssl.create_default_context(cafile="/w/server.crt")
    with connect(host, int(port)) as s, tls.wrap_socket(s, server_hostname=host) as t:
        call(t, 17, 1, string("PLAIN"))
        auth = f"\0{os.environ['KAFKA_USER']}\0{os.environ['KAFKA_PASSWORD']}".encode()
        call(t, 36, 0, struct.pack(">i", len(auth)) + auth)
        r = call(t, 0, 3, struct.pack(">hhii", -1, -1, 10000, 1) + string(topic)
                 + struct.pack(">iii", 1, int(partition), 0))
    at = 6 + struct.unpack(">h", r[4:6])[0] + 8
    print("produce_partition", struct.unpack(">h", r[at:at + 2])[0], flush=True)


def traffic(topic, seconds):
    p = Producer({**base(), "linger.ms": 5, "message.timeout.ms": 120000})
    c = consumer(**{"auto.offset.reset": "earliest"})
    c.assign([TopicPartition(topic, 0, 0)])
    seen, stop = set(), threading.Event()

    def read():
        while not stop.is_set():
            m = c.poll(0.5)
            if m is not None and not m.error():
                seen.add(m.value().decode())

    reader = threading.Thread(target=read)
    reader.start()
    delivered, failed = set(), []

    def done(err, msg):
        if err:
            failed.append(err.name())
        else:
            delivered.add(msg.value().decode())

    end, i = time.monotonic() + float(seconds), 0
    while time.monotonic() < end:
        p.produce(topic, value=f'{{"i": {i}}}'.encode(), partition=0, on_delivery=done)
        p.poll(0)
        i += 1
        time.sleep(0.01)
    p.flush(120)
    deadline = time.monotonic() + 30
    while not delivered <= seen and time.monotonic() < deadline:
        time.sleep(0.2)
    stop.set()
    reader.join()
    c.close()
    print("sent", i, "delivered", len(delivered), "failed", len(failed),
          "consumed", len(delivered & seen), flush=True)


def idempotent(topic, count):
    p = Producer({**base(), "enable.idempotence": True, "linger.ms": 5, "message.timeout.ms": 120000})
    acked, failed = [], []

    def done(err, msg):
        report(err, msg)
        (failed if err else acked).append(1)

    for i in range(int(count)):
        p.produce(topic, value=str(i).encode(), on_delivery=done)
        p.poll(0)
    p.flush(120)
    print("acked", len(acked), "failed", len(failed), flush=True)


def pipeline(fast, slow):
    p = Producer({**base(), "linger.ms": 0})
    seen = {}

    def done(name):
        return lambda err, msg: seen.setdefault(name, (time.monotonic(), err.name() if err else msg.offset()))

    p.list_topics(fast, timeout=10)
    p.list_topics(slow, timeout=10)
    p.produce(slow, value=b'{"s": -1}', partition=0)
    p.flush(30)
    start = time.monotonic()
    p.produce(fast, value=b'{"f": 1}', partition=0, on_delivery=done("fast"))
    p.poll(0)
    for i in range(3):
        p.produce(slow, value=f'{{"s": {i}}}'.encode(), partition=i, on_delivery=done(f"slow{i}"))
    p.poll(0)
    p.flush(30)
    print("pipeline", *sorted(seen, key=lambda k: seen[k][0]), flush=True)
    print("fast", seen["fast"][1], int((seen["fast"][0] - start) * 1000), flush=True)


def transactional():
    p = Producer({**base(), "transactional.id": "pg_topics_harness"})
    try:
        p.init_transactions(20)
        print("txn NONE", flush=True)
    except KafkaException as e:
        print("txn", e.args[0].name(), flush=True)


def member(topic, group, name, cfg=""):
    c = consumer(**{"group.id": group, "session.timeout.ms": 6000, "heartbeat.interval.ms": 1000,
                    "auto.offset.reset": "earliest",
                    **dict(item.split("=", 1) for item in cfg.split(",") if item)})
    c.subscribe([topic])
    last, read = None, 0
    while not os.path.exists(f"/w/stop-{name}"):
        m = c.poll(0.2)
        if m is not None and not m.error():
            read += 1
            print("msg", m.partition(), m.offset(), flush=True)
        now = ",".join(str(p.partition) for p in sorted(c.assignment(), key=lambda p: p.partition))
        if now != last:
            print("assigned", now or "-", flush=True)
            last = now
    c.close()
    print("closed read", read, flush=True)


def group_offsets(group):
    from confluent_kafka import ConsumerGroupTopicPartitions
    from confluent_kafka.admin import AdminClient
    admin = AdminClient(base())
    for future in admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)]).values():
        for tp in sorted(future.result(timeout=20).topic_partitions, key=lambda p: (p.topic, p.partition)):
            print("offset", tp.topic, tp.partition, tp.offset, tp.error.name() if tp.error else "NONE", flush=True)


def commit(topic, partition, offset, group):
    c = consumer(**{"group.id": group})
    try:
        for tp in c.commit(offsets=[TopicPartition(topic, int(partition), int(offset))], asynchronous=False):
            print("commit", tp.error.name() if tp.error else "NONE", flush=True)
    except KafkaException as e:
        print("commit", e.args[0].name(), flush=True)
    c.close()


def committed(topic, partition, group):
    c = consumer(**{"group.id": group})
    try:
        for tp in c.committed([TopicPartition(topic, int(partition))], timeout=10):
            print("committed", tp.error.name() if tp.error else tp.offset, flush=True)
    except KafkaException as e:
        print("committed", e.args[0].name(), flush=True)
    c.close()


def create_topic_validate_only(topic, partitions):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient(base())
    fs = admin.create_topics([NewTopic(topic, num_partitions=int(partitions), replication_factor=1)],
                              validate_only=True)
    for future in fs.values():
        try:
            future.result(timeout=20)
            print("validate_only NONE", flush=True)
        except KafkaException as e:
            print("validate_only", e.args[0].name(), flush=True)


def create_topic(topic, partitions, replication_factor):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient(base())
    fs = admin.create_topics([NewTopic(topic, num_partitions=int(partitions),
                                       replication_factor=int(replication_factor))])
    for future in fs.values():
        try:
            future.result(timeout=20)
            print("create_topic NONE", flush=True)
        except KafkaException as e:
            print("create_topic", e.args[0].name(), e.args[0].str(), flush=True)


def delete_topic(topic):
    from confluent_kafka.admin import AdminClient
    admin = AdminClient(base())
    for future in admin.delete_topics([topic]).values():
        try:
            future.result(timeout=20)
            print("delete_topic NONE", flush=True)
        except KafkaException as e:
            print("delete_topic", e.args[0].name(), flush=True)


def legacy_alter(topic, pairs):
    from confluent_kafka.admin import AdminClient, ConfigResource
    admin = AdminClient(base())
    set_config = dict(p.split("=", 1) for p in pairs.split(",") if p)
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic, set_config=set_config)
    for future in admin.alter_configs([resource]).values():
        try:
            future.result(timeout=20)
            print("legacy_alter NONE", flush=True)
        except KafkaException as e:
            print("legacy_alter", e.args[0].name(), flush=True)


def incremental_alter(topic, key, op, value=None):
    from confluent_kafka.admin import AdminClient, ConfigResource, ConfigEntry, AlterConfigOpType
    admin = AdminClient(base())
    entry = ConfigEntry(key, value, incremental_operation=AlterConfigOpType[op.upper()])
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic, incremental_configs=[entry])
    for future in admin.incremental_alter_configs([resource]).values():
        try:
            future.result(timeout=20)
            print("incremental_alter NONE", flush=True)
        except KafkaException as e:
            print("incremental_alter", e.args[0].name(), flush=True)


def cluster_id():
    c = consumer()
    print("cluster_id", c.list_topics(timeout=10).cluster_id, flush=True)
    c.close()


def describe_config_sources(topic):
    from confluent_kafka.admin import AdminClient, ConfigResource
    admin = AdminClient(base())
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
    for future in admin.describe_configs([resource]).values():
        for name, entry in sorted(future.result(timeout=20).items()):
            print("config", name, entry.value, int(entry.source), flush=True)


if __name__ == "__main__":
    {"produce": produce, "consume": consume, "offsets": offsets, "latency": latency, "metadata": metadata,
     "traffic": traffic, "raw": raw, "pipeline": pipeline, "member": member, "idempotent": idempotent, "transactional": transactional,
     "group_offsets": group_offsets, "commit": commit, "committed": committed,
     "produce_partition": produce_partition, "produce_headers": produce_headers, "headers": headers,
     "create_topic": create_topic, "create_topic_validate_only": create_topic_validate_only,
     "delete_topic": delete_topic,
     "legacy_alter": legacy_alter, "incremental_alter": incremental_alter, "cluster_id": cluster_id,
     "describe_config_sources": describe_config_sources}[sys.argv[1]](*sys.argv[2:])
