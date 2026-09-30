const fs = require('fs');

const [lib, cmd, topic, group, name] = process.argv.slice(2);
const { BOOTSTRAP, KAFKA_USER, KAFKA_PASSWORD, DEBUG } = process.env;

function clients() {
  if (lib === 'kafkajs') {
    const { Kafka, logLevel } = require('kafkajs');
    const kafka = new Kafka({
      brokers: [BOOTSTRAP],
      ssl: { ca: [fs.readFileSync('/w/server.crt')] },
      sasl: { mechanism: 'plain', username: KAFKA_USER, password: KAFKA_PASSWORD },
      logLevel: DEBUG ? logLevel.DEBUG : logLevel.WARN,
    });
    return {
      producer: () => kafka.producer(),
      consumer: () => kafka.consumer({ groupId: group }),
      subscription: { topics: [topic], fromBeginning: true },
    };
  }
  const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;
  const base = {
    'bootstrap.servers': BOOTSTRAP,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': KAFKA_USER,
    'sasl.password': KAFKA_PASSWORD,
    'ssl.ca.location': '/w/server.crt',
    ...(DEBUG ? { debug: DEBUG } : {}),
  };
  const kafka = new Kafka();
  return {
    producer: () => kafka.producer({ ...base, partitioner: 'murmur2_random' }),
    consumer: () => kafka.consumer({ ...base, 'group.id': group, 'auto.offset.reset': 'earliest',
      'auto.commit.interval.ms': 500 }),
    subscription: { topics: [topic] },
  };
}

const c = clients();

async function produce() {
  const p = c.producer();
  await p.connect();
  for (let b = 0; b < 1000; b += 100) {
    const messages = [];
    for (let i = b; i < b + 100; i++) {
      messages.push({ key: `k-${i}`, value: `{"i": ${i}}`, headers: { n: String(i) } });
    }
    await p.send({ topic, messages });
  }
  await p.disconnect();
  console.log('acked 1000');
}

async function member() {
  const k = c.consumer();
  await k.connect();
  await k.subscribe(c.subscription);
  await k.run({ eachMessage: async ({ partition, message }) => console.log('msg', partition, message.offset) });
  while (!fs.existsSync(`/w/stop-${name}`)) {
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  await k.disconnect();
  console.log('closed');
}

({ produce, member })[cmd]().catch((e) => {
  console.log('error', e.name, e.message);
  process.exit(1);
});
