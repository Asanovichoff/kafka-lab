# Kafka Lab

A hands-on playground for the parts of Apache Kafka that come up in system design
interviews and on the first day at a company that runs it: partitions, offsets, keys,
consumer groups, rebalancing, lag, redelivery and replay.

It starts a real Kafka broker, writes a steady stream of fake application logs to it,
and gives you a dashboard where you can poke at it: add and crash consumers, slow them
down, rewind them, and run the same job with and without Kafka to see what changes.

![The log: one topic, its partitions, and messages landing by key](docs/screenshots/01-the-log.png)

## Run it

You need Docker. Nothing else.

```bash
git clone https://github.com/Asanovichoff/kafka-lab.git
cd kafka-lab
docker compose up --build
```

Open <http://localhost:8000>. The first start pulls the `apache/kafka` image, so give
it a minute. Stop with `Ctrl+C`; `docker compose down -v` removes everything.

## What to try

### 1. The log

A topic is an append-only log split into partitions. The dashboard shows the newest
messages of each partition with their offsets.

- Six fake services write logs, keyed by service name. Each service's colour stays in
  one lane, because the key picks the partition.
- Switch to **No key** and the colours scatter across lanes.
- Send your own message with the key `user-42` a few times. Same partition every time,
  with the next offset.

### 2. Consumer groups

![Consumer groups: partition ownership, lag, and a crashed consumer](docs/screenshots/02-consumer-groups.png)

- **Add consumer** and watch Kafka rebalance the partitions between group members.
- **Add group** and see a second group read every message again, with its own offsets.
- Set **Work per message** to 250 ms. Lag grows; the producer does not slow down.
- **Pause** a consumer: it keeps its partitions (it still sends heartbeats) and falls behind.
- **Crash** a consumer: no final commit, no goodbye. About six seconds later
  (`session.timeout.ms`) the broker gives its partitions to someone else, and the
  messages it had processed but not committed are delivered again. That is
  at-least-once delivery, and why consumers should be idempotent.
- **Replay** from the start. Reading never deleted anything, so the whole log is still there.

### 3. With and without Kafka

![Benchmark with one service down](docs/screenshots/04-benchmark-outage.png)

*The screenshots in this README were captured on the mock broker (see below). Run
`make screenshots` against your own lab to replace them with real Kafka numbers.*

One job, done twice: deliver N events to several downstream services that each need a
few milliseconds per event.

| | Without Kafka | With Kafka |
|---|---|---|
| How | The sender POSTs each event to every service over HTTP and waits for the answers | The sender appends to a topic; each service is a consumer group reading at its own pace |
| The sender is busy | Until every service has handled every event | Until the broker has acknowledged the events |
| A service is down | Its events are gone unless you build retries and a buffer | Its events wait in the log; it reads them when it is back |
| More throughput | Add concurrency to the sender, hope the services keep up | Add partitions and consumers |

How to read the result honestly:

- Kafka does not make the services faster. Each one still spends the same time per
  event. What changes is that the sender stops waiting on them, and that several
  consumers can share the work.
- Set **Consumers per service** to 1 and Kafka's total time is about the same as the
  direct calls. The sender is still done almost at once.
- A single message is not faster through Kafka either. The producer batches, so the
  broker's acknowledgement can take longer than one HTTP call. The panel shows both numbers.
- The direct sender handles one event at a time and calls the services in parallel.
  You could give it concurrency, retries and a durable buffer. At that point you have
  written most of a message broker.
- Everything runs on one machine against one broker, with the log stream still
  running in the background. The numbers show the shape of the trade-off; they are
  not a capacity plan.

## Where the ideas live in the code

The code is meant to be read. Each file is short and covers one idea.

| File | What it shows |
|---|---|
| `app/generator.py` | A producer: keys, `acks=all`, idempotence, delivery callbacks |
| `app/tail.py` | Reading partitions directly with `assign()`, no group and no commits |
| `app/groups.py` | Consumer groups: `subscribe()`, rebalance callbacks, manual commits, pause, seek, and what a crash looks like |
| `app/benchmark.py` | The comparison: direct HTTP fan-out against produce and consume |
| `app/broker.py` | Connecting, creating topics, topic configuration |
| `app/main.py` | The FastAPI routes the dashboard calls |
| `docker-compose.yml` | A single-node KRaft broker, with the two-listener setup explained |

The client library is [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python),
the Python wrapper around librdkafka.

## Run the app from source

Useful if you want to change the code and see it reload.

```bash
docker compose up -d kafka            # the broker only, on localhost:9092
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --no-access-log
```

Settings are environment variables: `KAFKA_BOOTSTRAP_SERVERS` (default
`localhost:9092`), `KAFKA_LAB_TOPIC` (`app-logs`), `KAFKA_LAB_PARTITIONS` (`3`) and
`KAFKA_LAB_COMMIT_INTERVAL` (`2` seconds).

### No Docker at all

```bash
KAFKA_LAB_MOCK=1 uvicorn app.main:app --no-access-log
```

This uses the mock cluster built into librdkafka instead of a broker. It speaks enough
of the Kafka protocol for every panel to work, which is what the tests use, but it is
not Kafka: it always has 4 partitions, its timings mean nothing, and it rejects a
commit made during a rebalance, so you will see a few repeated messages after
adding a consumer. The dashboard shows a yellow banner in this mode.

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

The tests start the real app on the mock broker and drive it over HTTP: key routing,
rebalancing, a second group, replay, pause and lag, a crash, and the benchmark with a
service down.

## Screenshots

With the lab running:

```bash
pip install -r requirements-dev.txt
playwright install chromium
python scripts/screenshots.py         # writes docs/screenshots/*.png
```

## What this lab leaves out

It runs one broker, so there is no replication, no in-sync replicas and no leader
election. It also skips transactions and exactly-once, log compaction, Schema
Registry, Kafka Streams and Kafka Connect. Those are the next things to read about.

## License

MIT
# kafka-lab
