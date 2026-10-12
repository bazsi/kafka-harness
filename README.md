# kafka() source e2e harness

A single-node Kafka broker in a container plus scripts that run AxoSyslog
against it and assert on what the `kafka()` source delivers. It complements
the light tests in the AxoSyslog tree, which only cover the no-broker paths.

```
compose.yaml                   KRaft broker, apache/kafka image, port 9092
kafka.sh                       broker lifecycle + Kafka CLI wrappers (topics, produce, consume, groups)
run-syslog-ng.sh               start/stop an AxoSyslog instance, native or dbld build
syslog-ng/kafka-source.conf.in config template the scenarios instantiate
e2e.sh                         the scenarios
work/                          per-run state: configs, persist files, daemon logs, outputs
```

## Quick start

```sh
cd /home/bazsi/src/syslog-ng/kafka-container
./e2e.sh                 # broker up, all scenarios, broker down
./e2e.sh restart assign  # a subset
KEEP_BROKER=1 ./e2e.sh   # leave the broker running for manual poking
```

The syslog-ng binary comes from the AxoSyslog checkout in `AXOSYSLOG_DIR`
(default `/home/bazsi/src/syslog-ng/.kafka-source`). A cmake/autotools
install under `build/install/` runs natively; a dbld build runs inside the
dbld devshell container with host networking, the way `dbld/rules` runs it.
`SYSLOG_NG_MODE=native|dbld` forces one. Build first with `axosyslog-build`.

## Scenarios

| name | what it proves |
|---|---|
| `delivery` | all partitions of a 3-partition topic, `subscribe` strategy: every message arrives exactly once with topic, partition and offset metadata |
| `restart` | local bookmarks: messages produced while syslog-ng is down are delivered after restart, nothing is re-delivered, a second idle restart delivers nothing |
| `assign` | `assign` strategy with `topic("t" => "0,2")`: exactly the messages the broker holds on partitions 0 and 2 arrive, keys intact |
| `reload` | a `syslog-ng-ctl reload` mid-stream loses and duplicates nothing |

Each scenario uses its own topic (`e2e-<name>-<run-id>`) and work directory, so
runs do not interfere and a failed run can be inspected in
`work/<run-id>/<scenario>/`: `syslog-ng.conf`, `syslog-ng.log`,
`syslog-ng.persist`, `output.log` (tab separated: topic, partition, offset,
key, message). Passed runs clean up unless `KEEP_WORK=1`.

Knobs: `MSG_COUNT` (200), `TIMEOUT` (90 s per wait), `SYSLOG_NG_EXTRA_ARGS`
(e.g. `-d` for debug-level daemon logs; add `kafka-logging("kafka")` via the
template's `@EXTRA@` placeholder for librdkafka's own log lines).

Messages are produced keyed (`k<n>` -> `msg-<n>`) so they spread over the
partitions; the output's key column lets scenarios check key passthrough.

Lesson learned while building this: `poll-timeout()` and
`state-update-timeout()` take milliseconds. A `state-update-timeout(5)` makes
every seek time out, and in `assign` mode the driver then revokes and
re-assigns its partitions in a tight loop without ever consuming.

## Manual use

```sh
./kafka.sh up
./kafka.sh topic-create demo 3
seq 1 10 | sed 's/^/hello /' | ./kafka.sh produce demo
./kafka.sh consume demo            # prints partition, offset, key, value
./kafka.sh count demo 1            # messages on partition 1
./kafka.sh groups; ./kafka.sh group-describe <group>
./kafka.sh cli kafka-configs.sh --bootstrap-server localhost:9092 --describe --entity-type brokers --all
./kafka.sh down
```

To run syslog-ng by hand against it, instantiate the template and use the
runner:

```sh
mkdir -p work/manual
sed -e 's|@BOOTSTRAP@|localhost:9092|' -e 's|@TOPIC_CLAUSE@|topic("demo" => "-1")|' \
    -e 's|@STRATEGY@|subscribe|' -e 's|@PERSIST_STORE@|local|' \
    -e "s|@OUTPUT@|$PWD/work/manual/output.log|" -e 's|@EXTRA@||' \
    syslog-ng/kafka-source.conf.in > work/manual/syslog-ng.conf
./run-syslog-ng.sh start work/manual work/manual/syslog-ng.conf
./run-syslog-ng.sh ctl work/manual stats
./run-syslog-ng.sh stop work/manual
```

## Notes

- The broker advertises `localhost:9092`, so only clients on this host (or in
  host-networked containers) can use it. Change `KAFKA_PORT` if 9092 is taken.
- Broker data lives inside the container; `./kafka.sh reset` gives a clean
  broker. Topics are never auto-created, scenarios create theirs explicitly.
- Everything under `work/` is disposable.
