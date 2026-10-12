#!/bin/bash
#
# kafka.sh -- manage the e2e Kafka broker and talk to it with the Kafka CLI
# tools shipped in the container.
#
#   kafka.sh up                          start the broker, wait until healthy
#   kafka.sh down                        stop it, keep nothing (data is in the container)
#   kafka.sh reset                       down + up: a broker with no topics
#   kafka.sh status                      container + health state
#   kafka.sh logs [-f]                   broker log
#   kafka.sh topic-create NAME [PARTS]   create NAME with PARTS partitions (default 3)
#   kafka.sh topic-delete NAME
#   kafka.sh topic-list
#   kafka.sh topic-describe NAME
#   kafka.sh produce TOPIC               one message per stdin line
#   kafka.sh produce-keyed TOPIC         stdin lines are "key<TAB>value"
#   kafka.sh consume TOPIC [N] [PART]    print messages from the beginning, stop
#                                        after N (default: all currently there);
#                                        PART restricts to one partition
#   kafka.sh count TOPIC [PART]          number of messages in TOPIC (or one partition)
#   kafka.sh groups                      consumer groups
#   kafka.sh group-describe GROUP        committed offsets / lag of GROUP
#   kafka.sh cli CMD [ARGS...]           run /opt/kafka/bin/CMD inside the container
#
# Environment: KAFKA_PORT (9092), KAFKA_IMAGE, KAFKA_CONTAINER (axosyslog-kafka).

set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export KAFKA_PORT=${KAFKA_PORT:-9092}
export KAFKA_CONTAINER=${KAFKA_CONTAINER:-axosyslog-kafka}
BOOTSTRAP="localhost:$KAFKA_PORT"
COMPOSE=(docker compose -f "$HERE/compose.yaml")

die() { echo "kafka.sh: $*" >&2; exit 1; }

health() { docker inspect --format '{{.State.Health.Status}}' "$KAFKA_CONTAINER" 2>/dev/null || echo "absent"; }

wait_healthy() {
	local deadline=$(( $(date +%s) + ${1:-90} ))
	while [ "$(health)" != "healthy" ]; do
		[ $(date +%s) -lt $deadline ] || die "broker did not become healthy in time (state: $(health))"
		sleep 1
	done
}

cli() { "${COMPOSE[@]}" exec -T kafka "/opt/kafka/bin/$1" "${@:2}"; }

cmd=${1:-}; shift || true
case "$cmd" in
up)
	"${COMPOSE[@]}" up -d
	wait_healthy 120
	echo "kafka: ready at $BOOTSTRAP"
	;;
down)
	"${COMPOSE[@]}" down -v --remove-orphans
	;;
reset)
	"$0" down
	"$0" up
	;;
status)
	"${COMPOSE[@]}" ps
	echo "health: $(health)"
	;;
logs)
	"${COMPOSE[@]}" logs "$@" kafka
	;;
topic-create)
	[ -n "${1:-}" ] || die "topic-create needs a name"
	cli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
		--topic "$1" --partitions "${2:-3}" --replication-factor 1
	;;
topic-delete)
	[ -n "${1:-}" ] || die "topic-delete needs a name"
	cli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --delete --if-exists --topic "$1"
	;;
topic-list)
	cli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
	;;
topic-describe)
	[ -n "${1:-}" ] || die "topic-describe needs a name"
	cli kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$1"
	;;
produce)
	[ -n "${1:-}" ] || die "produce needs a topic"
	cli kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic "$1" \
		--request-required-acks 1 > /dev/null
	;;
produce-keyed)
	[ -n "${1:-}" ] || die "produce-keyed needs a topic"
	cli kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic "$1" \
		--request-required-acks 1 \
		--property parse.key=true --property "key.separator=	" > /dev/null
	;;
consume)
	[ -n "${1:-}" ] || die "consume needs a topic"
	args=(--bootstrap-server "$BOOTSTRAP" --topic "$1" --timeout-ms 5000
	      --property print.key=true --property print.partition=true --property print.offset=true
	      --property key.separator=$'\t')
	if [ -n "${3:-}" ]; then
		args+=(--partition "$3" --offset earliest)
	else
		args+=(--from-beginning)
	fi
	[ -n "${2:-}" ] && args+=(--max-messages "$2")
	cli kafka-console-consumer.sh "${args[@]}" 2> >(grep -v 'TimeoutException\|Processed a total of' >&2 || true)
	;;
count)
	[ -n "${1:-}" ] || die "count needs a topic"
	# kafka-get-offsets prints "topic:partition:end-offset" per partition
	cli kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP" --topic "$1" --time latest |
		awk -F: -v part="${2:-}" '{ if (part == "" || $2 == part) sum += $3 } END { print sum + 0 }'
	;;
groups)
	cli kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --list
	;;
group-describe)
	[ -n "${1:-}" ] || die "group-describe needs a group"
	cli kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --describe --group "$1"
	;;
cli)
	[ -n "${1:-}" ] || die "cli needs a command"
	cli "$@"
	;;
""|-h|--help|help)
	sed -n '2,30p' "$0"
	;;
*)
	die "unknown command '$cmd' (try: $0 help)"
	;;
esac
