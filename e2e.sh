#!/bin/bash
#
# e2e.sh -- end-to-end scenarios for the AxoSyslog kafka() source against the
# broker from compose.yaml.  Every scenario gets a fresh topic and a fresh
# work directory under work/<run-id>/<scenario>/ holding the generated config,
# the persist file, the daemon log and the output file it asserts on.
#
#   e2e.sh [all|delivery|restart|assign|reload]...   run the given scenarios (default: all)
#
# Options via environment:
#   KEEP_BROKER=1      leave the broker running afterwards (default: stop it when
#                      this script started it)
#   KEEP_WORK=1        keep work/<run-id> even when every scenario passed
#   MSG_COUNT=N        messages per scenario step (default 200)
#   TIMEOUT=S          seconds to wait for the output to fill (default 90)
#   SYSLOG_NG_MODE, AXOSYSLOG_DIR, SYSLOG_NG_EXTRA_ARGS   see run-syslog-ng.sh
#   KAFKA_PORT         see kafka.sh

set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
KAFKA="$HERE/kafka.sh"
SNG="$HERE/run-syslog-ng.sh"
TEMPLATE="$HERE/syslog-ng/kafka-source.conf.in"
MSG_COUNT=${MSG_COUNT:-200}
TIMEOUT=${TIMEOUT:-90}
RUN_ID=$(date +%Y%m%d-%H%M%S)-$$
WORK_ROOT="$HERE/work/$RUN_ID"
BOOTSTRAP="localhost:${KAFKA_PORT:-9092}"

PASSED=(); FAILED=()
CURRENT=""

log()  { echo "[$(date +%H:%M:%S)] $*"; }
fail() { echo "[$(date +%H:%M:%S)] FAIL ($CURRENT): $*" >&2; return 1; }

# ---------------------------------------------------------------------------
# helpers

make_config() {   # make_config WORKDIR TOPIC_CLAUSE STRATEGY PERSIST [EXTRA]
	local w=$1 topic_clause=$2 strategy=$3 persist=$4 extra=${5:-}
	sed -e "s|@BOOTSTRAP@|$BOOTSTRAP|" \
	    -e "s|@TOPIC_CLAUSE@|$topic_clause|" \
	    -e "s|@STRATEGY@|$strategy|" \
	    -e "s|@PERSIST_STORE@|$persist|" \
	    -e "s|@OUTPUT@|$w/output.log|" \
	    -e "s|@EXTRA@|$extra|" \
	    "$TEMPLATE" > "$w/syslog-ng.conf"
}

start_sng() { "$SNG" start "$1" "$1/syslog-ng.conf" > /dev/null; "$SNG" wait-ready "$1" 30; }
stop_sng()  { "$SNG" stop "$1"; }

# Messages are keyed ("k<n>" -> "msg-<n>") so the broker spreads them over the
# partitions; unkeyed console-producer batches tend to land on one partition.
produce_range() {   # produce_range TOPIC FROM TO
	seq "$2" "$3" | awk '{ printf "k%s\tmsg-%s\n", $1, $1 }' | "$KAFKA" produce-keyed "$1"
}

partitions_seen() { cut -f2 "$1/output.log" | sort -un | tr '\n' ',' | sed 's/,$//'; }

output_lines() { [ -f "$1/output.log" ] && wc -l < "$1/output.log" || echo 0; }

wait_for_lines() {   # wait_for_lines WORKDIR N  -> wait until output has at least N lines
	local w=$1 n=$2 deadline=$(( $(date +%s) + TIMEOUT ))
	while [ "$(output_lines "$w")" -lt "$n" ]; do
		if [ $(date +%s) -ge $deadline ]; then
			echo "--- tail of $w/syslog-ng.log ---" >&2; tail -30 "$w/syslog-ng.log" >&2
			fail "expected $n lines in output, got $(output_lines "$w") after ${TIMEOUT}s"
			return 1
		fi
		"$SNG" running "$w" || { tail -30 "$w/syslog-ng.log" >&2; fail "syslog-ng died"; return 1; }
		sleep 0.5
	done
	# a short grace period catches over-delivery (duplicates arriving late)
	sleep 2
}

assert_messages() {   # assert_messages WORKDIR FROM TO  -> output MESSAGE column == {msg-FROM..msg-TO}, no dups
	local w=$1 from=$2 to=$3
	local got expected
	got=$(cut -f5 "$w/output.log" | sort)
	expected=$(seq "$from" "$to" | sed 's/^/msg-/' | sort)
	if [ "$got" != "$expected" ]; then
		echo "--- unexpected or missing messages (<: got only, >: expected only) ---" >&2
		diff <(echo "$got") <(echo "$expected") | head -20 >&2
		fail "output does not match msg-$from..msg-$to ($(echo "$got" | wc -l) lines, $(echo "$got" | sort -u | wc -l) unique)"
		return 1
	fi
}

assert_metadata() {   # assert_metadata WORKDIR TOPIC  -> every line carries topic, numeric partition/offset
	local w=$1 topic=$2 bad
	bad=$(awk -F'\t' -v t="$topic" '$1 != t || $2 !~ /^[0-9]+$/ || $3 !~ /^[0-9]+$/' "$w/output.log" | head -3)
	[ -z "$bad" ] || { echo "$bad" >&2; fail "metadata columns are wrong on some lines"; return 1; }
}

new_workdir() { local w="$WORK_ROOT/$1"; mkdir -p "$w"; echo "$w"; }

# ---------------------------------------------------------------------------
# scenarios

# All partitions of a 3-partition topic, subscribe strategy: every produced
# message shows up exactly once and carries its Kafka metadata.
scenario_delivery() {
	local topic="e2e-delivery-$RUN_ID" w
	w=$(new_workdir delivery)
	"$KAFKA" topic-create "$topic" 3 > /dev/null
	make_config "$w" "topic(\"$topic\" => \"-1\")" subscribe local
	start_sng "$w"
	produce_range "$topic" 1 "$MSG_COUNT"
	wait_for_lines "$w" "$MSG_COUNT"
	stop_sng "$w"
	assert_messages "$w" 1 "$MSG_COUNT"
	assert_metadata "$w" "$topic"
	local parts; parts=$(partitions_seen "$w")
	[ "$(echo "$parts" | tr ',' '\n' | wc -l)" -ge 2 ] || { fail "messages came from a single partition ($parts), expected a spread"; return 1; }
	log "delivery: $MSG_COUNT messages over partitions $parts"
}

# Local bookmarks survive a restart: messages produced while syslog-ng is down
# are picked up afterwards, nothing already delivered is delivered again.
scenario_restart() {
	local topic="e2e-restart-$RUN_ID" w
	w=$(new_workdir restart)
	"$KAFKA" topic-create "$topic" 3 > /dev/null
	make_config "$w" "topic(\"$topic\" => \"-1\")" subscribe local
	start_sng "$w"
	produce_range "$topic" 1 "$MSG_COUNT"
	wait_for_lines "$w" "$MSG_COUNT"
	stop_sng "$w"
	assert_messages "$w" 1 "$MSG_COUNT"

	produce_range "$topic" $((MSG_COUNT + 1)) $((2 * MSG_COUNT))
	start_sng "$w"
	wait_for_lines "$w" $((2 * MSG_COUNT))
	# a second restart with nothing new must not re-deliver anything either
	stop_sng "$w"
	start_sng "$w"
	sleep 3
	stop_sng "$w"
	assert_messages "$w" 1 $((2 * MSG_COUNT))
	log "restart: $((2 * MSG_COUNT)) messages, no duplicates across two restarts"
}

# Explicit partition assignment: only the listed partitions of the topic are
# read, and exactly the messages the broker holds there arrive.
scenario_assign() {
	local topic="e2e-assign-$RUN_ID" w
	w=$(new_workdir assign)
	"$KAFKA" topic-create "$topic" 3 > /dev/null
	# produced before syslog-ng starts: read-old-records() defaults to yes, so
	# a fresh persist state starts the assigned partitions at the beginning
	produce_range "$topic" 1 "$MSG_COUNT"
	local n0 n2 expected
	n0=$("$KAFKA" count "$topic" 0); n2=$("$KAFKA" count "$topic" 2); expected=$((n0 + n2))
	[ "$expected" -gt 0 ] || { fail "no messages landed on partitions 0 and 2"; return 1; }
	make_config "$w" "topic(\"$topic\" => \"0,2\")" assign local
	start_sng "$w"
	wait_for_lines "$w" "$expected"
	stop_sng "$w"
	local parts; parts=$(partitions_seen "$w")
	case "$parts" in 0,2|0|2) ;; *) fail "unexpected partitions in output: $parts"; return 1 ;; esac
	[ "$(output_lines "$w")" -eq "$expected" ] || { fail "expected $expected lines (p0=$n0 p2=$n2), got $(output_lines "$w")"; return 1; }
	# keys came through and every value matches its key
	local badkeys; badkeys=$(awk -F'\t' '{ sub(/^k/, "", $4); if ("msg-" $4 != $5) print }' "$w/output.log" | head -3)
	[ -z "$badkeys" ] || { echo "$badkeys" >&2; fail "key/value mismatch"; return 1; }
	assert_metadata "$w" "$topic"
	log "assign: $expected messages from partitions 0 and 2 (p0=$n0 p2=$n2), keys intact"
}

# A config reload (SIGHUP) in the middle of a stream loses nothing and
# duplicates nothing.
scenario_reload() {
	local topic="e2e-reload-$RUN_ID" w
	w=$(new_workdir reload)
	"$KAFKA" topic-create "$topic" 3 > /dev/null
	make_config "$w" "topic(\"$topic\" => \"-1\")" subscribe local
	start_sng "$w"
	produce_range "$topic" 1 "$MSG_COUNT"
	wait_for_lines "$w" "$MSG_COUNT"
	"$SNG" ctl "$w" reload > /dev/null
	produce_range "$topic" $((MSG_COUNT + 1)) $((2 * MSG_COUNT))
	wait_for_lines "$w" $((2 * MSG_COUNT))
	stop_sng "$w"
	assert_messages "$w" 1 $((2 * MSG_COUNT))
	log "reload: $((2 * MSG_COUNT)) messages across a reload, no duplicates"
}

# ---------------------------------------------------------------------------
# driver

run_scenario() {
	CURRENT=$1
	log "=== $1 ==="
	# a subshell with its own errexit: the first failing step ends the scenario
	# (errexit is suspended inside an `if` condition, hence not `if scenario_x`)
	set +e
	( set -e; "scenario_$1" )
	local rc=$?
	set -e
	if [ $rc -eq 0 ]; then
		PASSED+=("$1")
	else
		FAILED+=("$1")
		# never leave an instance behind
		"$SNG" stop "$WORK_ROOT/$1" 2> /dev/null || true
	fi
}

scenarios=("$@")
[ ${#scenarios[@]} -eq 0 ] || [ "${scenarios[0]}" = all ] && scenarios=(delivery restart assign reload)

mkdir -p "$WORK_ROOT"
log "run id $RUN_ID, work dir $WORK_ROOT, syslog-ng mode: $("$SNG" mode)"

STARTED_BROKER=0
if [ "$(docker inspect --format '{{.State.Health.Status}}' "${KAFKA_CONTAINER:-axosyslog-kafka}" 2>/dev/null)" != healthy ]; then
	"$KAFKA" up
	STARTED_BROKER=1
fi

for s in "${scenarios[@]}"; do run_scenario "$s"; done

if [ $STARTED_BROKER -eq 1 ] && [ "${KEEP_BROKER:-0}" != 1 ]; then
	"$KAFKA" down > /dev/null
fi

echo
log "passed: ${PASSED[*]:-none}"
[ ${#FAILED[@]} -eq 0 ] || log "FAILED: ${FAILED[*]}"
if [ ${#FAILED[@]} -eq 0 ] && [ "${KEEP_WORK:-0}" != 1 ]; then
	rm -rf "$WORK_ROOT"
else
	log "work dir kept: $WORK_ROOT"
fi
[ ${#FAILED[@]} -eq 0 ]
