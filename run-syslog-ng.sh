#!/bin/bash
#
# run-syslog-ng.sh -- start/stop an AxoSyslog instance for the e2e harness.
#
#   run-syslog-ng.sh start WORKDIR CONFIG    start in the background; state goes to WORKDIR
#   run-syslog-ng.sh stop WORKDIR            SIGTERM and wait for exit
#   run-syslog-ng.sh wait-ready WORKDIR [S]  block until the control socket answers
#   run-syslog-ng.sh ctl WORKDIR ARGS...     syslog-ng-ctl against the running instance
#   run-syslog-ng.sh running WORKDIR         exit 0 if the instance is alive
#   run-syslog-ng.sh mode                    print the mode that would be used
#
# Where the binary comes from (SYSLOG_NG_MODE=auto|native|dbld, default auto):
#   native  AXOSYSLOG_DIR/build/install/sbin/syslog-ng, a cmake or autotools
#           install, run directly on the host
#   dbld    AXOSYSLOG_DIR/dbld/install/sbin/syslog-ng, which only runs inside the
#           dbld devshell container (it is linked against /install there); the
#           container is started with --network=host and the worktree, dbld/
#           and WORKDIR mounted at their host paths, the same way dbld/rules does
#   auto    native if its binary exists, else dbld
#
# Environment: AXOSYSLOG_DIR (default: /home/bazsi/src/syslog-ng/.kafka-source),
# SYSLOG_NG_EXTRA_ARGS (e.g. "-v"), DBLD_IMAGE (ghcr.io/axoflow/axosyslog-dbld-devshell).

set -euo pipefail

AXOSYSLOG_DIR=${AXOSYSLOG_DIR:-/home/bazsi/src/syslog-ng/.kafka-source}
SYSLOG_NG_MODE=${SYSLOG_NG_MODE:-auto}
DBLD_IMAGE=${DBLD_IMAGE:-ghcr.io/axoflow/axosyslog-dbld-devshell}
NATIVE_BIN="$AXOSYSLOG_DIR/build/install/sbin/syslog-ng"
DBLD_BIN="$AXOSYSLOG_DIR/dbld/install/sbin/syslog-ng"

die() { echo "run-syslog-ng.sh: $*" >&2; exit 1; }

resolve_mode() {
	case "$SYSLOG_NG_MODE" in
	native) [ -x "$NATIVE_BIN" ] || die "no native binary at $NATIVE_BIN"; echo native ;;
	dbld)   [ -x "$DBLD_BIN" ] || die "no dbld binary at $DBLD_BIN"; echo dbld ;;
	auto)
		if [ -x "$NATIVE_BIN" ]; then echo native
		elif [ -x "$DBLD_BIN" ]; then echo dbld
		else die "no syslog-ng found under $AXOSYSLOG_DIR (build it with axosyslog-build first)"; fi ;;
	*) die "SYSLOG_NG_MODE must be auto, native or dbld" ;;
	esac
}

container_name() { echo "axosyslog-e2e-$(basename "$1")"; }

# the daemon's own state files all live in WORKDIR, so native and dbld runs look the same
daemon_args() {
	local w=$1 cfg=$2
	echo -F -e --no-caps -f "$cfg" \
		--persist-file "$w/syslog-ng.persist" --pidfile "$w/syslog-ng.pid" --control "$w/syslog-ng.ctl" \
		${SYSLOG_NG_EXTRA_ARGS:-}
}

start() {
	local w=$1 cfg=$2 mode
	[ -d "$w" ] || die "workdir $w does not exist"
	[ -f "$cfg" ] || die "config $cfg does not exist"
	w=$(cd "$w" && pwd); cfg=$(cd "$(dirname "$cfg")" && pwd)/$(basename "$cfg")
	mode=$(resolve_mode)
	rm -f "$w/syslog-ng.ctl" "$w/syslog-ng.pid"
	echo "$mode" > "$w/.mode"
	case "$mode" in
	native)
		nohup "$NATIVE_BIN" $(daemon_args "$w" "$cfg") >> "$w/syslog-ng.log" 2>&1 &
		echo $! > "$w/.host-pid"
		;;
	dbld)
		docker run -d --rm --name "$(container_name "$w")" --network=host \
			-e USER_NAME_ON_HOST="$(id -un)" -e SOURCE_DIR_ON_HOST="$AXOSYSLOG_DIR" \
			--entrypoint /dbld/images/entrypoint.sh \
			-v "$AXOSYSLOG_DIR:$AXOSYSLOG_DIR" \
			-v "$AXOSYSLOG_DIR/dbld:/dbld" \
			-v "$AXOSYSLOG_DIR/dbld/install:/install" \
			-v "$w:$w" $( [ "$(dirname "$cfg")" = "$w" ] || echo "-v $(dirname "$cfg"):$(dirname "$cfg")" ) \
			"$DBLD_IMAGE" bash -c "exec /install/sbin/syslog-ng $(daemon_args "$w" "$cfg") >> '$w/syslog-ng.log' 2>&1" > /dev/null
		;;
	esac
	echo "syslog-ng ($mode) starting, log: $w/syslog-ng.log"
}

running() {
	local w=$1
	case "$(cat "$w/.mode" 2>/dev/null)" in
	native) [ -f "$w/.host-pid" ] && kill -0 "$(cat "$w/.host-pid")" 2>/dev/null ;;
	dbld)   [ "$(docker inspect --format '{{.State.Running}}' "$(container_name "$w")" 2>/dev/null)" = "true" ] ;;
	*) return 1 ;;
	esac
}

ctl() {
	local w=$1; shift
	case "$(cat "$w/.mode" 2>/dev/null)" in
	native) "$(dirname "$NATIVE_BIN")/syslog-ng-ctl" "$@" --control="$w/syslog-ng.ctl" ;;
	dbld)   docker exec "$(container_name "$w")" /install/sbin/syslog-ng-ctl "$@" --control="$w/syslog-ng.ctl" ;;
	*) die "no instance recorded in $w" ;;
	esac
}

wait_ready() {
	local w=$1 deadline=$(( $(date +%s) + ${2:-30} ))
	while true; do
		running "$w" || { echo "--- syslog-ng.log ---" >&2; tail -20 "$w/syslog-ng.log" >&2; die "syslog-ng exited before becoming ready"; }
		[ -S "$w/syslog-ng.ctl" ] && ctl "$w" stats > /dev/null 2>&1 && return 0
		[ $(date +%s) -lt $deadline ] || die "syslog-ng did not become ready in time"
		sleep 0.5
	done
}

stop() {
	local w=$1
	running "$w" || return 0
	case "$(cat "$w/.mode")" in
	native)
		kill -TERM "$(cat "$w/.host-pid")" 2>/dev/null || true
		for _ in $(seq 1 60); do running "$w" || break; sleep 0.5; done
		running "$w" && { echo "syslog-ng did not stop, killing" >&2; kill -KILL "$(cat "$w/.host-pid")"; }
		;;
	dbld)
		docker stop -t 30 "$(container_name "$w")" > /dev/null
		;;
	esac
	rm -f "$w/.host-pid"
}

cmd=${1:-}; shift || true
case "$cmd" in
start)      start "$1" "$2" ;;
stop)       stop "$1" ;;
wait-ready) wait_ready "$1" "${2:-30}" ;;
ctl)        w=$1; shift; ctl "$w" "$@" ;;
running)    running "$1" ;;
mode)       resolve_mode ;;
""|-h|--help|help) sed -n '2,24p' "$0" ;;
*) die "unknown command '$cmd' (try: $0 help)" ;;
esac
