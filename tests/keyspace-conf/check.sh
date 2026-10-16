#!/bin/sh
# Asserts that the keyspace warning tells the truth in both directions.
#
# The entrypoint runs twice on a normal start: once as root, which writes pg_keyspace.conf from
# SUPATYPE_KEYSPACE_*, and again as postgres after the gosu re-exec. The second pass tried the
# write again, could not, and logged "pg_keyspace stays disabled" next to a keyspace that was
# serving. A warning that is wrong on every start is one nobody reads, so the case where it is
# right is asserted too: a container run as postgres from the start cannot write the shipped
# file, and there the keyspace really is off and the warning must say so.
#
# Usage: check.sh <image> [container-name-prefix]
#   Starts and removes its own containers, named <prefix>-on and <prefix>-readonly.
set -eu

image=$1
prefix=${2:-ksconf-check}
on="$prefix-on"
readonly_c="$prefix-readonly"
failed=0
warning='pg_keyspace stays disabled'
denied='pg_keyspace.conf: Permission denied'

cleanup() { docker rm -f "$on" "$readonly_c" > /dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

ok() { echo "ok      $1"; }
fail() { echo "FAIL    $1"; failed=$((failed + 1)); }

start() {
	name=$1
	shift
	docker run -d --name "$name" "$@" \
		-e POSTGRES_PASSWORD=testpassword \
		-e POSTGRES_DB=supatype \
		-e SUPATYPE_KEYSPACE_ENABLED=true \
		-e SUPATYPE_KEYSPACE_KEYS=10000 \
		-e SUPATYPE_KEYSPACE_RING_MB=4 \
		"$image" > /dev/null
}

# Over TCP: the temporary server used for initialisation listens on the socket alone, so a TCP
# answer means the real postmaster, the one whose workers serve RESP, is up.
wait_ready() {
	for _ in $(seq 1 90); do
		docker exec "$1" pg_isready -h 127.0.0.1 -U supatype_admin -q 2>/dev/null && return 0
		sleep 2
	done
	return 1
}

# A RESP PING from inside the container, so nothing is published on the host. Prints the reply's
# first line, or nothing when no listener answers.
resp_ping() {
	docker exec "$1" bash -c '
		exec 3<>/dev/tcp/127.0.0.1/6379 || exit 1
		printf "*1\r\n\$4\r\nPING\r\n" >&3
		timeout 5 head -n 1 <&3 | tr -d "\r"' 2>/dev/null || true
}

# The worker starts after the postmaster, so give it a moment before deciding it is absent.
wait_pong() {
	for _ in $(seq 1 30); do
		[ "$(resp_ping "$1")" = "+PONG" ] && return 0
		sleep 2
	done
	return 1
}

# 1. Enabled the normal way, then restarted: both passes run on each start, the second start over
#    an existing cluster and the file the first one wrote. The keyspace serves throughout, nothing
#    claims otherwise, and each start says once that it is enabled.
start "$on"
if wait_ready "$on"; then ok "enabled: server accepts TCP"; else fail "enabled: server never accepted TCP"; fi
if wait_pong "$on"; then ok "enabled: RESP answers PING with PONG"; else fail "enabled: no PONG on :6379"; fi
docker restart "$on" > /dev/null
if wait_ready "$on"; then ok "restarted: server accepts TCP"; else fail "restarted: server never accepted TCP"; fi
if wait_pong "$on"; then ok "restarted: RESP answers PING with PONG"; else fail "restarted: no PONG on :6379"; fi
logs=$(docker logs "$on" 2>&1)
announced=$(printf '%s\n' "$logs" | grep -c 'pg_keyspace enabled: RESP on :6379' || true)
if [ "$announced" -eq 2 ]; then
	ok "enabled: each of the two starts says the keyspace is enabled, once"
else
	fail "enabled: 'pg_keyspace enabled' logged $announced time(s) over two starts, expected 2"
fi
case "$logs" in
	*"$warning"*) fail "enabled: log says '$warning' although the keyspace serves" ;;
	*) ok "enabled: no '$warning' in the log" ;;
esac
case "$logs" in
	*"$denied"*) fail "enabled: log carries the shell's '$denied'" ;;
	*) ok "enabled: no '$denied' in the log" ;;
esac
[ "$failed" -eq 0 ] || echo "$logs" | grep -i 'keyspace' | tail -20

# 2. Run as postgres from the start: the root-owned shipped file cannot be rewritten, the keyspace
#    stays off, and the warning is the only thing that says why.
before=$failed
start "$readonly_c" --user postgres
if wait_ready "$readonly_c"; then ok "unwritable: server accepts TCP"; else fail "unwritable: server never accepted TCP"; fi
if [ -z "$(resp_ping "$readonly_c")" ]; then ok "unwritable: nothing answers on :6379"; else fail "unwritable: RESP answered although the conf was never written"; fi
logs=$(docker logs "$readonly_c" 2>&1)
case "$logs" in
	*"$warning"*) ok "unwritable: log says '$warning'" ;;
	*) fail "unwritable: keyspace is off and the log does not say so" ;;
esac
[ "$failed" -eq "$before" ] || echo "$logs" | grep -i 'keyspace' | tail -20

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed"
	exit 1
fi
echo "all keyspace conf checks passed"
