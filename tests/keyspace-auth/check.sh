#!/bin/sh
# Asserts that the image's RESP keyspace authenticates every command, not only the keyed ones.
#
# Found in the 0.9.2 release test: with a credential loaded, an unauthenticated client could still
# PING, DBSIZE, list every key name with KEYS or SCAN, and FLUSHALL, because the gate covered only
# commands that take keys. And with no credential at all, which is every fresh start, the keyspace
# served everyone, on every interface.
#
# 1. SUPATYPE_KEYSPACE_PASSWORD set: unauthenticated PING, KEYS, FLUSHALL, SCAN, DBSIZE are refused;
#    AUTH supatype <secret> opens SET, GET, SCAN and pub/sub; the secret is in no log or conf file.
# 2. The same volume restarted with a different password: the old one is refused, the new works.
# 3. pg_keyspace.require_auth on and no credential: everything is refused, no password works.
#    pg_keyspace.listen_address = 127.0.0.1: loopback answers, the container's own address does not.
# 4. No password and neither setting: unchanged, RESP answers without AUTH on every interface.
# 5. SUPATYPE_KEYSPACE_DATABASE names a database other than POSTGRES_DB: the image creates it, and
#    the extension and the credential in it, where the worker connects, on the first start and again
#    on a later one.
# 6. SUPATYPE_KEYSPACE_PASSWORD_FILE names a file holding the password (trailing newline and all):
#    it works as the variable does. Both set, or a file holding only whitespace: the container
#    refuses to start and says why.
#
# Usage: check.sh <image> [container-name-prefix]
#   Starts and removes its own containers and volume, all named after the prefix.
set -eu
# `KEYS *` is a command here, never a glob.
set -f

ks_db=''
pw_file=''

image=$1
prefix=${2:-ksauth-check}
first="$prefix-first"
second="$prefix-second"
closed="$prefix-closed"
open_c="$prefix-open"
otherdb="$prefix-otherdb"
fromfile="$prefix-fromfile"
both="$prefix-both"
volume="$prefix-data"
secret1='first secret, with spaces'
secret2='second-secret'
failed=0
noauth='-NOAUTH Authentication required.'
wrongpass='-WRONGPASS invalid username-password pair or user is disabled.'

cleanup() {
	docker rm -f "$first" "$second" "$closed" "$open_c" "$otherdb" "$fromfile" "$both" > /dev/null 2>&1 || true
	docker volume rm "$volume" > /dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

ok() { echo "ok      $1"; }
fail() { echo "FAIL    $1"; failed=$((failed + 1)); }

# create <name> <keyspace password, '' for none> [postgres arguments...] makes a container without
# starting it; start does both. Every container mounts the one volume, removed first except across
# the password change, which is what it is shared for. $ks_db, when set, becomes
# SUPATYPE_KEYSPACE_DATABASE, and $pw_file SUPATYPE_KEYSPACE_PASSWORD_FILE; empty means unset.
create() {
	name=$1
	pass=$2
	shift 2
	docker create --name "$name" \
		-v "$volume:/var/lib/postgresql/data" \
		-e "SUPATYPE_KEYSPACE_PASSWORD=$pass" \
		-e "SUPATYPE_KEYSPACE_PASSWORD_FILE=$pw_file" \
		-e "SUPATYPE_KEYSPACE_DATABASE=$ks_db" \
		-e POSTGRES_PASSWORD=testpassword \
		-e POSTGRES_DB=supatype \
		-e SUPATYPE_KEYSPACE_ENABLED=true \
		-e SUPATYPE_KEYSPACE_KEYS=10000 \
		-e SUPATYPE_KEYSPACE_RING_MB=4 \
		"$image" postgres -c config_file=/etc/postgresql/postgresql.conf "$@" > /dev/null
}

start() {
	create "$@"
	docker start "$1" > /dev/null
}

# A password file in the container at $pw_file, written before its first start, the way a Docker
# or Kubernetes secret would be mounted. With a trailing newline, as an editor would leave one.
start_with_password_file() {
	create "$1" ''
	# Streamed as a tar so no host path is handed to docker, which a Windows shell would rewrite.
	# Mode 0644 like a mounted secret: the extension reads it as the postgres user.
	dir=$(mktemp -d)
	printf '%s\n' "$2" > "$dir/$(basename "$pw_file")"
	chmod 644 "$dir/$(basename "$pw_file")"
	tar -C "$dir" --owner=0 --group=0 -cf - "$(basename "$pw_file")" |
		docker cp - "$1:$(dirname "$pw_file")" > /dev/null
	rm -r "$dir"
	docker start "$1" > /dev/null
}

# One command's arguments joined by the unit separator, so an argument may hold spaces.
US=$(printf '\037')
c() {
	(
		IFS=$US
		echo "$*"
	)
}

# Run inside the container: `send <fd> <command>...` writes each command as a RESP array of bulk
# strings. Encoding happens there because a shell command substitution here would strip the
# trailing CRLF every frame ends with.
resp_sh='
send() {
	fd=$1
	shift
	for line in "$@"; do
		IFS=$(printf "\037") read -r -a f <<< "$line"
		printf "*%d\r\n" "${#f[@]}" >&"$fd"
		for a in "${f[@]}"; do printf "\$%d\r\n%s\r\n" "${#a}" "$a" >&"$fd"; done
	done
}
'

# talk <container> <host> <command>...: send the commands and a QUIT from inside the container, so
# nothing is published on the host, and print every reply line. Nothing when no listener answers.
talk() {
	ctr=$1
	host=$2
	shift 2
	docker exec "$ctr" bash -c "$resp_sh"'
		host=$1
		shift
		exec 3<>"/dev/tcp/$host/6379" || exit 1
		send 3 "$@" QUIT
		timeout 5 cat <&3 | tr -d "\r"' _ "$host" "$@" 2> /dev/null || true
}

# Over TCP: the temporary server used for initialisation listens on the socket alone, so a TCP
# answer means the real postmaster, the one whose workers serve RESP, is up.
wait_ready() {
	for _ in $(seq 1 90); do
		docker exec "$1" pg_isready -h 127.0.0.1 -U supatype_admin -q 2> /dev/null && return 0
		sleep 2
	done
	return 1
}

# Any reply to PING, refused or not, means the worker is serving.
wait_resp() {
	for _ in $(seq 1 30); do
		[ -n "$(talk "$1" 127.0.0.1 PING)" ] && return 0
		sleep 2
	done
	return 1
}

ready() {
	if wait_ready "$1" && wait_resp "$1"; then ok "$2: server and RESP are up"; else fail "$2: never came up"; fi
}

# expect <label> <want> <got>: the reply lines must equal <want> exactly.
expect() {
	if [ "$3" = "$2" ]; then ok "$1"; else fail "$1: got [$3], want [$2]"; fi
}

lines() { printf '%s\n' "$@"; }

# sql <container> <query> [database, default supatype]
sql() {
	docker exec -e PGPASSWORD=testpassword "$1" psql -h 127.0.0.1 -U supatype_admin -d "${3:-supatype}" -tAc "$2"
}

# Each command on its own connection, so one command's state (MULTI, a subscription) cannot change
# what the next is told, and the first line of each reply.
refused_everywhere() {
	got=''
	for word in PING 'KEYS *' FLUSHALL 'SCAN 0' DBSIZE INFO 'CLIENT ID' RANDOMKEY 'SUBSCRIBE ch'; do
		# shellcheck disable=SC2086 # each word is a command line, split on purpose
		got="$got$(talk "$1" 127.0.0.1 "$(c $word)" | head -n 1)|"
	done
	want="$noauth|$noauth|$noauth|$noauth|$noauth|$noauth|$noauth|$noauth|$noauth|"
	expect "$2: PING KEYS FLUSHALL SCAN DBSIZE INFO CLIENT RANDOMKEY SUBSCRIBE refused without AUTH" "$want" "$got"
}

# ---- 1. SUPATYPE_KEYSPACE_PASSWORD set ----------------------------------------------------------
start "$first" "$secret1"
ready "$first" "password"
refused_everywhere "$first" "password"

login=$(c AUTH supatype "$secret1")
got=$(talk "$first" 127.0.0.1 "$login" "$(c SET k v)" "$(c GET k)" DBSIZE)
expect "password: AUTH then SET, GET, DBSIZE work" "$(lines +OK +OK '$1' v :1 +OK)" "$got"
got=$(talk "$first" 127.0.0.1 "$login" "$(c SCAN 0)" | tr '\n' ' ')
expect "password: SCAN after AUTH lists the key" "+OK *2 \$1 0 *1 \$1 k +OK " "$got"
got=$(talk "$first" 127.0.0.1 "$(c HELLO 3 AUTH supatype "$secret1")" PING | head -n 1)
expect "password: HELLO 3 AUTH logs in" "%7" "$got"

got=$(docker exec "$first" bash -c "$resp_sh"'
	exec 3<>/dev/tcp/127.0.0.1/6379 || exit 1
	send 3 "$1" "$2"
	sleep 1
	exec 4<>/dev/tcp/127.0.0.1/6379 || exit 1
	send 4 "$1" "$3" QUIT
	timeout 3 cat <&4 | tr -d "\r" | sed "s/^/pub /"
	timeout 2 cat <&3 | tr -d "\r" | sed "s/^/sub /"' _ \
	"$login" "$(c SUBSCRIBE ch)" "$(c PUBLISH ch hello)" 2> /dev/null || true)
case "$got" in
	*"pub :1"*"sub hello"*) ok "password: SUBSCRIBE and PUBLISH work after AUTH" ;;
	*) fail "password: pub/sub after AUTH: [$got]" ;;
esac

leaks=$( {
	docker logs "$first" 2>&1
	docker exec "$first" sh -c 'cat /etc/postgresql-custom/*.conf /etc/postgresql/*.conf /var/lib/postgresql/data/*.conf' 2> /dev/null
} | grep -cF "$secret1" || true)
expect "password: the secret is in no log line or conf file" 0 "$leaks"
stored=$(sql "$first" "SELECT role_name || ' ' || tenant || '|' || left(secret, 7) FROM supacache.resp_credential WHERE username = 'supatype'")
expect "password: stored as a verifier for service_role, tenant ''" "service_role |sha256\$" "$stored"
shm=/dev/shm/supatype/keyspace-password
expect "password: pg_keyspace.password_file names the staged file, not the password" "$shm" \
	"$(sql "$first" 'SHOW pg_keyspace.password_file' 2>&1)"
expect "password: the staged file is the postgres user's alone" "600 postgres" \
	"$(docker exec "$first" stat -c '%a %U' "$shm" 2>&1)"
# PID 1 is the postmaster, which every backend and worker inherits its environment from. Read as
# postgres: root in the container may not read another user's environ, and an unreadable file would
# make the count 0 whatever it holds.
environ=$(docker exec -u postgres "$first" sh -c "tr '\0' '\n' < /proc/1/environ" 2>&1)
case "$environ" in
	*PATH=*) ok "password: the server's environment can be read" ;;
	*) fail "password: could not read the server's environment: [$environ]" ;;
esac
expect "password: the server's environment does not carry the password" 0 \
	"$(printf '%s\n' "$environ" | grep -c '^SUPATYPE_KEYSPACE_PASSWORD=' || true)"

# The same password across a restart: the credential is checked, not rewritten with a fresh salt.
verifier="SELECT secret FROM supacache.resp_credential WHERE username = 'supatype'"
before=$(sql "$first" "$verifier")
docker restart "$first" > /dev/null
ready "$first" "password, restarted"
expect "password, restarted: the stored verifier is left as it was" "$before" "$(sql "$first" "$verifier")"
got=$(talk "$first" 127.0.0.1 "$login" PING | head -n 2)
expect "password, restarted: AUTH still works" "$(lines +OK +PONG)" "$got"

# ---- 2. the same data, a new password ----------------------------------------------------------
docker rm -f "$first" > /dev/null
start "$second" "$secret2"
ready "$second" "new password"
got=$(talk "$second" 127.0.0.1 "$(c AUTH supatype "$secret1")" "$(c GET k)" | head -n 2)
expect "new password: the old one is refused" "$(lines "$wrongpass" "$noauth")" "$got"
got=$(talk "$second" 127.0.0.1 "$(c AUTH supatype "$secret2")" "$(c SET k2 v2)" | head -n 2)
expect "new password: the new one works" "$(lines +OK +OK)" "$got"
refused_everywhere "$second" "new password"
leaks=$( {
	docker logs "$second" 2>&1
	sql "$second" "SELECT query FROM extensions.pg_stat_statements" 2> /dev/null
} | grep -cF "$secret2" || true)
expect "new password: the secret is in no log line and not in pg_stat_statements" 0 "$leaks"
docker rm -f "$second" > /dev/null

# ---- 3. require_auth with no credential, and a narrowed listen address -------------------------
docker volume rm "$volume" > /dev/null
start "$closed" '' -c pg_keyspace.require_auth=on -c pg_keyspace.listen_address=127.0.0.1
ready "$closed" "require_auth"
refused_everywhere "$closed" "require_auth, no credential"
got=$(talk "$closed" 127.0.0.1 "$(c AUTH supatype anything)" "$(c AUTH anything)" | head -n 2)
expect "require_auth, no credential: no password is accepted" "$(lines "$wrongpass" "$wrongpass")" "$got"
own=$(docker exec "$closed" hostname -i | awk '{print $1}')
expect "listen_address 127.0.0.1: nothing answers on $own" "" "$(talk "$closed" "$own" PING)"

# ---- 4. no password: unchanged -----------------------------------------------------------------
docker rm -f "$closed" > /dev/null
docker volume rm "$volume" > /dev/null
start "$open_c" ''
ready "$open_c" "no password"
own=$(docker exec "$open_c" hostname -i | awk '{print $1}')
got=$(talk "$open_c" "$own" PING "$(c SET k v)" DBSIZE)
expect "no password: RESP serves without AUTH on $own, as before" "$(lines +PONG +OK :1 +OK)" "$got"

# ---- 5. the keyspace in its own database -------------------------------------------------------
# The worker connects to SUPATYPE_KEYSPACE_DATABASE, so that is where the extension and the
# credential have to be. They used to go to POSTGRES_DB, and the worker found neither.
docker rm -f "$open_c" > /dev/null
docker volume rm "$volume" > /dev/null
ks_db=kscache
start "$otherdb" "$secret1"
ready "$otherdb" "own database"
expect "own database: pg_keyspace.database is kscache" kscache "$(sql "$otherdb" 'SHOW pg_keyspace.database')"
expect "own database: the extension is in kscache" 1 \
	"$(sql "$otherdb" "SELECT count(*) FROM pg_extension WHERE extname = 'pg_keyspace'" kscache 2>&1)"
expect "own database: the credential is in kscache" 1 \
	"$(sql "$otherdb" "SELECT count(*) FROM supacache.resp_credential WHERE username = 'supatype'" kscache 2>&1)"
refused_everywhere "$otherdb" "own database"
got=$(talk "$otherdb" 127.0.0.1 "$login" "$(c SET k v)" | head -n 2)
expect "own database: AUTH then SET work" "$(lines +OK +OK)" "$got"
# Every start, not only the first: remove the credential, restart, and it is back.
sql "$otherdb" "DELETE FROM supacache.resp_credential WHERE username = 'supatype'" kscache > /dev/null
docker restart "$otherdb" > /dev/null
ready "$otherdb" "own database, restarted"
got=$(talk "$otherdb" 127.0.0.1 "$login" "$(c SET k v)" | head -n 2)
expect "own database, restarted: the credential is set again and AUTH works" "$(lines +OK +OK)" "$got"

# ---- 6. the password from a file ---------------------------------------------------------------
docker rm -f "$otherdb" > /dev/null
docker volume rm "$volume" > /dev/null
ks_db=''
pw_file=/tmp/keyspace-password
start_with_password_file "$fromfile" "$secret2"
ready "$fromfile" "password file"
refused_everywhere "$fromfile" "password file"
got=$(talk "$fromfile" 127.0.0.1 "$(c AUTH supatype "$secret2")" "$(c SET k v)" | head -n 2)
expect "password file: the file's password works, its trailing newline dropped" "$(lines +OK +OK)" "$got"
got=$(talk "$fromfile" 127.0.0.1 "$(c AUTH supatype "$secret1")" | head -n 1)
expect "password file: any other password is refused" "$wrongpass" "$got"
expect "password file: pg_keyspace.password_file names the mounted file" "$pw_file" \
	"$(sql "$fromfile" 'SHOW pg_keyspace.password_file' 2>&1)"

# Rotation with no restart: rewrite the file and reload, and the new password is the only one.
rewrite_password_file() {
	docker exec "$fromfile" sh -c 'printf "%s\n" "$1" > "$2"' _ "$1" "$pw_file"
	sql "$fromfile" 'SELECT pg_reload_conf()' > /dev/null
}
# The first reply to AUTH with $1, once a reload has had time to apply (up to ten seconds).
auth_after_reload() {
	for _ in $(seq 1 20); do
		got=$(talk "$fromfile" 127.0.0.1 "$(c AUTH supatype "$1")" | head -n 1)
		[ "$got" = "$2" ] && break
		sleep 0.5
	done
	echo "$got"
}
rotated='rotated-secret'
rewrite_password_file "$rotated"
expect "password file, rewritten and reloaded: the new password works" "+OK" "$(auth_after_reload "$rotated" +OK)"
got=$(talk "$fromfile" 127.0.0.1 "$(c AUTH supatype "$secret2")" | head -n 1)
expect "password file, rewritten and reloaded: the old one is refused" "$wrongpass" "$got"
# Emptied and reloaded: the extension refuses supatype rather than keep the last password.
rewrite_password_file ''
expect "password file, emptied and reloaded: supatype is refused" "$wrongpass" \
	"$(auth_after_reload "$rotated" "$wrongpass")"
case "$(docker logs "$fromfile" 2>&1)" in
	*"pg_keyspace.password_file '$pw_file' holds no password"*) ok "password file, emptied: the log says it holds no password" ;;
	*) fail "password file, emptied: no 'holds no password' in the log" ;;
esac
docker rm -f "$fromfile" > /dev/null
docker volume rm "$volume" > /dev/null

# refused_start <container> <label> <log text>: the container stopped on its own, saying why.
refused_start() {
	for _ in $(seq 1 30); do
		[ "$(docker inspect -f '{{.State.Running}}' "$1")" = false ] && break
		sleep 1
	done
	expect "$2: the container does not start" false "$(docker inspect -f '{{.State.Running}}' "$1")"
	case "$(docker logs "$1" 2>&1)" in
		*"$3"*) ok "$2: the log says '$3'" ;;
		*) fail "$2: no '$3' in the log" ;;
	esac
	docker rm -f "$1" > /dev/null
	docker volume rm "$volume" > /dev/null
}

# A file with no password in it would otherwise read as no password at all, and leave the keyspace
# open, when whoever mounted it asked for one.
start_with_password_file "$fromfile" '   '
refused_start "$fromfile" "blank password file" \
	"error: SUPATYPE_KEYSPACE_PASSWORD_FILE ($pw_file) holds no password"

# Both forms at once is ambiguous, so the start stops, as it does for POSTGRES_PASSWORD(_FILE).
start "$both" "$secret1"
refused_start "$both" "both set" \
	"error: both SUPATYPE_KEYSPACE_PASSWORD and SUPATYPE_KEYSPACE_PASSWORD_FILE are set (but are exclusive)"

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed"
	exit 1
fi
echo "all keyspace auth checks passed"
