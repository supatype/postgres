#!/usr/bin/env bash
# Runs check-unix-runtime.sh against a packaged Linux archive in a clean Fedora container, which is
# not the distribution it was built on: no Ubuntu libraries, no /usr/share/proj, and CA
# certificates under /etc/pki rather than /usr/lib/ssl.
#
# On top of the usual checks, pg_net fetches an HTTPS URL from a server in the container whose
# certificate comes from a CA made for this run and trusted only in Fedora's own bundle, so the
# request succeeds only when the archive finds the host's CA certificates. Nothing leaves the
# container, and nothing on the network is trusted by accident.
#
# Needs docker and openssl. Usage: check-linux-portable.sh <supatype-pg-17-linux-*.tar.gz> [image]
# The image defaults to fedora:40. Inside the container the script runs itself with --inside.
set -euo pipefail

https_port=58443
pg_port=58432

# make_certs <dir>: a CA, and a certificate for localhost and 127.0.0.1 signed by it. File names are
# relative to the directory, and MSYS_NO_PATHCONV keeps Git Bash on Windows from rewriting each
# -subj as a path, which would also stop it converting an absolute file path.
make_certs() (
	cd "$1"
	export MSYS_NO_PATHCONV=1
	openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=native-archive test CA" \
		-keyout ca.key -out ca.pem 2>/dev/null
	openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout server.key -out server.csr 2>/dev/null
	printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' >san.ext
	openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 1 \
		-extfile san.ext -out server.pem 2>/dev/null
)

# outside <archive> <image>: stage the archive, the checks, the image's config and the certificates,
# and stream them into a fresh container on stdin, so no host path has to be mounted.
outside() {
	local archive=$1 image=$2 here repo
	here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
	repo=$(cd "${here}/../.." && pwd)
	# Global, not local: the EXIT trap runs after this function has returned.
	work=$(mktemp -d)
	trap 'rm -rf "${work}"' EXIT
	mkdir -p "${work}/repo/tests" "${work}/repo/config" "${work}/certs"
	cp "${archive}" "${work}/archive.tar.gz"
	cp -R "${here}" "${work}/repo/tests/"
	cp "${repo}/config/postgresql.conf" "${work}/repo/config/"
	make_certs "${work}/certs"
	tar -C "${work}" -cf - . | docker run --rm -i "${image}" bash -c \
		'mkdir /work && tar -xf - -C /work && bash /work/repo/tests/native-archive/check-linux-portable.sh --inside'
}

# inside: as root in the container. Trust the CA where Fedora keeps its bundle, serve HTTPS with
# the certificate, unpack the archive as a user (initdb refuses root) and run the checks.
inside() {
	cat /work/certs/ca.pem >>/etc/pki/tls/certs/ca-bundle.crt
	useradd -m checker
	chown -R checker: /work
	sudo -u checker python3 -c '
import http.server, ssl, sys
server = http.server.HTTPServer(("127.0.0.1", int(sys.argv[1])), http.server.SimpleHTTPRequestHandler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain("/work/certs/server.pem", "/work/certs/server.key")
server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()' "${https_port}" >/dev/null 2>&1 &
	sudo -u checker bash -c "mkdir /home/checker/pg && tar -xzf /work/archive.tar.gz -C /home/checker/pg"
	sudo -u checker env NATIVE_ARCHIVE_HTTPS_URL="https://localhost:${https_port}/" \
		bash /work/repo/tests/native-archive/check-unix-runtime.sh /home/checker/pg "${pg_port}"
}

if [ "${1:-}" = --inside ]; then
	inside
else
	[ $# -ge 1 ] || { echo "Usage: $(basename "$0") <archive.tar.gz> [image]" >&2; exit 2; }
	outside "$1" "${2:-fedora:40}"
fi
