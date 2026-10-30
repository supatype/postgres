#!/usr/bin/env bash
# bin/postgres in the Linux archives. scripts/bundle-linux-libraries.sh installs it in place of the
# server, which it moves beside it to bin/postgres.bin, and this starts that with one setting made.
#
# pg_net's libcurl verifies HTTPS through the archive's OpenSSL, which was built on Ubuntu and
# looks for CA certificates only under /usr/lib/ssl. Other distributions keep them elsewhere
# (Fedora and RHEL under /etc/pki/tls), so there every HTTPS request pg_net made failed
# certificate verification. When the caller has not chosen a CA store itself (SSL_CERT_FILE or
# SSL_CERT_DIR, which OpenSSL reads), point OpenSSL at the host's bundle: the first of the usual
# locations that exists, the list Go's crypto/x509 searches on Linux.
if [ -z "${SSL_CERT_FILE:-}" ] && [ -z "${SSL_CERT_DIR:-}" ]; then
	for bundle in /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt \
		/etc/ssl/ca-bundle.pem /etc/pki/tls/cacert.pem \
		/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem /etc/ssl/cert.pem; do
		if [ -r "${bundle}" ]; then
			export SSL_CERT_FILE="${bundle}"
			break
		fi
	done
fi

# Under this script's own name, so the server still calls itself postgres in its messages and
# resolves its share and lib directories from bin/, as it would without the wrapper.
exec -a "$0" "$(dirname "$0")/postgres.bin" "$@"
