#!/usr/bin/env bash
# test-hetzner-cdn.sh — Quick Hetzner Object Storage connectivity + upload test.
# Mirrors the CDN settings used in .github/workflows/native-archives.yml.
#
# Usage (requires ~/.aws/credentials profile "supatype"):
#   ./scripts/test-hetzner-cdn.sh
#
# Optional overrides:
#   AWS_PROFILE    (default: supatype)
#   CDN_ENDPOINT   (default: https://nbg1.your-objectstorage.com)
#   CDN_BUCKET     (default: supatype-releases)
#   CDN_AWS_REGION (default: eu-central-1)
#   PG_VERSION     (default: 17.2)
#
#   ./scripts/test-hetzner-cdn.sh --cleanup   # remove the test object after upload

set -euo pipefail

AWS_PROFILE="${AWS_PROFILE:-supatype}"
CDN_ENDPOINT="${CDN_ENDPOINT:-https://nbg1.your-objectstorage.com}"
CDN_BUCKET="${CDN_BUCKET:-supatype-releases}"
CDN_AWS_REGION="${CDN_AWS_REGION:-eu-central-1}"
PG_VERSION="${PG_VERSION:-17.2}"
CLEANUP="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cleanup)
      CLEANUP="true"
      shift
      ;;
    -h|--help)
      sed -n '2,16p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

export AWS_PROFILE
export AWS_DEFAULT_REGION="${CDN_AWS_REGION}"
export AWS_EC2_METADATA_DISABLED=true
export AWS_RETRY_MODE=standard
export AWS_MAX_ATTEMPTS=10

if ! aws configure list-profiles | grep -Fxq "${AWS_PROFILE}"; then
  echo "AWS profile '${AWS_PROFILE}' not found. Add it to ~/.aws/credentials and ~/.aws/config." >&2
  exit 1
fi

if [[ -z "$(aws configure get aws_access_key_id --profile "${AWS_PROFILE}")" ]]; then
  echo "Profile '${AWS_PROFILE}' has no aws_access_key_id. Set Hetzner S3 keys in ~/.aws/credentials." >&2
  exit 1
fi

CDN_ENDPOINT="${CDN_ENDPOINT%/}"

if [[ "${CDN_ENDPOINT}" == *"/${CDN_BUCKET}"* ]]; then
  echo "CDN_ENDPOINT must be the regional host only (no bucket in the URL)." >&2
  exit 1
fi

aws configure set s3.addressing_style path --profile "${AWS_PROFILE}" >/dev/null
aws configure set s3.signature_version s3v4 --profile "${AWS_PROFILE}" >/dev/null

TEST_FILE="$(mktemp /tmp/supatype-cdn-test.XXXXXX.txt)"
TEST_KEY="postgres/v${PG_VERSION}/ci-connectivity-test-$(date -u +%Y%m%dT%H%M%SZ).txt"
DEST="s3://${CDN_BUCKET}/${TEST_KEY}"

{
  echo "supatype-postgres Hetzner CDN connectivity test"
  echo "created_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "profile=${AWS_PROFILE}"
  echo "endpoint=${CDN_ENDPOINT}"
  echo "bucket=${CDN_BUCKET}"
  echo "region=${CDN_AWS_REGION}"
} > "${TEST_FILE}"

cleanup() {
  rm -f "${TEST_FILE}"
}
trap cleanup EXIT

echo "=== Hetzner CDN test ==="
echo "profile:  ${AWS_PROFILE}"
echo "endpoint: ${CDN_ENDPOINT}"
echo "bucket:   ${CDN_BUCKET}"
echo "region:   ${CDN_AWS_REGION}"
echo "test file: ${TEST_FILE}"
echo "dest:     ${DEST}"
echo

# Hetzner is S3-compatible only — do not use aws sts get-caller-identity (hits real AWS).

echo "1) Preflight: list bucket"
aws s3 ls "s3://${CDN_BUCKET}/" --endpoint-url "${CDN_ENDPOINT}"
echo

echo "2) Upload test object"
aws s3 cp "${TEST_FILE}" "${DEST}" \
  --endpoint-url "${CDN_ENDPOINT}" \
  --cache-control "public, max-age=60" \
  --no-progress
echo

echo "3) Verify object exists"
aws s3 ls "${DEST}" --endpoint-url "${CDN_ENDPOINT}"
echo

if [[ "${CLEANUP}" == "true" ]]; then
  echo "4) Cleanup test object"
  aws s3 rm "${DEST}" --endpoint-url "${CDN_ENDPOINT}"
  echo "Removed ${DEST}"
else
  echo "Test object left at: ${DEST}"
  echo "Re-run with --cleanup to delete it."
fi

echo
echo "OK — Hetzner CDN upload path works from this machine."
