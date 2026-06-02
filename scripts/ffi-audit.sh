#!/usr/bin/env bash
set -euo pipefail
RESULT=$(cargo tree --edges normal 2>/dev/null \
  | grep -E '(flate2 v|protobuf-src v|ring v|aws-lc-sys v|openssl-sys v|native-tls v)' || true)
if [ -n "$RESULT" ]; then
  echo "FFI AUDIT FAILED:"
  echo "$RESULT"
  exit 1
fi
echo "FFI audit PASSED"
