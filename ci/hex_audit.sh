#!/bin/sh
set -o errexit
set -o nounset
set -o xtrace

mix local.hex --force

set +o xtrace
set +o errexit
audit_output=$(mix hex.audit 2>&1)
audit_status=$?
set -o errexit

printf '%s\n' "$audit_output"

if printf '%s\n' "$audit_output" | awk '
  /^Advisories:$/ { section = "advisories"; next }
  /^[A-Za-z].*:$/ { section = "other"; next }
  section == "advisories" && /^  [^ ].* \((HIGH|CRITICAL)\)$/ { found = 1 }
  END { exit found ? 0 : 1 }
'; then
  echo "Failing the job: HIGH or CRITICAL advisories found"
  exit 1
fi

if [ "$audit_status" -eq 0 ]; then
  exit 0
fi

if printf '%s\n' "$audit_output" | grep -q -E 'Found (retired packages|packages with security advisories)'; then
  exit 0
fi

exit "$audit_status"
