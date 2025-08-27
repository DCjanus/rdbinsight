#!/usr/bin/env bash
set -euo pipefail

# Usage: check.sh [TAG]
# TAG can be like "v1.2.3" or "1.2.3". If omitted, uses $GITHUB_REF_NAME when available.

TAG="${1:-${GITHUB_REF_NAME:-}}"
if [[ -z "${TAG}" ]]; then
  echo "::error::Tag is not provided (pass as arg or set GITHUB_REF_NAME)"
  exit 1
fi

TAG_VERSION="${TAG#v}"

# Read Cargo version from [package] section
CARGO_VERSION=$(awk '
$0 ~ /^\[package\]/ { in_pkg=1; next }
$0 ~ /^\[/ && $0 !~ /^\[package\]/ { in_pkg=0 }
in_pkg && $0 ~ /^[[:space:]]*version[[:space:]]*=[[:space:]]*"/ {
  line=$0
  sub(/^[[:space:]]*version[[:space:]]*=[[:space:]]*"/, "", line)
  sub(/".*$/, "", line)
  print line; exit
}' Cargo.toml)
if [ -z "${CARGO_VERSION:-}" ]; then
  echo "::error::Failed to parse version from Cargo.toml"
  exit 1
fi

# Read latest version from CHANGELOG (first non-Unreleased heading)
CHANGELOG_VERSION=$(grep -E '^## \[[^]]+\]' CHANGELOG.md | grep -v 'Unreleased' | head -n1 | sed -E 's/^## \[([^]]+)\].*/\1/')
if [ -z "${CHANGELOG_VERSION:-}" ]; then
  echo "::error::Failed to parse latest released version from CHANGELOG.md"
  exit 1
fi

# Ensure Unreleased section is empty
if awk '
  /^## \[Unreleased\]/ { in=1; next }
  in && /^## \[/ { in=0 }
  in {
    if ($0 !~ /^[[:space:]]*$/) bad=1
  }
  END { if (bad) exit 1 }
' CHANGELOG.md; then
  :
else
  echo "::error::CHANGELOG.md has non-empty Unreleased section on tag ${TAG}"
  exit 1
fi

# Compare versions
ok=1
if [ "${TAG_VERSION}" != "${CARGO_VERSION}" ]; then
  echo "::error::Tag version (${TAG_VERSION}) != Cargo.toml version (${CARGO_VERSION})"
  ok=0
fi
if [ "${TAG_VERSION}" != "${CHANGELOG_VERSION}" ]; then
  echo "::error::Tag version (${TAG_VERSION}) != CHANGELOG latest version (${CHANGELOG_VERSION})"
  ok=0
fi
if [ "${ok}" -ne 1 ]; then
  echo "TAG=${TAG_VERSION}"
  echo "CARGO=${CARGO_VERSION}"
  echo "CHANGELOG=${CHANGELOG_VERSION}"
  exit 1
fi

echo "Version check passed: ${TAG_VERSION}"


