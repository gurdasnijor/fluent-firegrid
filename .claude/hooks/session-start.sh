#!/bin/bash
# SessionStart hook: install the native dependencies that aren't in the npm
# tree so the durable runner + spec suite can actually run in Claude Code on
# the web sessions (the container is ephemeral; these are re-provisioned each
# session and then cached). Idempotent and non-interactive.
set -euo pipefail

# Web/remote sessions only; a local dev machine manages its own toolchain.
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "${CLAUDE_PROJECT_DIR:-$(pwd)}"

# 1. Node deps (cached after first run).
pnpm install --frozen-lockfile

# 2. S2 CLI — `s2 lite` is the local S2 emulator the durable tests spawn
#    (packages/durable-cucumber/src/s2lite.ts). Pinned + checksum-verified to match CI.
S2_VERSION="s2-cli-v0.36.6"
S2_SHA256="ea181584c8568a8fc607da894f5299225345268cb7419dfd488baef251eea04e"
if ! command -v s2 >/dev/null 2>&1; then
  curl --fail --location --silent --show-error --max-time 180 \
    --output /tmp/s2.zip \
    "https://github.com/s2-streamstore/s2/releases/download/${S2_VERSION}/s2-x86_64-unknown-linux-gnu.zip"
  echo "${S2_SHA256}  /tmp/s2.zip" | sha256sum --check
  unzip -oq /tmp/s2.zip -d /tmp/s2
  install -m 0755 /tmp/s2/s2 /usr/local/bin/s2
fi
s2 --version

# 3. chDB native binding — the trace-proof spec suite loads libchdb.so from a
#    hard-coded path; mirror CI's symlink + LD_LIBRARY_PATH so those run too.
chdb_lib_dir="$(find node_modules/.pnpm -maxdepth 5 -type d -path '*@chdb+lib-linux-x64-gnu@*/node_modules/@chdb/lib-linux-x64-gnu' 2>/dev/null | head -1 || true)"
if [ -n "${chdb_lib_dir}" ] && [ -f "${chdb_lib_dir}/libchdb.so" ]; then
  abs="${PWD}/${chdb_lib_dir}"
  mkdir -p /home/runner/work/chdb-node/chdb-node
  ln -sf "${abs}/libchdb.so" /home/runner/work/chdb-node/chdb-node/libchdb.so
  if [ -n "${CLAUDE_ENV_FILE:-}" ]; then
    echo "export LD_LIBRARY_PATH=\"${abs}\${LD_LIBRARY_PATH:+:\$LD_LIBRARY_PATH}\"" >> "$CLAUDE_ENV_FILE"
  fi
fi

echo "session-start: dependencies ready (s2 $(s2 --version 2>/dev/null | awk '{print $2}'))"
