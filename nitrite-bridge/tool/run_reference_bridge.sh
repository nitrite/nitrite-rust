#!/usr/bin/env bash
# Starts the Nitrite reference bridge and prints `{"host":…,"port":…,"code":…}` on its first line,
# so the conformance suite can be pointed at it without parsing the pairing banner.
#
#   tool/run_reference_bridge.sh [memory|fjall] &
#   dart run .../conformance/bin/dbinspect_conformance.dart 127.0.0.1:<port> <code>
#
# `--features bridge` is what puts the bridge in the binary at all; a build without it has no
# server in it, which is this crate's half of criterion 2.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$here"

# Built quietly so the JSON line is the first thing on stdout.
cargo build --quiet --features bridge --example reference_bridge

exec ./target/debug/examples/reference_bridge "${1:-memory}"
