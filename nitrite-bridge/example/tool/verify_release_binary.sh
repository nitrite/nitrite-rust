#!/usr/bin/env bash
# docs/THREAT-MODEL.md §7 criterion 2, on a built artifact rather than in-process.
#
#   tool/verify_release_binary.sh
#
# Builds the example twice in release mode and greps both binaries:
#
#   1. the plain build must contain NONE of the protocol strings, and
#   2. the `--features inspect` build must contain ALL of them.
#
# The second build is the negative control. Without it the first passes for a moved path, a
# renamed method or an empty file, and proves nothing.
#
# On Rust the guard is the Cargo feature, not a flag: a build that does not name `inspect` never
# compiles `nitrite-bridge` or `dbinspect-bridge` at all, so there is nothing in the artifact for
# a linker to strip. That is stronger than Dart's tree shaking and the JVM's `provided` scope,
# both of which guard code that is present — and it is why this check lives here, with the
# application, rather than in the core's own tests.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$here"

# Wire vocabulary a client would find in an artifact that still has the bridge in it. Not
# `listStores` or `queryPage`: a `match` on a `&str` compiles to a length test and an inline
# comparison, so those names are not contiguous in any Rust binary and a needle that is absent by
# accident is not a check.
needles=(adapterId pageSizeClamped watchScope sampledDocs "dbinspect")

binary="target/release/nitrite-bridge-example"

echo "==> building the release artifact without the bridge"
cargo build --release --quiet

missing=0
for needle in "${needles[@]}"; do
  if grep -qa -- "$needle" "$binary"; then
    echo "FAIL: the release binary contains \"$needle\""
    missing=1
  fi
done
if [ "$missing" -ne 0 ]; then
  echo
  echo "criterion 2 failed: a release build that did not ask for the bridge carries it."
  exit 1
fi
echo "    none of the protocol strings are in it"

echo "==> building again with --features inspect, as the negative control"
cargo build --release --quiet --features inspect

for needle in "${needles[@]}"; do
  if ! grep -qa -- "$needle" "$binary"; then
    echo "FAIL: the opt-in build is missing \"$needle\", so the check above proves nothing"
    exit 1
  fi
done
echo "    all of them are back"

echo
echo "criterion 2 passes: the bridge is in the artifact only when the feature asked for it."
