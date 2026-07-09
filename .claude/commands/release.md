---
description: Bump version, update CHANGELOG, and publish a new nitrite-rust release to crates.io
argument-hint: "[major|minor|patch] (optional - overrides the auto-detected bump type)"
---

You are cutting a release of nitrite-rust. This publishes real, permanent crate versions to
crates.io. **crates.io does not allow re-publishing or reusing a version number, even a failed
one** — a version can only be "yanked" (hidden from new dependents), never deleted or replaced.
Follow every step; do not skip the confirmation gates.

## How this repo's release pipeline actually works (verified, do not re-derive from scratch)

- **One shared version** across all 8 workspace crates (each has its own literal `version =`
  field — there is no `[workspace.package]` version to bump centrally):
  `nitrite`, `nitrite-derive`, `nitrite-fjall-adapter`, `nitrite-spatial`,
  `nitrite-tantivy-fts`, `nitrite-vector`, `nitrite-bench` (`publish = false`),
  `nitrite-int-test` (`publish = false`). Bump all 8 in lockstep even though only 6 are actually
  published, to keep the workspace internally consistent (matches current repo state).
- Four crates also pin `nitrite` as a path+version dependency and need that version string bumped
  too: `nitrite-fjall-adapter/Cargo.toml`, `nitrite-spatial/Cargo.toml`,
  `nitrite-tantivy-fts/Cargo.toml`, `nitrite-vector/Cargo.toml` (line with
  `nitrite = { version = "OLD", path = "../nitrite" }`).
- Tag format is a single **`vX.Y.Z`** tag (not per-crate).
- `.github/workflows/publish.yml` ("Publish to crates.io") triggers on push of a tag matching
  `v*.*.*`. It publishes crates **in dependency order** with `sleep 30` pauses for the crates.io
  index to catch up: `nitrite-derive` → wait → `nitrite` → wait ×2 → `nitrite-fjall-adapter` →
  `nitrite-spatial` → `nitrite-tantivy-fts` → `nitrite-vector`. Takes ~6-10 minutes.
- This workflow **also** supports `workflow_dispatch` with a `dry_run` input that **defaults to
  `true`**. Do not use `workflow_dispatch` to trigger a real release — if you (or the user) ever
  invoke it manually, you must explicitly pass `dry_run=false` or nothing will actually publish.
  The tag-push trigger is the only path this command uses.
- **Tag type matters.** Use a **lightweight tag** (`git tag vX.Y.Z`, no `-a`, no `-m`) — this
  matches every historical working tag in this repo (`git cat-file -t v0.4.2` → `commit`, not
  `tag`) and is the proven-safe form; nitrite-flutter's otherwise-identical tag pattern silently
  failed to trigger its workflow when pushed as an annotated tag.
- No workflow creates a GitHub Release automatically — create one yourself with
  `gh release create` after the tag exists and crates.io confirms the publish, purely for
  changelog visibility.
- `Cargo.lock` is committed to the repo. Regenerate it after bumping versions
  (`cargo build --workspace` or `cargo update -w`) so it isn't stale, and include it in the
  version-bump commit.

## Step 1 — Preflight

1. `git status --short` — must be clean. If not, stop and tell the user what's uncommitted.
2. `git fetch origin --quiet && git log --oneline main..origin/main` — must be empty. If not,
   stop; do not silently rebase over unknown remote commits.
3. Confirm you are on `main`.

## Step 2 — Determine the version bump

Note: nitrite-rust is still pre-1.0 (`0.x.y`); under SemVer, breaking changes in a `0.x` series
conventionally bump the **minor** field, not major (major stays `0` until a deliberate 1.0
declaration) — apply that convention here unless the user explicitly asks to cut `1.0.0`.

1. Find the last release tag: `git tag -l 'v*' | sort -V | tail -1`.
2. `git log <lastTag>..HEAD --oneline` to see everything since the last release.
3. Classify using Conventional Commits semantics, adjusted for the pre-1.0 convention above:
   - **minor** (the pre-1.0 stand-in for "major"): breaking change — an explicit
     `BREAKING CHANGE:` footer, a `!` after the type, an incompatible on-disk/journal format
     change, or a removed/renamed public API. This repo's CHANGELOG has precedent for calling
     out format changes explicitly (e.g. journal format work) — treat those as minor-worthy here.
   - **minor** also covers plain new features (`feat:`, new crate, new index/backend, new public
     API added without breaking anything).
   - **patch**: everything else — bug fixes / maintenance only (`fix:`, `chore:`, `docs:`,
     `refactor:`, `test:`, `ci:`, dependency bumps).
   - Take the highest applicable level across all commits since the last tag. Since both
     "breaking" and "new feature" map to minor pre-1.0, call out in your reasoning which of the
     two (if either) applied, even though the resulting bump is the same.
4. If `$ARGUMENTS` explicitly names `major`, `minor`, or `patch`, that overrides your
   classification — but still show your own analysis first so the user can see the discrepancy.
5. Compute the new version from the last tag + bump type.
6. **Use AskUserQuestion to confirm the proposed version and bump type before touching any
   file.** Show the commit list and your reasoning. Let the user override.

## Step 3 — Bump versions

1. In all 8 `Cargo.toml` files, replace the crate's own `version = "OLD"` with
   `version = "NEW"`.
2. In the 4 dependent crates listed above, also replace
   `nitrite = { version = "OLD", path = "../nitrite" }` with the new version.
3. Verify: `grep -rn "version = \"NEW\"" --include="Cargo.toml" .` should show 8 own-version
   hits plus 4 path-dependency hits = 12 total (not counting `Cargo.lock`).
4. `cargo build --workspace` to regenerate `Cargo.lock` against the new versions (this also acts
   as a compile check). Do not use `cargo publish --dry-run` here yet — save registry-facing
   dry-run checks for the CI step, this is just a local compile/lockfile sanity check.
5. Update `CHANGELOG.md` (Keep a Changelog format): add a new
   `## [NEW] - <YYYY-MM-DD>` section at the top with `### Added` / `### Changed` / `### Fixed`
   subsections as applicable, mirroring the existing entries. Draft from the commit log. Show the
   drafted section to the user before proceeding.

## Step 4 — Verify before committing

Run `cargo test --workspace`. Do not proceed past a red build. If tests fail, stop and report —
do not weaken or skip tests to force a release through.

## Step 5 — Commit and push

1. Commit as `chore(release): vNEW - <one-line summary of the headline change>` (matches repo
   convention — see `git log --oneline -- Cargo.toml`), including all 8 `Cargo.toml` files,
   `Cargo.lock`, and `CHANGELOG.md`.
2. `git fetch origin --quiet` once more and confirm no new divergence, then `git push origin
   main`.

## Step 6 — Tag and trigger the release

**This is the point of no return** — crates.io accepts each `cargo publish` permanently; there is
no un-publish. Use AskUserQuestion one more time to get explicit go-ahead before this step,
showing the exact tag and version about to be published, and reminding the user that a mid-way
failure (e.g. `nitrite` publishes but `nitrite-spatial` fails) cannot be retried under the same
version number — it would require bumping to the next patch and re-running this whole command.

1. `git tag vNEW` (lightweight — no `-a`/`-m`).
2. `git push origin vNEW`.
3. Within ~30s, confirm the workflow picked it up:
   `gh run list --workflow=publish.yml --limit 3` — look for a run with `headBranch == vNEW` and
   `event == push` (not `workflow_dispatch`). If none appears within a couple of minutes,
   something is wrong (check tag type with `git cat-file -t vNEW` — must print `commit`); do not
   just wait indefinitely, investigate.

## Step 7 — Wait and verify

1. Poll (use ScheduleWakeup at a few-minute cadence, or Monitor for a polling loop)
   `gh run list --workflow=publish.yml --limit 3` until the run completes. Expect ~6-10 minutes.
2. If it fails partway, use `gh run view <id> --log-failed` to see exactly which `cargo publish`
   step failed and which ones (if any) already succeeded before it. Report this precisely to the
   user — do not guess. Since already-published crates in this run cannot be republished at this
   version, recovery requires: bump to the next patch version and re-run the whole command (the
   crates that already published stay at NEW; only the failed ones and anything depending on them
   need the bumped patch version — but the simplest, safest correct path is to just re-run the
   full command at NEW+1 for all crates, keeping the workspace in lockstep as always).
3. Once it succeeds, verify each of the 6 published crates on crates.io (allow a minute for index
   propagation):
   `for c in nitrite_derive nitrite nitrite_fjall_adapter nitrite_spatial nitrite_tantivy_fts
   nitrite_vector; do curl -s "https://crates.io/api/v1/crates/$c" | jq -r '.crate.max_version';
   done` — every line should read `NEW`.

## Step 8 — Create the GitHub Release

**Always use the CHANGELOG.md content as the release notes — never `--generate-notes`.** This
repo's past releases (e.g. v0.4.2) all use the curated CHANGELOG section verbatim as the release
body; `--generate-notes` instead produces a bare auto-generated PR list, which is inconsistent
with every existing release here and must not be used.

1. Extract exactly the new section you added to `CHANGELOG.md` in Step 3 — everything between
   the `## [NEW] - ...` heading and the next `## [` heading, excluding both heading lines — into
   a temp file, e.g.:
   `awk '/^## \[/ && ++c==1 {next} /^## \[/ && c==1 {exit} c==1 {print}' CHANGELOG.md > /tmp/notes.md`
2. `gh release create vNEW --repo nitrite/nitrite-rust --title vNEW --notes-file /tmp/notes.md`
3. Verify: `gh release view vNEW --repo nitrite/nitrite-rust --json body -q '.body'` should print
   the same prose you drafted in Step 3, not a "What's Changed" / "Full Changelog" auto-generated
   block.

## Step 9 — Report

Summarize: old → new version, bump type and why, publish workflow outcome and duration,
crates.io confirmation per crate, GitHub Release URL.
