# Fork maintenance

Chromeria (`toolboxmd/chromeria`) stays a thin stack of topic commits on
`pingdotgg/t3code`. `origin` names the fork and `upstream` names the canonical
repository. Never push fork commits to `upstream`.

## V2 integration

`fork/v2` begins at the exact upstream commit
`12069eefd707f78eafc27812027c994eea0613cf`. Foundation and feature PRs target
that integration branch. The existing `main` is not the V2 integration base;
changing it or cutting over the daily app requires a separate reviewed action.
Use the approved pinned foundation even when a newer upstream commit exists.

Keep a merge-free topic stack. Features belong in fork-owned files, with the
smallest edits to upstream files. [The feature map](fork-features.md) assigns
every carried upstream edit one primary owner; other carried features may list
it as shared. `scripts/fork-upstream-edits.txt` contains only those carried edits.
Features deferred to separate integration Issues do not belong in this inventory.

## Desktop and data isolation

The production desktop build is Chromeria, bundle id `md.toolbox.chromeria`,
with its existing Electron `chromeria` profile. Development keeps upstream's
`t3code-dev` identity. On Windows, legacy profile migration copies only
`Local State` for safeStorage, never locked Chromium databases. Builds are
identified by their embedded commit SHA; upstream package versions are unchanged.
There is no Chromeria desktop update feed.

The server uses `chromeria-v2.sqlite` in its userdata directory. When missing,
upstream initialization seeds it from a read-only sibling `state.sqlite` and
imports V1 data into V2. Backfills run after shell import and before recovery;
they can use imported V2 shells and frozen V1 data, never hydrated transcripts.
Transcript hydration remains upstream's later background work.

Never start a server against live `~/.t3/userdata` or open that source read-write.
For real-data import proof, the snapshot harness opens the source read-only and
uses `VACUUM INTO` to create a private, consistent snapshot. Migrations and import
run only on that snapshot, without a server or providers:

```bash
CHROMERIA_V1_SNAPSHOT_SOURCE="$HOME/.t3/userdata/state.sqlite" \
  pnpm exec vp test run apps/server/src/persistence/forkV1Backfills.test.ts
```

The test refuses snapshot overwrite, checks shell-only hook order, exercises
partial-failure recovery and repeated execution, then verifies transcript import.
Feature backfills register in the fork-owned registry and own their idempotency.
Keep private database contents out of GitHub evidence.

## Proof and upstream overlap

Before absorption, retain the complete overlap report in the owning Issue or PR:

```bash
node scripts/fork-features.mjs report <last-absorbed-upstream-sha> <approved-target-sha>
```

The report scans every commit in that range, comparing merge commits with their
first parent. Literal keywords match titles, paths and complete diffs, and paths
match every carried feature that watches them. Git output has a bounded 1 GiB
buffer; an overflow fails with the offending command and commit, never a partial
report. For every match, record a reviewed decision and rationale:
keep ours, adopt upstream and delete ours, or merge both. Heuristic matches alone
do not establish duplication. Preserve the original pre-absorption report.

`scripts/fork-check.sh` compares the entire stack from the merge base with
`upstream/main`, independent of the PR base. A missing `upstream/main` fails;
there is no fallback to `origin/main`. CI fetches upstream explicitly. An explicit
`--base` is available for reproducing proof against an approved historical pin.

Run focused tests and scoped lint/typecheck for the carried changes. CI owns
repo-wide checks. Neutralize inherited desktop and host environment differences
when running focused proof on macOS:

```bash
env -u ELECTRON_RUN_AS_NODE \
  TMPDIR="$(cd "${TMPDIR:-/tmp}" && pwd -P)/" \
  PATH="$(printf '%s' "$PATH" | tr ':' '\n' | grep -v '^/opt/homebrew' | paste -sd: -)" \
  pnpm exec vp test run <affected-test-files>
```

Use the project's supported Node version, targeted `pnpm exec vp lint <files>`,
and the affected package's typecheck. Do not run the rebase helper's `--proof`
mode without explicit authority for its repo-wide checks. Do not run it while
preparing a pinned integration foundation: it fetches and rebases onto newer
upstream `main`.

## CI and delivery

Fork CI retains upstream's jobs and gates while mapping PR and main checks from
Blacksmith to GitHub-hosted Ubuntu and macOS runners. Release, deployment and
preview workflows retain upstream labels and secrets requirements; this routine
does not authorize running them. The additional Fork Stack Model job verifies
no merge commits, the allowlist and exactly one owner per carried upstream edit.

Keep exact base/head, reviewed decisions, mechanical conflict resolutions,
commands/results and independent exact-head review in the integration PR. Link
its owning Issue without closing it before merge. No merge, main rewrite,
installation or cutover follows merely from opening that PR. Preview background
capture runtime retest from #152 remains required before #177 cutover, after the
user explicitly grants browser authority.
