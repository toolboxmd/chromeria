# Fork maintenance

Chromeria (`toolboxmd/chromeria`) is a personal, rebasable stack of topic commits on
`pingdotgg/t3code`. `origin` names the fork and `upstream` names the canonical
repository. Never push fork commits to `upstream`.

## Upstream absorption

`main` contains the completed V2 port. For each absorption, record its upstream
merge base and the approved target SHA in the owning Issue, then replay the fork
topics onto that target in one exclusive integration worktree. Keep the approved
target fixed while preparing the candidate. Before delivery, carry every fork
topic merged into `main` since preparation began and record the complete range-diff.

Keep a merge-free topic stack. Features live in fork-owned modules; upstream
files carry only the hooks those modules need. [The carried-edit registry](fork-features.md) assigns
every carried upstream edit one primary owner; other carried features may list
it as shared. `scripts/fork-upstream-edits.txt` contains only those carried edits.
Features deferred to separate integration Issues do not belong in this inventory.
When upstream fully absorbs a carried capability, prove equivalent behavior before
removing its registry entry and carried edits. In that same PR, change its mapped
part to `live` if it remains an existing capability, removing its tracked refs.

## Desktop and data isolation

The production desktop build is Chromeria, bundle id `md.toolbox.chromeria`,
with its existing Electron `chromeria` profile. Development keeps upstream's
`t3code-dev` identity. On Windows, legacy profile migration copies only
`Local State` for safeStorage, never locked Chromium databases. Builds are
identified by their embedded commit SHA; upstream package versions are unchanged.
There is no Chromeria desktop update feed.

For the continuous macOS Chrome login mirror, set `chromeCookieKeyCommand` in
`desktop-settings.json` in the desktop state directory:

```json
{
  "chromeCookieKeyCommand": ["bao", "kv", "get", "-field=<field>", "<path>"]
}
```

The command runs as argv without a shell. Its stdout must contain Chrome's Safe
Storage secret, optionally followed by one newline. The app derives the existing
macOS v10 key and caches successful reads only in memory. Remove the
setting to use Keychain. A configured command that fails or is invalid stops Chrome
observation; it never falls back to Keychain. Failed reads retry with bounded
backoff. Command changes take effect on the next scan, within about 30 seconds,
and discard the cached key. Ad-hoc signed builds lose Keychain's
"Always Allow" grant after rebuilds, so the command avoids repeated prompts.

Remote pairings need the explicit **Mirror desktop browser logins** permission
(`preview:mirror-cookies`) in Connections settings. Standard client scopes do not
include it. Re-pair existing connections with that permission before live proof;
the local desktop secret derives a separate, scoped main-process credential. The
renderer bootstrap token cannot mint or rotate that credential.
Desktop main resolves destinations and parent credentials from its own backend
pool and saved connection catalog, verifies the environment identity and scope,
and keeps mirror grants out of the renderer. A client-supplied surface is never
authorization. T3 Connect currently mints standard environment scopes only, so its
automatic session does not grant cookie mirroring. Use an explicitly scoped direct
pairing for the same environment to enable mirroring.

Mirroring runs only while the desktop app runs. Desktop retains value-free managed
identities and removal metadata; the server retains a profile catalog. Cookie values
remain in browser profiles and process memory. Failures have fixed
diagnostic codes. Settings > Integrations > Browser reports each observed
device-bound site as `device-bound, not mirrored`. Chrome's DBSC store supplies
site identities only; the mirror never reads its credential protobufs or binding
keys. Google credential cookies are also excluded on macOS. See
[reuse browser logins](user/browser-import.md) for session-cookie behavior.

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
commands/results and independent exact-head review in the integration PR. A proof
PR may target a branch at the approved upstream SHA so pull-request CI checks the
candidate's native ancestry. Mark it **DO NOT MERGE**: a squash into the old main
would discard the upstream history and defeat the stack model.

Link the owning Issue without closing it before delivery. Replacing `main` needs
explicit authority and a coordinated window. Preserve its current tip in a pushed
backup tag before preparing the replacement, refresh the fork-topic coverage and
proof, then use `--force-with-lease` against the exact recorded live main SHA.
Stop if branch protection rejects the replacement; do not change protection.
Opening a PR grants no replacement, installation or deployment authority.

## Absorbed projects

Prism absorbed Model Router (`toolboxmd/model-router`), the runner that Prism's
submit tools once called to carry a delegated task through dispatcher and
worker threads to a PR. Prism now routes `delegate_task` and
`t3_thread_launch` itself, and the T3 routes the runner called
(`/api/prism/snapshot`, `/api/prism/liveness`, `/api/orchestration/dispatch`)
no longer exist. Model Router is retired; do not restore those routes for it.
