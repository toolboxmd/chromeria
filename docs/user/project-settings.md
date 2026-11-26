# Settings and project overrides

T3 Code separates **device preferences** (saved locally per browser/device, like Appearance and Confirmations) from **server settings** (stored on each environment, like Threads, Worktrees, and Source control).

## Scope and inheritance

Use the **"Applying settings for …"** selector at the top of Settings (web/desktop) or the header filter (mobile) to choose which **environment(s)** and **project** you are editing:

- **All environments**: Bulk-edits every currently connected environment (offline environments keep their values). Controls show **Mixed** where environments disagree.
- **Project scope**: Creates a project override for that setting without changing environment defaults. Settings that cannot be overridden appear read-only.
- **Inheritance order**: `Project override` → `Environment setting` → `t3.json` in the repo → `Built-in default`. Leave a setting on **Inherit** to defer to the next tier.
- Click the **layers icon** beside a server setting to inspect its inheritance chain, jump to overriding projects, or **Reset all** back to the environment default.
- **Per-project providers**: With a project selected in **Settings → Providers**, toggling a provider enables or disables it for that project only (in either direction). Running turns finish; no new turns start on a disabled provider.

## Pin and archive projects

- **Pin**: Pinned projects sort first in project pickers, the command palette, the sidebar filter, and mobile (`mod+shift+p` on a highlighted row, or **Settings → Projects**).
- **Archive**: Archive finished projects in **Settings → Projects** (mobile: **Settings → Projects → Organize**). Archived projects leave pickers while keeping their threads, settings, and files; unarchive anytime from Settings.
- Pinning or archiving a project that exists on multiple machines requires all of those machines to be connected.

## Project icons and multi-repo projects

- **Icons**: In **Settings → Projects**, set an icon, emoji, monogram, or image (applies across checkouts). Without one, T3 Code generates a two-character monogram (or single CJK character) from the project name.
- **Multiple repositories**: See [Projects with several repositories](./multi-repo-projects.md).

## Worktree settings

Configure worktrees in **Settings → Worktrees** (environment default or project override):

- **Branch naming**: Choose a static prefix (default `t3/`), model-selected semantic prefix (`feat/`, `fix/`), or custom instructions. Falls back to the temporary branch name if generation fails or collides.
- **Submodules** (under Advanced): Defaults to recursive checkout; set to **Top level only** or **Skip** (or `"worktreeSubmodules"` in `t3.json`) for repos with heavy nested submodules.
- **Sparse checkout**: Set `"worktreeSparsePatternsPath"` in `t3.json` to a repo-relative file of Git sparse-checkout patterns (non-cone syntax; requires Git 2.35+; enables `extensions.worktreeConfig`). Set Submodules to **Skip** if you don't want submodules initialized under excluded paths.
- **Worktree location** (per machine): Set an absolute path (e.g. `D:\worktrees` or `~/worktrees`) to store new worktrees elsewhere. Existing worktrees stay where they are; cleanup covers both folders.
- **Fast worktree checkout (macOS beta)**: Enable **Settings → Labs → Fast worktree checkout** (Git 2.42+) to clone new worktrees via APFS copy-on-write from a local cache. Falls back to normal checkout for cross-drive paths, sparse checkouts, or Git LFS/filter repos.

### Storage cleanup

Enable automatic cleanup in **Settings → Worktrees → Cleanup** (off by default; runs on startup, on change, and hourly). Projects can set **Automatic worktree cleanup** to **Inherit**, **Off**, or **Custom**.

- **Triggers**: Inactive days, days after settlement (`0` = next cleanup), merged PRs (commits included in the remote default branch, or the worktree still at the PR's last commit, which covers GitHub squash/rebase merges and stacked PRs merged into their parent branch), or settled/archived threads with no commits beyond the default branch.
- **Safety**: Active sessions and shared worktrees are never removed. **Keep worktrees with local changes** defaults to **Uncommitted changes** (protects tracked edits and untracked files; removes ignored files like `.env`/build output). Choose **Any local files** (also protects ignored files except `node_modules`) or **Edited tracked files** (protects tracked edits only).
- **Delete with deleted threads**: Enable **Delete worktrees with deleted threads** to automatically clean up safe worktrees once their last thread is deleted.
- **Scratch folders**: Inactive and settled rules also apply to [no-project scratch folders](./thread-sidebar.md#start-without-a-project) (skipped if they contain a Git repo or enabled scheduled task). Because scratch files aren't in Git, deleted files cannot be recovered.
- **Manual run**: Click **Delete now** to run rules immediately and inspect what was removed, kept, or orphaned (untracked folders are listed but never auto-deleted).
- **Captures and logs**: Browser captures and rotated logs have separate machine-wide retention periods.

## Keep the default branch current

Enable **Settings → Source control & issues → Automatically pull** to fast-forward clean default-branch checkouts from their configured upstream. Skips checkouts with local changes, untracked files, local commits, other branches, or no upstream.

## Scheduled tasks and webhooks

Open **Scheduled tasks** from the icon at the bottom of the sidebar (on mobile, **Settings → Scheduled tasks**) to create and manage tasks.

- **Schedules**: Recurring fixed-time schedules use the target environment's time zone. **Once** uses your local time zone (runs on reconnect if offline).
- **Destination**: Choose **New thread each run**, **One thread (continue conversation)**, or **One thread (fresh session each run)**. Deleting a target thread deletes tasks bound to it; archiving a target thread causes runs to fail (settle it instead).
- **List**: Every environment's tasks appear together: one-time tasks still due come first, soonest at the top, then recurring tasks. One-time tasks that already ran move to **History**, which stays closed until you open it. When a task's last run failed, **Failing** appears above the list and shows only those tasks.
- **Controls**: Edit, pause, resume, run now (except webhook tasks), or delete from the list. Leaving an edited form asks before discarding unsaved changes.
- **Chromeria 1 tasks**: Tasks brought over from Chromeria 1 start paused, and their earlier runs stay as history that never resumes; **Run now** starts a new run. Turn a task on when you stop using it in Chromeria 1.

### Agent-managed scheduled tasks

Agents can schedule work for you with the scheduled task tools, including a task that runs once at a set time. On desktop and web, an **At a time** schedule has more options under its time and days. Ask an agent for the same options, or set them in the task form:

- **Time zone**: Read the times in a named zone, such as `Europe/Warsaw`, instead of the environment's zone. Once set, the zone can be changed but not cleared.
- **Also at**: More times on each day the task runs.
- **Days of the month**: Run on chosen days of the month instead of weekdays. When a month lacks a chosen day, such as the 31st in April, the task runs on that month's last day, so no month is skipped.
- **Spread up to**: Let Chromeria move each time by up to that many minutes to a quieter minute. The list shows the move, such as `09:00 (+4 min)`.

If the environment was off when a task with these options was due, the task runs once when it is back, for the latest missed time. Mobile shows these tasks but cannot edit their schedule.

- **Outcome check**: A shell command that proves the work is done. The agent keeps working in the same thread until the check passes. If it still fails after a few retries, the task shows **Needs you**, and **Run now** continues it in the same thread.
- **Prism role or command**: A task can start each run with a Prism role, or run a shell command instead of an agent. On desktop and web, Chromeria alerts you when a scheduled command starts failing.
- **Spectrum runs**: When a scheduled run asks a Spectrum, the run finishes only after the turn that delivers the Spectrum's report has completed in its thread. If that delivery fails, Spectrum may send the same report again, up to three attempts in total, so a partially processed report can be processed twice. Stop, explicit abandonment and a failure that cannot be retried end automatic retries. If delivery cannot finish, the task shows **Needs you** with the reason. Choose **Abandon report** in web settings or the mobile task menu to stop delivering it and continue to the task's check. An active Spectrum still holds that check.

### Webhook automations

Choose **On webhook** as a task's schedule to trigger runs from external services (e.g., GitHub PR or CI events). A public URL requires a [T3 Connect](./remote-access.md) tunnel; click **Rotate** to invalidate and replace the URL.

- **Prompt placeholders**: Insert `{{body.path}}`, `{{headers.name}}`, `{{query.name}}`, `{{body}}`, or `{{request}}`. Missing values render empty.
- **Untrusted input protection**: Placeholder values are wrapped in `<webhook-data-…>` tags, and `$`/`@`/leading `/` tokens are neutralized so untrusted payloads can't invoke skills or commands.
- **Signatures** (web/desktop setup): Enable **Require signature** to verify HMAC signatures (for GitHub, keep `x-hub-signature-256`, hex, and `sha256=` prefix with the same secret and `application/json`).
- **Filters**: Add conditions (using paths without braces, e.g. `headers.x-github-event` equals `pull_request`, or `body.action` is one of `opened, reopened`). All filters must match; non-matching requests are logged as **Filtered**.
- **Deduplication & coalescing**: Set a **Duplicate request key** (e.g. `{{headers.delivery-id}}`; ignores repeats for 7 days) or a **Coalesce key** with a 10–3600s window (default 120s; runs once with the latest payload when the window closes). Coalesced updates are rate-limited to 60/min (`HTTP 429`).
- **Offline delivery**: By default, offline environments return an error. Enable **Settings → Sharing & access → Hold webhooks while offline** to buffer T3 Connect webhooks for up to 24 hours, and optionally set **Skip requests older than** on the task.
- **Debugging**: Open **Deliveries** from a task's menu (web/desktop) to inspect recent payloads and rendered prompts.

## Spectrum conversations

Set default slots, White, mode and limits in **Settings > Spectrum**, then ask your agent to start a Spectrum with just a question.

- **Slots**: Each slot has a unique label, a primary Color and optional fallbacks used only when that Color cannot start.
- **White**: Has its own primary Color and fallbacks, set separately from the slots. White is not a Color, so it never answers or takes a turn as one.
- **Scope**: Settings apply to the selected environment. A project override replaces the Spectrum defaults for that project; resetting it restores inheritance. A new environment starts without configured slots.
- **Explicit values**: You can supply values when starting. Explicit slots replace the saved slots and have no fallbacks unless supplied. With explicit slots, White copies the first Color's model unless you give White its own. Saved White applies when you use the saved slots.
- **Council**: Collects independent answers, then each Color reads the others' answers and replies for the number of **Council: reply rounds** (at least two), then White writes the final answer.
- **Free**: Each Color speaks once in order; then White picks who speaks next after every turn, or ends the discussion, and writes a closing summary. It ends after **Free discussion: turns per Color** times the number of Colors, or earlier when White says it is done.

The Spectrum thread shows the full discussion. Send a message there to join at the next barrier. **Stop** cancels its owned Drafter work. After the report finishes or you explicitly abandon it, reopen the thread to continue.

A Spectrum an agent starts from its own thread is one of that agent's delegated tasks. The agent can check it with `task_status`, whose result is the report, and stop it with `task_cancel`. The agent still learns of the result from the report message.
