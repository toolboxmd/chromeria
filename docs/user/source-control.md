# Source control

T3 Code integrates with GitHub, GitLab, Forgejo, Gitea, Bitbucket, Azure DevOps, GitCafe, Cursor Origin, Linear, and Jira Cloud to clone/publish repos, review and merge pull requests, manage issues, and watch CI.

## Connect an account

Install Git and authenticate on the machine running your T3 Code server (for remote environments, do this on the remote machine), then click **Rescan** in **Settings → Source control & issues**.

| Host                               | Setup                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **GitHub**                         | 1) Save a token in **Settings → Source control & issues → GitHub**, 2) set `GH_TOKEN` (`GH_ENTERPRISE_TOKEN` + `GH_HOST` for Enterprise), or 3) sign in with [GitHub CLI](https://cli.github.com/) 2.81.0+ (`gh auth login`; use `--hostname` for Enterprise). Pick accounts or disable hosts in Settings when multiple `gh` logins exist.                                                                                |
| **GitLab**                         | Install [GitLab CLI](https://gitlab.com/gitlab-org/cli) and run `glab auth login`.                                                                                                                                                                                                                                                                                                                                        |
| **Forgejo / Gitea**                | Install [`fj`](https://codeberg.org/forgejo-contrib/forgejo-cli) (`fj --host https://your-server auth add-token`) or [`gt`](https://gitea.com/gitea/cli) 2.0+ (`gt auth login --host https://your-server`; it replaces the archived `tea`, so sign in again). Prefers `fj`, falls back to `gt` (use `gt` for subpath-hosted servers). Use full URLs when multiple servers exist.                                          |
| **Bitbucket**                      | Save a repo/project/workspace **Access token** or **Atlassian email + API token** (with repo/PR read-write, `read:user:bitbucket`, `read:workspace:bitbucket`) in **Settings → Source control & issues → Bitbucket**, or set `T3CODE_BITBUCKET_ACCESS_TOKEN` / `T3CODE_BITBUCKET_EMAIL` + `T3CODE_BITBUCKET_API_TOKEN` in the server env (or [`service.env`](./background-service.md#configure-the-service-environment)). |
| **Azure DevOps**                   | Install [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/), run `az extension add --name azure-devops`, then `az login`.                                                                                                                                                                                                                                                                                           |
| **GitCafe**                        | Save a token in Settings, set `CAFE_TOKEN` (+ `CAFE_HOST`), or install `@gitcafe/cli` and run `cafe auth login --host https://git.cafe/api`. Line comments are not yet supported; stack merges/updates are native.                                                                                                                                                                                                        |
| **Cursor Origin**                  | Install [Origin CLI](https://cursor.com/docs/origin/cli) and run `origin auth login` (`origin auth setup-git` if push prompts for credentials). Uses private HTTPS repos; GitHub-mirrored PRs stay on GitHub.                                                                                                                                                                                                             |
| **TanStack Source** (private beta) | Set `T3CODE_FEATURE_FLAGS=tanstack_source` and restart the server. Save a token in Settings, set `SRC_TOKEN`, or run `src auth login --web`. Supports live CI logs, private PRs on public repos, and issues.                                                                                                                                                                                                              |

> Note: Git `push` and `clone` still require Git SSH/HTTPS credentials on the server, separate from API tokens.

> Note: With several `gt` servers and no `fj` login, `owner/repo` and newly published repositories go to `gt`'s default server (`gt config set default_host https://your-server`). With several `gt` accounts on one server, pick one with `gt auth switch <user> --host https://your-server`. If an SSH remote uses a different host name than the server's web address, run `gt config set ssh_host <ssh-host> --host https://your-server`.

## Start, clone, or publish a project

- **New project**: Run **New project** from the command palette (`Cmd/Ctrl+K`) or **Add Project** to initialize a new Git repo in `~/.t3/projects/<slug>` with a README, icon, and first commit (optionally creating a private GitHub repo).
- **Clone**: Use **Add Project** to pick a host (enable non-GitHub hosts in **Settings → Source control & issues**) or paste a Git URL or web page URL. Cloning runs in the background so you can draft your first prompt immediately; falls back to HTTPS if SSH auth fails. Enable **Clone into host folders** (**Settings → Threads**, or **Source control** on mobile) for `ghq`-style `<host>/<owner>/<repo>` paths.
- **Publish**: For a local repo without a remote, **Publish Repository** creates the remote, adds `origin`, and pushes commits.

## Review turn changes

A turn's diff shows only that turn's own edits, commits, and conflict fixes—excluding unrelated files brought in by a pull, merge, or rebase. Use the branch comparison to view everything changed against your base branch.

- **Commits view** (web/desktop): Switch the Diff pane source picker to **Commits** to inspect the latest 100 local commits one by one.
- **Commit links**: Links to local commits open in the Diff pane (follows **Settings → Diffs → Open pull requests in T3 Code**; `Cmd/Ctrl+Click` opens in browser).
- **Multi-repo projects**: Use the repository picker in the Diff pane and expand per-repo rows in thread details. See [Projects with several repositories](./multi-repo-projects.md).

## Create, review, and merge pull requests

- **Create**: Commit, push, and open PRs from a thread's Git actions. Configure AI-generated commit/PR writing style in **Settings → Source control & issues → Commits & pull requests** (**Repository conventions** or **Custom instructions**).
- **Review**: Open **Pull requests** to inspect diffs, comments, checks, commits, and request reviewers. Draft line comments stay local until submitted. If new commits are pushed while you review, click **Load new commits** before merging or submitting; until you do, a merge or review is refused and your review draft is kept. Merging from a thread's PR row or the list lands the head that row had when you confirmed. On GitHub, a merge from the PR panel or an agent's merge tool waits while a required check is still running, names a required check that failed or was cancelled, and never skips those checks through an administrator bypass.
- **Saved views**: Save and update filter/sort setups under **Views → Save current view** (stored per device).
- **Mark files viewed**: Check off files in the **Code** tab to collapse them (reopens as **Changed** on new pushes). Syncs natively with GitHub; stored on your T3 Code server for other hosts.
- **Mobile**: Open **Pull requests** across environments (cached lists, details, guides, and diffs stay readable offline). Tap **Work on PR** to start a worktree thread on the PR's branch.
- **Quick actions & options**:
  - Enable **Remove agent credits when merging** in **Settings → Source control & issues** to strip recognized agent co-author/generated-by trailers on GitHub merge/squash commits (keeps human co-authors).
  - Hold `Shift` in the GitHub PR list (web/desktop) to drag-close multiple PRs (`Esc` cancels).
  - GitHub, GitLab, and Azure DevOps support auto-merge; GitHub also supports approving fork workflows and opening revert PRs.
  - Azure DevOps comments must be edited on the host site; Bitbucket cannot reopen declined PRs.

### Review guides (beta)

Turn on **Settings → Source control & issues → Review guides** to add a **Guide** tab to pull requests. Click **Write guide** to have Claude or Codex read the PR in a temporary read-only checkout and produce an ordered tour of the change (highlighting **Look closely** spots and grouping mechanical changes at the end).

- Customize emphasis/ordering via **Review guide instructions** in **Settings → Source control & issues** (project-overridable).
- On web, select lines in any stop to add review comments or ask an agent about that stop.
- Agents can also generate guides for multiple PRs when asked. Choose **Rewrite guide** from the PR's **⋯** menu after new pushes.

### Pull request sharing across environments

Disabled by default. In **Settings → Environments → Pull request sharing** (**Pull request routing** on mobile), opt in trusted environments to **Read PRs** or **Read and act** so review reads/actions can route through another connected environment signed into the same account on the same host (credentials never leave their machines). Enable on both source and target environments; only grant **Read and act** to environments you control.

## Linked pull requests and watching

Threads can link multiple PRs (even across repos on the same host). Creating a PR via Git actions, `gh pr create`, `glab mr create`, or an agent's `link_pull_request` links it automatically; manage links from **Linked items** or the command palette.

- **Agent PR tools**: Agents can read, open, update, comment on, and merge PRs in the thread's repo using your configured sign-in (refusing merges if the head moved since last read).
- **Watch / babysit a PR**: Ask an agent to watch or babysit a PR (or toggle it in **Linked items** / click the eye icon in thread details). The server polls every 2 minutes and wakes the agent on failed checks, passed required checks, new comments/reviews from others, conflicts, merge-queue drops, or merge/close.
  - Watched threads stay in the **Working** section while checks run (or until merge/close if the agent is waiting to act) and never auto-settle.
  - Watching stops automatically on merge/close, Stop, settle/archive, 10 comment-only wakes in a row, or 8 consecutive read failures. Subagents cannot watch PRs—the parent thread watches instead.

## GitHub stacks

The Pull requests page displays stack layer positions and navigation:

- **Merge stack**: Merges the selected PR and every open layer below it together, respecting branch rules and merge queues.
- **Rebase stack**: Updates remote branches bottom-to-top without modifying your local checkout (may rewrite history and restart checks).

## Issues

Open **Issues** from the sidebar utility row or **Open issues** in the command palette to browse, filter, search, save views, comment, edit, and transition issues across GitHub, GitLab, Azure DevOps, Forgejo, Linear, and Jira Cloud.

- **Linear**: Connect an API key in **Settings → Source control & issues** and bind one or more teams per project (supports multiple Linear accounts). Agents file new issues in the first bound team unless another is named. An issue linked by its full URL needs no binding: any thread, No project threads included, can open and change an issue from a connected workspace, as far as your Linear account allows.
- **Jira Cloud**: Connect your site (`yourteam.atlassian.net`), account email, and an [API token](https://id.atlassian.com/manage-profile/security/api-tokens) in **Settings → Source control & issues**, then bind Jira projects (supports multiple Jira sites and accounts). Issues move along their workflow's transitions; reactions and linked pull requests are not available, and new issues are filed as a Task. On a Jira Service Management issue, a comment is an **Internal note** the customer never sees unless you switch it to **Reply to customer** before sending.
- **Link to threads**: Click **Link to this thread** on an issue beside a thread (or let agents call `link_issue`) to track it in **Linked items**.
- Remote issue writes require the `source-control:write` scope.

### GitHub Issue status

You can also switch to **Issues** from the PRs/Issues switch on the Pull Requests page. For GitHub, the list covers the repositories of your projects on every connected environment.

Open Issues are grouped by whose move it is, worked out from GitHub and your threads; nothing is
written to GitHub. The first rule that applies wins, and the row's second line says why:

| Group          | When                                                                                         | The row says                        |
| -------------- | -------------------------------------------------------------------------------------------- | ----------------------------------- |
| Your move      | The Issue has the label `needs-user`                                                         | Needs you                           |
| Your move      | A linked thread, or a thread it started, waits for your approval or answer                   | A thread is waiting for your answer |
| Waiting        | The Issue has the label `waiting`: a date or someone outside holds it                        | Waiting                             |
| Agents working | A linked thread works now                                                                    | Its status, and any open blockers   |
| Agents working | An open pull request is in review                                                            | In review                           |
| Agents working | An open pull request, not a draft, passed review and its checks are running                  | Reviewed, checks running            |
| Your move      | An open pull request, not a draft, passed review and its checks have finished                | Reviewed, not merged                |
| Blocked        | An open Issue blocks it on GitHub                                                            | Blocked by N open Issues            |
| Stalled        | A thread is linked, or a task branch or open pull request exists, and none of the above fits | Its status (see below)              |
| Nobody on it   | Nothing was started                                                                          |                                     |

The two labels are yours and your agents' to set; Chromeria only reads them, from an Issue's first 20
labels. A thread that ended its turn with a question in plain text is not a pending request, so its
Issue shows as Stalled until someone adds `needs-user`.

A reviewed pull request is your move only when Chromeria can tell its checks have finished: all of
them ended, or its head has none. While one still runs the Issue stays with the agents, also when
another check has already failed. When that cannot be told, for example from a server that predates
this, the Issue shows as Stalled with the status Waiting for merge.

A Stalled row names its status, with two exceptions. A reviewed draft says Paused: a draft waits for
no merge. A row with no open pull request whose linked threads are all archived says "Last linked
thread archived".

When the reason names a thread or a pull request, a link right after it opens that thread or pull
request; on a narrow row the link shows as an icon. Clicking anywhere else on the row opens the
Issue.

Above the list, one line gives the open Issues of the listed repositories and the week's flow:
"113 open Issues", then "Last 7 days: 12 filed, 9 closed". The 7 days are the 168 hours before the
numbers were read, not calendar days. Filed counts the Issues created in them and closed the Issues
closed in them, so the two show whether the pile grows; they are not the change in the open total,
because a reopened Issue grows the pile without being filed.
These numbers cover every listed repository whatever the list is narrowed to. Under a search text,
a filter or a repository the line reads "5 of 113 open Issues"; with State set to Closed or All it
counts the rows shown and gives the open total beside them. The line also says how many open Issues
nobody updated for 30 days, while the list holds every open Issue.

The total and the week's flow are read when the page opens and when you press Refresh, not when
you sort, search or filter, so they can lag the rows by a few Issues until the next Refresh. While
Refresh reads them again, the numbers stay in sight with a `~` in front; rows that are only being
read again do not mark them.

If the rows come to hold more open Issues than the total that was counted, that total is out of
date and is not shown. The unfiltered Open list then counts its own rows: exactly once it is read
to its end, and with a `+` while more are arriving. Under a search, a filter that GitHub applies,
or another State, the line gives the rows' own count and "Open Issues: unknown" until the next
Refresh.

The totals are for the repositories listed, and need each of them counted by a server that
answers. A server that stopped answering, or whose listing GitHub refused, does not hold them back
when another server lists the same repositories; it does when it listed a repository no other
server has. A server that is not connected and has listed nothing since the page was opened adds no
rows and no repositories, so the numbers go without it. When they cannot be read, the line says
"Last 7 days: unknown" instead of a number: a connected server has not yet said what it lists, a
server is too old to count, or GitHub did not answer in time. The rows are not affected.

A count ends in `+` while more Issues are still being read, and a group's count starts with `~`
until every Issue and its threads are read, because a row can still move to another group.

The status a row names is the first of these that applies:

| Status             | When                                                                |
| ------------------ | ------------------------------------------------------------------- |
| Done               | Closed as completed                                                 |
| Not planned        | Closed as not planned or duplicate                                  |
| In review          | An open pull request's head has a pending `review/independent` mark |
| In progress        | A task branch or open pull request exists and a linked thread works |
| Waiting for merge  | The review mark passed                                              |
| Changes requested  | The review mark failed                                              |
| Waiting for review | A pull request is open and ready for review (not a draft), unmarked |
| Paused             | A task branch or draft pull request exists and nobody is working    |
| Blocked            | An open Issue blocks it on GitHub                                   |
| Discussion         | A thread is linked, with no branch or pull request                  |
| To do              | Nothing is linked                                                   |

The pull requests that count are the open ones that close the Issue (`Closes #N`) and the open
ones linked to its linked threads, so a pull request into a branch other than the default one
counts too. A task branch is a linked thread's branch named `<type>/<number>-<slug>`. Review marks
count only when the GitHub account of a server that lists the Issue posted them. A thread counts
as working while it or any of its subagent threads works, on any connected server.

Closed Issues keep GitHub's own two groups, Done and Not planned, which start collapsed. **Group by
parent** shows the sub-Issue tree instead, including sub-Issues in repositories outside your
projects, which are marked as such and open on GitHub. Search, sort and filter by group (the
**Status** filter), whether a thread is linked, repository, labels, milestone and parent. Rows show their linked threads; select one to open it.
Select an Issue to read it, see its linked threads and pull requests, comment, close or reopen it,
or start a thread from it: the new thread opens in the matching project with the Issue in the
composer, already linked. Selecting an Issue in a thread's **Linked items** panel opens it
beside the thread. An Issue's pull requests and GitHub Issue links in chat open in the app: pull
requests in the pull request panel, Issues beside the thread, and anything else as a normal link,
following the **Open links in** setting. Cmd/Ctrl-click opens GitHub directly. While the list is open, the command palette searches it too. Anywhere,
typing `#12`, `owner/repo#12` or an Issue URL in the command palette finds the threads linked to
that Issue.
A bare `#12` for an Issue the list has not loaded finds only threads linked by hand, by an agent,
by starting from the Issue or by branch name, not through a pull request that closes it.

Commenting on, closing or reopening an Issue needs the `source-control:write` permission, like
pull request changes; standard pairings include it. A client paired without it can read Issues
but not change them; give it the permission with a fresh pairing link, as described in
[Manage or revoke access](./remote-access.md#manage-or-revoke-access).

To link a thread by hand, open its **Linked items** panel, choose **Link** and enter the
Issue's number or URL; the same panel removes a link, and a removed branch link stays removed.
Agents can link and unlink their own thread with the `link_issue` and `unlink_issue` MCP tools.

## Feature map

The **Map** tab beside a thread draws the thread's project from `docs/map.md` on its GitHub default
branch, and colours each part by what GitHub shows now: planned, working, broken or done. Open it
from the add-tab menu (`A`). The tab rereads GitHub every minute while shown and when the window
regains focus. If GitHub is unreachable, it keeps the last map and shows when it was read.

The file holds one fenced `mermaid` flowchart. Declare each part once as `ID["Name"]`, and bind it
on its own comment line, which GitHub ignores:

```text
C["Capture"]
%% C: #7 PR #22
SR["Serve report"]
%% SR: PR #38 #15
S["Session"]
%% S: context
B["Existing base capability"]
%% B: live
```

A ref is `#7`, `PR #38`, `owner/repo#231` or `owner/repo PR #227`, on the project's GitHub host. The
first ref is the primary ref, and it alone decides planned, working or done:

| Primary ref | Planned                                     | Working                                                  | Done                                 |
| ----------- | ------------------------------------------- | -------------------------------------------------------- | ------------------------------------ |
| Issue       | Open, with no open closing PR and no thread | Open, with an open closing PR or a linked working thread | Closed as completed                  |
| PR          | (never)                                     | Open, or merged into a branch other than the default     | Merged into its repository's default |

A part is broken instead while any open Issue among its refs has the `bug` label, or while an open
PR of the part (a PR ref, or a PR that closes its primary Issue) fails its checks. A merged
component PR does not finish an open Issue: the part stays planned or working until GitHub closes
the Issue. A primary ref closed as not planned, or closed unmerged, shows as cancelled. A ref that
GitHub does not return, because it is private or deleted, shows as unavailable. If an open Issue
has more labels or closing PRs than one GitHub page holds, a part they could break shows as
incomplete instead of a state. A primary ref the map cannot read shows as unknown; the next ref
never takes its place. `context` marks a part another project owns; it is drawn grey and never
counted as missing. `live` marks an existing capability in the mapped project, asserted by the
map author rather than inferred from GitHub. Live is distinct from Done and Context and is not
counted as an untracked part. Live, context and refs cannot be combined; contradictory bindings
appear under unreadable refs.

On github.com, a map from `toolboxmd/chromeria` marks tracked parts with the Chromeria crystal
in the accessible part list. This identifies Chromeria-tracked work, regardless of its status,
rather than complete historical provenance. Live and context parts carry no crystal.

A part that a thread is working on carries a thread icon; select it to open that thread. Select a
part, or its row in the list under the drawing, to open its primary ref. The gaps list shows parts
with no refs, Issues bound to more than one part, open Issues on no part (marked partial after the
first 500), cancelled, unavailable or incomplete refs, ref lines that name no part, and refs the map
cannot read.

## Troubleshooting

- **Not authenticated**: Run the host's CLI login on the server and click **Rescan** (for Bitbucket/tokens, verify Settings or server env vars).
- **GitHub verification fails**: Upgrade `gh` to 2.81.0+ or save a token in Settings.
- **Push fails**: Check Git SSH/HTTPS credentials separately from API auth.
- **Fork PR badge missing**: Set upstream with `git branch --set-upstream-to=<remote>/<branch>`.
