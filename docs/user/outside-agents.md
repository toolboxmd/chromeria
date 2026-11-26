# Outside agents

Connect external agents (Claude Code or Codex in your terminal, ChatGPT, or any OAuth-capable MCP client) to your T3 Code environment over MCP to read projects/threads, launch and message threads, and monitor events.

## Quick setup

Open the command palette (`Cmd/Ctrl+K`) and run **Use T3 Code in another app (MCP)** (or open **Settings → Sharing & access**), pick your app, and follow the prompts.

- Cloud assistants (like ChatGPT or Claude) require **T3 Connect** enabled on the environment.
- To also reach other machines through the same connection, enable **Settings → Labs → Linked environments** and [link them](./remote-access.md#link-another-environment).
- To copy the raw MCP endpoint (`https://<environment-address>/mcp`), open an environment's menu in **Settings → Environments → Copy MCP URL**. OAuth requires `https://` or `localhost` (plain `http://` LAN/Tailscale addresses cannot complete OAuth).

### CLI clients and ChatGPT

- **Claude Code**:
  ```sh
  claude mcp add --transport http t3 https://<environment-address>/mcp
  claude mcp login t3   # add --no-browser on headless hosts
  ```
- **Codex**:
  ```sh
  codex mcp add t3 --url https://<environment-address>/mcp
  codex mcp login t3    # add --no-browser on headless hosts
  ```
- **ChatGPT / other OAuth MCP clients**: Add a custom remote MCP server pointing to `https://<environment-address>/mcp` with **OAuth** authentication (no client ID or secret needed).

## Approve access and permissions

The first time an outside agent connects, approve its sign-in page (via your T3 Connect account, an already-authorized browser, or a one-time pairing code from **Settings → Sharing & access** / `t3 auth pairing create`).

- **Read only**: Inspect projects, threads, providers, and models.
- **Supervised → Full access**: Also launch, message, and stop threads in the projects you approve, up to the granted permission mode. Managing [workflow runs](./workflows.md) or using approval-skipping agent modes requires **Full access** (or your policy's maximum mode).
- Sign-ins last **30 days** by default. Revoke one anytime in **Settings → Sharing & access** to cut off its access and renewal immediately.
- **Until revoked** keeps an unattended service connected. Its OAuth client must save and use refresh tokens, including the replacement refresh token returned by each renewal. It renews without another approval, including after the environment restarts. Immediate retries of the last refresh return the same replacement for five seconds. Reusing an older token, or retrying after that window, revokes the sign-in.
- **Limit this agent's access** restricts it to an immutable project id and named operations: launch, send, read, interrupt and settle. Read allows reading a project or a specific thread and waiting for a thread; settle allows settling a thread, not other organization actions. Other MCP tools are unavailable with these limits. Leave the project id empty only when you intend to allow those operations across all projects. Renewal keeps the exact approved limits and permission mode.

## Available tools and limits

Tools that act as a running T3 thread (`delegate_task`, `create_threads`, `request_secret`, `html_render`, `preview_*`, `device_*`, `t3_worktree_handoff`) only work for agents running **inside** T3 Code.

- Use `t3_thread_launch` + `t3_thread_wait` / `t3_thread_read` to run and follow work.
- Pass `projectId` or `threadId` explicitly (or `allProjects: true` on `t3_thread_list`).
- **MCP Events (`io.modelcontextprotocol/events`)**: Bots can poll, stream, or subscribe via HTTPS webhooks (up to 1-hour leases; latest 1,000 milestones buffered in memory) for turn, approval, guest, PR watch, and workflow events. For local webhook testing, set `T3CODE_MCP_WEBHOOK_ALLOWED_HOSTS=localhost,127.0.0.1` on the server.

## Permissions between linked environments

On the receiving environment, open **Settings → Connections** to choose a linked
environment's access to existing resources, onward delegation, browsers, and
devices. On mobile these permissions are in the environment's settings. Save
only the permissions you want that environment's new work to use.

Existing delegated work keeps the ceiling it started with, even after a restart
or a permission increase. Removing permission also limits existing work. Update
both environments before using remote return or onward delegation.

A remote delegated child can queue a notice to its own parent without a reverse
link. This does not let it interrupt the parent or use unrelated threads. A
pending return receipt means the notice is stored for delivery; the parent has
not acknowledged it yet.

## Use a linked browser or device

Keep your client connected to the source environment. Open a resource returned by
its tools to view the linked environment without saving another connection.
Closing the view releases that viewer; `t3_preview_close` or `device_close`
closes the tool session. Reopening creates a new session, so use its new reference.
Both environments must support remote viewers and permit the requested access.

For devices, use `device_commands` to discover supported commands, then pass the
qualified target from `device_open` to `device_execute`. Use snapshot references
for clicks and literal text for fills. Follow the returned typed examples; a
remote device does not require running a command on its host.

Use `t3_attachment_inspect` to obtain a verified reference after an upload.
Pass qualified file references to supported device commands or `preview_upload`;
a file path on your computer is not a file on the linked computer. Use
`t3_attachment_acquire` to bring a supported result into the source environment.
Browser downloads larger than 50 MiB remain available through the authenticated
viewer download, but cannot be acquired as agent attachments.

## Recover an interrupted call

For tools that accept a request key, keep the same key when retrying the same
operation after a lost reply or sign-in renewal. Changing the operation needs a
new key. If a tool reports `outcome_unknown`, inspect its destination before
starting another action: the original action may already have happened.

A keyed snapshot save returns its original PNG artifact receipt; its qualified
path is in `artifact.screenshotPath`. For a remote saved snapshot, its server
must remain reachable and your access must still permit that preview. Its
renderer can be disconnected; update both servers if the renderer-free replay
check is unsupported. Ordinary snapshot inspection remains available separately. A confirmed-operation receipt describes the original acceptance. Read current
preferences with `t3_environment_read`, scheduled tasks with
`list_scheduled_tasks`, and links with their list tools. For a new timer, report
the current cadence and next run time from that list. For a webhook, read its
current public URL there.

Linked environments can transfer supported attachments with a send or launch.
The source must own or be authorized to read the attachment, and the destination
checks the received bytes before accepting it. A local path alone is not a
transferable attachment.

## Host scripts (`t3 thread`)

On the server machine itself, drive threads directly without OAuth:

```sh
id=$(t3 thread start . "Find why the tests are slow")
t3 thread send "$id" "Now fix it" --wait
t3 thread show "$id"
t3 thread list
```

- Pass `--worktree <base-branch>`, `--title` / `--generate-title`, `--provider <id> --model <slug>`, `--option id=value` (e.g. `reasoningEffort=high` for Codex, `effort=high` for Claude), or `-` to read the prompt from stdin.
- `--wait` prints the reply and exits `1` on failure/stop or `2` when waiting for user input. Add `--json` for structured output including the thread ID.

## Launch with only named context

Pass `bare: {}` to `t3_thread_launch`, `delegate_task`, a `create_threads` entry,
`schedule_task`, or a Spectrum Color to strip the context that harness can disable.
Bare keeps native shell and file tools. Permissions use your approved ceiling;
it cannot give a child more access than its caller.

Name additions in order, for example
`bare: { additions: [{ type: "skillFile", path: "/project/library/testing/SKILL.md" }] }`.
A skill contributes its body only. Its dependencies are not enabled. `skill` looks
up a name in the destination's skill inventory; `file` includes a readable absolute
path. Named `mcp` server `t3-code` is supported except on Grok. To attach only
specific tool descriptions, add one entry per tool, for example
`{ type: "mcp", server: "t3-code", tool: "preview_open" }`. Omitting `tool` attaches
the whole server. Exact plugins, apps and other MCP servers return an unsupported reason.
Missing additions fail before inference. A changed addition fails when a new native
context needs its body again.

The thread shows a bare receipt with its permissions, planned stripping, native
evidence, unobserved categories and native record location. Native content that
cannot be disabled remains disclosed there. Cursor's custom prompt access remains
unverified. On OpenCode 2, bare runs on a private OpenCode server and keeps the
model identity, date and environment block that OpenCode always adds. Bare OpenCode
keeps the providers defined in your global OpenCode config, so local models work bare;
the launch fails before inference, naming the config file, when that file is not valid
JSONC before `{env:}` substitution, when an `{env:}` value would change its JSON structure
or uses a variable bare overrides such as `XDG_CONFIG_HOME`, or when a provider uses a
`{file:}` path relative to the config folder, or when providers are spread over more than one
global config file. Use an absolute or `~/` path for provider files.
On OpenCode 2, bare reads provider changes when the OpenCode provider restarts. OpenCode
always loads managed config set by an administrator; on OpenCode 2 a bare launch fails
before inference when that config adds instructions. OpenCode substitutes provider `{file:}`
paths itself, so if a file is unreadable, OpenCode's own log records the full path,
including any `{env:}` value inside it; keep secrets out of file paths. External
OpenCode servers, Antigravity and generic ACP adapters currently refuse bare launches.

To start a bare thread yourself, turn on **Bare** in the model picker of a new
thread, or in a new task's options on mobile. It starts off for every new thread
and is unavailable, with the reason, where the selected provider refuses bare
launches. The thread header then shows **Bare**.

Bare is fixed when the thread starts. Native forks inherit it; importing or
transferring portable history is unsupported. Start a normal thread to use ordinary
context. A bare schedule without an explicit bind starts a fresh thread each time.

## Restrict a thread's tools

`delegate_task` and `t3_thread_launch` accept an optional exact allowlist:

```json
{
  "runtimeMode": "full-access",
  "bare": {},
  "tools": {
    "allow": ["mcp:outreach:read_venue", "mcp:outreach:submit_result"],
    "mcpServers": {
      "outreach": {
        "type": "http",
        "url": "http://localhost:8080/mcp",
        "headers": { "Authorization": "Bearer <venue-generation-token>" }
      }
    }
  }
}
```

With this option, built-in tools such as `Bash` and `Read` must be listed by name;
`allow: []` disables every tool. Wildcards and permission patterns are rejected.
Without `tools`, existing launches keep their normal permissions, including bare
launches. Restrictions require a Claude thread with full-access runtime and default
interaction mode. Other modes are rejected at launch; changing a restricted thread
to an unsupported mode is also rejected. Pass a Claude model selection explicitly
if your Prism kit normally selects another harness. Other harnesses reject the
launch until they have a proven exact-tool enforcement path. An older linked environment
that does not advertise support also rejects restricted requests.

An outside OAuth client can pass both options to `t3_thread_launch` with its
project and model selection. MCP attachments use HTTP URLs and headers. Process-spawning `stdio` attachments
are rejected. Configuration stays in the private secret store;
thread state, exports and `t3_thread_configuration` contain only the allowlist and
an opaque reference. Follow-ups, restart and compaction retain the restriction and
restore the private configuration.

Use `t3_thread_mcp_configure` with `threadId` and `servers` to replace one idle
thread's attachments without changing its allowlist; `servers: {}` removes them.
Other threads keep their own configuration. Revoke the old token at its issuer
when rotating a generation. Superseded configuration records remain in the private
secret store; removal detaches them from the thread rather than erasing that store.
Stop active work before changing its configuration.

To keep credentials out of every conversation, pass them from an outside OAuth
client, as in the venue service. A model that supplies a token in its own tool
arguments has already exposed it in its caller conversation; private child
configuration cannot erase that caller history. Non-bare restricted threads report
their safe policy through `t3_thread_configuration`; bare threads also include it
in their receipt.
