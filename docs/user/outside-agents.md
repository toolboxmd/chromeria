# Outside agents

Agents that T3 Code did not start can work with an environment through its MCP
server. They can read projects and threads, start and message threads, and
check which providers and models are available. This covers Claude Code or
Codex in your own terminal, ChatGPT, and bots that support MCP. Each agent signs
in once, and you choose what it may do.

## Get the MCP URL

In **Settings → Connections**, open a saved environment's menu and choose
**Copy MCP URL**. The URL is the environment's address followed by `/mcp`, for
example:

```text
https://<environment-address>/mcp
```

The copied URL uses the route this device is connected over, so an agent on the
same device can reach it.

- **An agent on your own computers** can use any address that computer reaches
  the environment at: a LAN or Tailscale address, T3 Connect, or `localhost` on
  the host. See [remote access](./remote-access.md).
- **A hosted agent**, such as ChatGPT or a bot running in the cloud, must reach
  the environment from the internet. Use the T3 Connect address. A Tailscale
  address only works from your own tailnet.

## Approve a sign-in

The first time an agent connects, it opens a sign-in page on the environment.
The page names the agent and where its access goes:

- For an agent on your computer, approval returns to a `localhost` address on
  the computer that opened the page.
- For a hosted agent, approval goes to that service, such as `chatgpt.com`.
  Anyone who runs that service gets the access.

Only approve a sign-in you just started. To approve, enter a pairing code from
**Settings → Connections** or from `t3 auth pairing create` on the host. A
browser already signed in to the environment with permission to manage access
can approve without a code.

Then choose what the agent may do:

- **Read only** is the default. The agent can read projects and threads and see
  the available providers and models. It cannot change anything.
- **Supervised** through **Full access** also let it start, message and stop
  threads in the projects you approve. A thread it starts or steers cannot run with more
  permissions than the mode you chose.

To keep an unattended service connected, choose **Until revoked** instead of the
30-day default. Its OAuth client must save and use refresh tokens, including the
replacement refresh token returned by each renewal. It can renew without another
approval, including after the environment restarts. Immediate retries of the last
refresh return the same replacement for five seconds. Reusing an older token, or
retrying after that window, revokes the sign-in.

Enable **Limit this agent's access** to restrict it to an immutable project id
and named operations: launch, send, read, interrupt and settle. The read operation
allows reading a project or a specific thread and waiting for a thread; settle
allows settling a thread, not other organization actions. Other MCP tools are
unavailable with these limits. Leave the project id empty only when you intend
to allow those operations across all projects. Renewal keeps the exact approved
limits and permission mode.

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

## Claude Code

```sh
claude mcp add --transport http t3 https://<environment-address>/mcp
claude mcp login t3
```

`claude mcp login` opens the sign-in page in your browser. On a machine
without a browser, add `--no-browser`, open the printed URL elsewhere, and
paste the final URL back when asked. `claude mcp list` shows `t3` as connected
once you approve.

## Codex

```sh
codex mcp add t3 --url https://<environment-address>/mcp
codex mcp login t3
```

`codex mcp login --no-browser` prints the sign-in URL instead of opening it.
`codex mcp list` shows `t3` with OAuth once you approve.

## ChatGPT

Add the environment as a custom MCP app in ChatGPT's apps settings:

1. Create a new app or plugin with a custom MCP server.
2. Set the server URL to `https://<environment-address>/mcp`, using the T3
   Connect address.
3. Choose **OAuth** for authentication. ChatGPT discovers the rest; you do not
   need a client ID or secret.
4. Create the app, then approve the sign-in page that opens.

## Other agents and bots

Any agent that supports remote MCP servers over HTTP with OAuth can connect. Give
it the MCP URL and choose OAuth, and the agent finds the sign-in settings itself.
Agents without OAuth support cannot connect.

## Remove an agent

Approved agents appear under **Settings → Connections** like other clients.
Revoke one there to cut off its access and renewal immediately. By default a
sign-in lasts 30 days; after that the agent asks you to approve it again. An
agent approved **Until revoked** renews its sign-in automatically.

## Launch with only named context

Pass `bare: {}` to `t3_thread_launch`, `delegate_task`, a `create_threads` entry,
`schedule_task`, or a Spectrum Color to strip the context that harness can disable.
Bare keeps native shell and file tools. Permissions use your approved ceiling;
it cannot give a child more access than its caller.

Name additions in order, for example
`bare: { additions: [{ type: "skillFile", path: "/project/library/testing/SKILL.md" }] }`.
A skill contributes its body only. Its dependencies are not enabled. `skill` looks
up a name in the destination's skill inventory; `file` includes a readable absolute
path. Named `mcp` server `t3-code` is supported except on Grok; exact plugins, apps,
other MCP servers and individual MCP tools currently return an unsupported reason.
Missing additions fail before inference. A changed addition fails when a new native
context needs its body again.

The thread shows a bare receipt with its permissions, planned stripping, native
evidence, unobserved categories and native record location. Native content that
cannot be disabled remains disclosed there. Cursor's custom prompt access remains
unverified. OpenCode 2, external OpenCode servers, Antigravity and generic ACP
adapters currently refuse bare launches.

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
