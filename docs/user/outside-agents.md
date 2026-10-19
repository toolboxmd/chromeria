# Outside agents

Agents that T3 Code did not start can work with an environment through its MCP
server. They can read projects and threads, start and message threads, and
check which providers and models are available. This covers Claude Code or
Codex in your own terminal, ChatGPT, and bots that support MCP. Each agent signs
in once, and you choose what it may do.

## Get the MCP URL

In **Settings → Environments**, open a saved environment's menu and choose
**Copy MCP URL**. The URL is the environment's address followed by `/mcp`, for
example:

```text
https://<environment-address>/mcp
```

The copied URL uses the route this device is connected over, so an agent on the
same device can reach it.

- **An agent on your own computers** can use `localhost` on the host, T3
  Connect, or any `https` address, such as
  [Tailscale HTTPS](./remote-access.md#tailscale-https). Signing in sends the
  agent's credentials to the environment, and agents only do that over `https`
  or to `localhost`. A plain-`http` LAN or Tailscale address, like
  `http://100.x.y.z:3773`, lets an agent find the environment but not finish
  signing in.
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
**Settings → Sharing & access** or from `t3 auth pairing create` on the host. A
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

## Scripts on the host

On the machine that runs the environment, `t3 thread` uses the same tools
without a sign-in:

```sh
id=$(t3 thread start . "Find why the tests are slow")
t3 thread send "$id" "Now fix it" --wait
t3 thread show "$id"
t3 thread list
```

`.` means the project that contains the current folder; a project id also
works. A new thread uses the project's default model and permission mode, and
runs in the project folder unless you pass `--worktree <base-branch>`. `send`
never interrupts: a busy thread gets the message as its next turn. Pass `-` as
the message to read it from stdin.

`--wait` prints the agent's reply. It exits 1 when the run fails or stops, and
2 when the agent waits for an answer you give in the app. Add `--json` for a
result that always includes the thread id.

## Remove an agent

Approved agents appear under **Settings → Sharing & access** like other clients.
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
