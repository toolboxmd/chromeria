# Remote access

Drive T3 Code from your phone, browser (`app.t3.codes`), or another desktop app while your agents run on a host computer or server. The host must stay awake and running while you work.

## T3 Connect (recommended)

T3 Connect links your host to your other devices without router port-forwarding.

- **Desktop host**: Open **Settings → Sharing & access**, sign in, and turn on **T3 Connect** (note: closing the desktop app's last window stops its local server—use the [background service](./background-service.md) for unattended access).
- **CLI / headless host**: Run `t3 up` (or `t3 connect` / `t3 up --connect --lan --yes` for non-interactive scripts) and complete the printed sign-in link on any device. Check status with `t3 status [--json]`.
- **Other devices**: Sign in to the same T3 Connect account and pick the environment.

### Approve new devices

By default, any device signed into your T3 Connect account can connect. Turn on **Require approval for new devices** in **Settings → Sharing & access** on the host so new devices must present a short code (valid 10 minutes) that you approve from an existing device or via `t3 auth connect-device approve <code>`. Revoke devices anytime in **Settings → Sharing & access** or `t3 auth connect-device revoke <code>`.

### More machines with T3 Chat

Free T3 Connect accounts can link up to **3 host machines** (phones and browsers don't count). Link a paid T3 Chat account from your account menu → **T3 Chat → Link T3 Chat** to raise the limit to **10** (Pro) or **100** (Premier).

## Pair over a LAN or private network

- **Desktop host**: Enable **Settings → Sharing & access → Network access** (restarts desktop) and create a one-time pairing link.
- **CLI host**: Run `t3 up --lan` (background service) or `t3 serve --lan` (foreground), then run `t3 pair` to generate a QR code / pairing URL (`t3code://pair` opens directly in the desktop app).
- **Hosted web app (`app.t3.codes`)**: Requires an HTTPS endpoint (T3 Connect or Tailscale HTTPS). For plain HTTP on a LAN, open the direct pairing URL in a local browser or pair from the desktop/mobile app. For custom CAs on Android, install the CA certificate in Android security settings.
- **Session lifetime**: Paired devices stay signed in while active; sessions unused for 30 days must re-pair.

### Reach one machine several ways

A host can have multiple routes (LAN, Tailscale, VPN, public URL, SSH, T3 Connect). T3 Code automatically learns LAN and Tailscale routes when network access is enabled, connects over the fastest working route, and switches back to higher-priority routes when you return home. Reorder or add routes in **Settings → Environments** (**Add route**).

When you sign in to T3 Connect, a saved remote machine also gains its T3 Connect route automatically once its managed tunnel answers and supports this client. The new route goes after LAN and Tailscale and uses the machine's existing tunnel; it does not create another one. Without a T3 Connect sign-in, your paired routes keep working. If the account no longer lists that tunnel, its relay route is removed when another independently authorized route remains. A machine whose only routes depend on T3 Connect stays saved to preserve its local data. Removing a relay route by hand can cause it to be learned again while the account still lists the tunnel.

### Tailscale HTTPS

Join both devices to a tailnet with [MagicDNS and HTTPS certificates](https://tailscale.com/docs/how-to/set-up-https-certificates) enabled (on Linux, grant [operator permission](https://tailscale.com/docs/reference/troubleshooting/linux/linux-operator-permission)).

- **Desktop**: Enable **Settings → Sharing & access → Tailscale HTTPS**.
- **CLI**: Run `t3 serve --tailscale-serve` or `t3 pair --tailscale` (use `--tailscale-serve-port` if 443 is taken; remove with `tailscale serve --https=443 --set-path=/ off`).

### Balance new threads across machines

On web and desktop with 2+ connected machines, enable **Settings → Environments → Load balancing** (per client) to auto-select a host for new threads in shared projects. Set each machine to **Prefer**, **Normal**, **Less often**, or **Manual only**.

Agents can read each machine's current CPU and memory load with `t3_environment_read`. With **Auto balance** on (**Settings → Connections → Load balancing**), agents on this machine also start their worktree tasks on whichever linked machine has the most free CPU and memory, this one included, using the same per-machine preferences; **Manual only** never receives them. A task moves only when its branch is pushed and the agent names no machine. Auto balance is saved on the machine your app runs on (an app's earlier choice moves there the first time it connects), so turn it on in each machine's app whose agents should balance. If the chosen machine lacks the project, it starts cloning it in the background and the task runs here.

## Desktop-managed SSH

In the desktop app, open **Settings → Environments → Add environment → SSH** and enter a host or SSH alias (e.g. `user@example.com`). T3 Code downloads its runtime to `~/.t3/runtime` on the Linux or Apple Silicon macOS host, starts or reuses a server, and manages port forwarding automatically.

- Provider CLIs must be on a non-interactive login shell's `PATH` (check with `ssh user@example.com 'sh -lc "command -v claude codex"'`).
- For 2FA/Duo hosts on macOS/Linux, enable SSH `ControlMaster` in `~/.ssh/config` and open a terminal SSH session first.
- Revoke unused SSH sessions in **Settings → Sharing & access** or `t3 auth session revoke <id>`.

## Browser on a remote environment

Browser tabs belong to the host environment so you and your agents share them from any device.

- **Streaming & local loading**: Web and mobile stream tabs from the host. The desktop app shows the tabs you open in its own environment directly; agents' tabs stream from the host. In the desktop app, tabs you open for a remote environment load locally while routing `localhost` requests to the host over the **preview tunnel** (so element picker and annotations work locally).
- **Port forwarding (desktop)**: Forward any remote host port to your local `localhost` via the thread's **Servers** list or **Forwarded ports** in the command palette (`Cmd/Ctrl-click` forwards and opens in your browser).
- **Open on this device (mobile)**: Tap **Open on this device** on a streamed `localhost` tab or long-press a `localhost` link in chat for native touch scrolling.
- **Disable preview tunnel**: Turn off **Settings → Sharing & access → Preview tunnel** on the host (or via [managed policy](./managed-policy.md#other-capabilities)) to disable port forwarding and **Open on this device** (streamed tabs continue working).
- **Take control**: Click **Take control** before typing into an agent's tab, and release control when done. Read-only connections can watch without changing the page.
- **Agent tabs**: They share sign-ins and one Chrome process per saved profile; incognito tabs share a process with separate cookie jars. See the [Browser guide](./browser.md) for profiles, logins and tab limits.
- **Local servers**: T3 Code lists listening ports from your terminals and agents as `http://localhost:<port>` (set a script's **Preview URL** for HTTPS or subpaths). Disable discovery with `T3CODE_PORT_DISCOVERY=0`.

### Browser host setup

The first server browser tab downloads a ~200 MB headless Chrome (shared with [HTML renders](./html-renders.md)). On Ubuntu 23.10+ or minimal Linux containers, run the one-time setup command on the host:

```sh
sudo t3 browser setup
```

This installs an AppArmor profile for Chrome's sandbox plus missing libraries and fonts. Never run the T3 server as root (Chrome's sandbox rejects root); if sandboxing is impossible on your host, set `T3CODE_SERVER_BROWSER_SANDBOX=0` in the server environment and restart.

## Link another environment

Link two T3 Code machines (beta) so agents on one can launch, message, delegate to, and manage threads on the other.

1. Enable **Settings → Labs → Linked environments** on **both** machines (or `"linkedEnvironmentsEnabled": true` in `userdata/settings.json`).
2. On the source machine, open **Settings → Environments → Linked environments → Link** (or drag from one machine's `+` to another on the Environments map, or run `t3 environment link <address> --pairing-code <code> --access <mode>`). Use **Link several…** or `--from <file>` for batch linking.
3. Links are one-way (tick **Also link … back** for two-way) and do not chain. Links expire after 30 days of inactivity and can be revoked in **Settings → Sharing & access** or **Forget**.

- **Access boundary**: Agents never get more permissions on the target than the link's access level or their own thread mode. Below Full access, linked agents can only modify threads they started and skip project setup scripts.
- **Delegation**: Agents can target a linked machine with `delegate_task`, or ask you to link an unlinked machine via an in-thread approval card.

### Read a linked machine's threads

Open a delegated task or thread link on a linked machine to follow it live (read-only) in the right panel without pairing your client directly. Click **Connect to interact** to pair and take action (one-click if the link has Full access and you can manage access). Files, images, and HTML renders stay on the remote machine until you connect directly.

## Continue delegated work on another environment

A new remote delegated task keeps its first result. Later messages to the same task's
thread continue that job, and each completed run becomes its latest result.
The parent can also see queued work, pending children, questions, and recovery.
Both environments must support delegated lifecycle observation before a new
remote task can start.

Open the remote task from the thread's agent relationships to inspect its work
through the parent environment. This works on web, desktop, and mobile without
saving a separate client connection to the destination. Refresh to retry a stale
observation or page through the messages. Older jobs keep their original direct
connection navigation and first-result observation.

**Stop remote work** records a request to stop the destination thread and its
nested work. Pending or unconfirmed means the destination has not confirmed the
request. Acknowledged means it accepted Stop; confirmed means its execution has
settled. Refused means the destination thread runs above the modes or grant this
side may stop, so it keeps running and its result still wakes the parent. A
cancel takes effect when the destination accepts it: until then the task keeps
its status, and a result that arrives first wakes the parent unless the cancel
is accepted before the parent reads it. The first result stays
available. A later Stop request applies to work started since the earlier Stop,
subject to the current link and permissions. If the link is removed or replaced,
or the destination thread is deleted, the task ends as lost: whether its work
finished there is unknown.

## Automations on another environment

Save a schedule, webhook or Spectrum while its contributors are reachable. The
receiving environment can then run that accepted work while the source sleeps,
within the saved permissions and their known expiry. Edits and Spectrum reopens
require fresh authorization. Both environments must support this behavior;
older peers refuse the save.

Disable the automation on its receiving environment to stop new execution
there immediately. Revoking or unlinking its source queues a disable that remains
unconfirmed until the receiver durably acknowledges it. If the receiver cannot
be reached, accepted local work can continue until that disable arrives or its
known authorization expires. Relinking does not revive the old automation.
Calling another environment still requires the original contributors online;
local progress and returning its result are separate operations. Webhooks keep
the receiving environment's URL.

## Share research across environments

Agents in Chromeria can share one environment's public-page cache and request
limits through `web_read` and discovery budgets through `web_search`. Link each source environment to the research
environment with agent access, then find its ID with `t3 environment list`.
In each source server's `userdata/settings.json`, set:

```json
{ "webResearch": { "host": "<linked-environment-id>" } }
```

Merge this entry into your existing settings. Leave it unset, or set `host` to
`null`, on the research environment itself. An unavailable or revoked link
returns an error; reads and searches do not fall back to the source machine. These server
settings apply to agents used from web, desktop and mobile.

Logged-in reading is off by default. With your explicit account authorization,
an agent can pass `useSession:true` and `session:{environmentId,profileId,tabId}`
to read one page through an existing Chromeria profile tab selected with
`preview_status`. The tab stays on its owning environment, even when your public
research uses another server. A remote owner requires a direct agent-access link
and `session.contextThreadId` for a live thread owned by that link with browser
access; unrelated threads and forwarding through a second environment are refused.
Human control, expired logins, account warnings and anti-bot checks stop the
read. The reader does not log in or perform account actions. It waits a short gap
after the profile's last action on that site, and adds no quotas.
Private results stay outside the public cache. Continue with the same session
selection and explicit opt-in within five minutes, or read the URL again.

Use `web_search(query, locale?)` for site/social and non-English community
queries, preserving `site:` and `-site:` filters and a locale such as `pl-PL`.
Keep the harness's own search for bulk discovery. Results include attributed
snippets and each engine's health; an unhealthy engine's empty output does not
mean no matches. Search results are not cached, and shared 25-second spacing
and provider budgets can delay or limit new queries.

The research server uses the existing SearXNG JSON service at
`https://bestia.napoleon-sun.ts.net:8888`. Set `webResearch.searxngUrl` on that
server to use another configured service. TinyFish search uses the same server
secret as its page-reading fallback and only accepts observed zero-cost calls.

For long pages, pass the returned `cacheId`, `cacheEnvironmentId` and
`nextOffset` (as `offset`) to continue. Changing the research host requires
reading the URL again. Outside MCP clients and links with read-only access
cannot spend the research environment's request budget.

When a maintainer provisions `web-research-treg-token` in the research
environment's server secret store, TinyFish can read public pages after an
eligible local render failure, through a pinned zero-cost endpoint. Its bounded
attempt must fit the active reader's remaining budget. A traversal keeps its
30-second limit and returns the local typed status when there is too little
time for hosted work. Root provider/budget errors stay explicit; a failed child
reports `providerError` while preserving the root and prior children.

Results identify TinyFish and the observed charge. Hosted pages have
`cacheStorage: "memory"`: continue them on the same research server before
`cacheExpiresAt`, at most 15 minutes. Buffers can be evicted sooner and are lost
on restart; an expired continuation asks you to read the URL again. Reading the
URL again repeats the local ladder rather than reusing a hosted buffer.
Provider retention permission, including this temporary buffer, remains
unresolved; persistent hosted caching stays disabled. Local pages keep their
persistent cache. Requested and final hosted URLs are checked here; TinyFish
owns networking inside its service, whose DNS and subresource isolation remain
unverified.

## Manage or revoke access

Manage pairing links and active client sessions in **Settings → Sharing & access** or via `t3 auth --help`.

- **Scoped tokens**: Pass `--scope <scope>` (repeatable) to `t3 pair`, `t3 auth pairing create`, or `t3 auth session issue` to restrict permissions (e.g. `orchestration:read`, `source-control:write`, `filesystem:read`, `filesystem:write`, `terminal:operate`, `relay:configure`). Re-pair with a fresh link to change an existing client's scopes.
- **Deregister from T3 Connect**: Open your account menu → **T3 Connect → Deregister** (or `t3 connect unlink` / `t3 connect logout` on CLI hosts). Idle tunnels are automatically cleaned up after 5–10 minutes offline and recreated on reconnect (hosts on older versions offline for 7+ days need an update first).
- Treat pairing URLs and codes as passwords—never paste them into logs or screenshots.

### Renew existing pairings

New direct pairings renew while the client is connected. Renewal preserves the
device's permissions and identity. A revoked or expired session cannot renew;
devices left offline past expiry need a fresh pairing.

Pairings created before renewal was available need one administrator approval
per device. This can be done over SSH without opening Settings or creating a
new pairing code. On the server, list requests with:

```sh
t3 auth renewal list
```

Match the client name, route, and last-seen time. Independently read the full
commitment on the intended desktop, using its actual state directory:

```sh
t3 auth renewal client-commitments --desktop-state-dir /path/to/desktop/userdata
```

For an existing Chromium, Chrome, Brave, or Edge browser pairing, run this on
the client machine with that browser's executable and profile directory:

```sh
t3 browser renewal-commitments --browser-executable /path/to/browser \
  --profile-dir /path/to/profile/Default --origin https://server.example
```

This headless command reads a private temporary copy of the origin's storage,
then removes it. Use the same browser engine version or newer. It does not
contact the server or restart the open browser. Legacy Firefox, Safari, and
mobile-browser pairings have no supported headless commitment read and retain
their original expiry; new pairings in these browsers enroll automatically.

Approve on the server with that device value:

```sh
t3 auth renewal approve <enrollment-id> --commitment <full-device-commitment>
```

Names and routes help select a request but do not prove which device submitted
it. Do not copy the commitment from the server's pending list for approval.
Use `t3 auth renewal reject <enrollment-id>` to reject a pending request, and
`t3 auth session revoke <session-id>` to revoke approved access. These commands
also accept `--base-dir` when the server uses a custom T3 home. Existing direct
mobile pairings retain their original lifetime; new mobile pairings renew.

## Desktop app as a remote only

Switch off **Settings → Environments → Local environment** in the desktop app to run with no local server, WSL backend, or incoming connections while continuing to drive remote hosts (if a local background service uses the same T3 home, **Open in** still works locally).

## Troubleshooting

See [T3 Connect troubleshooting](./t3-connect-troubleshooting.md) for step-by-step connection diagnostics, and [Outside agents](./outside-agents.md) for MCP connections.
