# Reuse browser logins

Reuse your signed-in sessions in the desktop app's [preview browser](./browser.md), either by mirroring Chrome continuously on macOS or by importing another browser's cookies once. The [Browser guide](./browser.md) explains native and streamed tabs, profiles, and their separate login stores.

## Mirror Chrome logins (macOS)

The Chrome login source is off by default on each machine. To enable continuous mirroring, quit Chromeria and [configure the Chrome key command](../fork.md#desktop-and-data-isolation) on the source Mac. Removing the command stops Chrome observation.

- **What is mirrored**: While the desktop app runs, Chromeria mirrors Chrome's logins continuously, including while Chrome is open. Chrome's Default profile uses Chromeria's default profile; other Chrome profiles get separate profiles, initially named from Chrome. After creation, your Chromeria profile names take precedence. Guest, System and Incognito profiles are excluded.
- **Profiles**: Add or remove Chromeria profiles normally. A discovered Chrome profile you remove stays removed, including after restarting Chromeria.
- **Timing**: Changes reach each connected environment's browser through its matching desktop partition. Allow roughly a minute for Chrome to save a change and Chromeria to observe it.
- **Permission**: The paired desktop connection needs **Mirror desktop browser logins** in that environment's Connections settings. Existing connections may need pairing again with that permission.
- **Local logins**: Logins made inside one environment's desktop partition stay with that environment. A login made inside an agent tab keeps its cookies until the desktop sends a new value for one of them, for example when you sign in to that site on the desktop. Signing out on the desktop does not remove them. A restart of the environment's server can still replace them once with the desktop's values.
- **When the Mac app is off**: Environments keep the cookies they last received. Persistent-cookie removals catch up after reconnect or restart, except for cookies an agent tab has changed. Session-cookie sign-outs propagate when Chrome stays open between observations. After Chrome or Chromeria restarts, vanished session cookies are kept downstream because Chrome can clear them during startup. An expired or replaced cookie can still end a session according to the site's own rules.
- **Device-bound logins**: Some logins cannot move between devices. Google account sign-in on current macOS Chrome and other observed device-bound sites appear in **Settings → Browser** as **device-bound, not mirrored**. Sign in separately in each environment for those sites. Cookie convergence alone does not prove a working login.
- **Partitioned cookies**: The continuous Chrome mirror keeps them.

Anti-bot verdict cookies never move: `datadome`, `cf_clearance`, `__cf_bm`,
`_abck`, `bm_sz`, `reese84`, and names starting with `_px` or `incap_ses_`. Sites
issue them to the browser that passed their check, so a copy on another device
can look like a mismatch and trigger a block. Mirroring, imports and copies skip
them. Chromeria removes each one it copied before, once, even when the site has
since refreshed it in the agent browser, because a refreshed verdict still
carries the copied device's identity. The next visit to that site may then show
a check. Your Chrome keeps its own, and after that removal each agent browser
keeps the ones it earns itself.

## Agents on other machines

An agent on another linked environment can copy your session for a site from the environment that holds your logins, such as the one your desktop mirrors Chrome into. It copies one snapshot of that site's cookies into a browser profile on its own machine, then opens the site with that profile. If the site is still logged out, the agent signs in from your [rbw vault](#agent-sign-in-from-an-rbw-vault) instead.

To allow it, open **Settings > Connections** on the environment that holds your logins, turn on **Share browser logins** for the linked environment whose agents may copy them, then save the link permissions. It is off by default and separate from **Browser access**: letting an environment drive this browser does not let it copy your sessions. Turning it off stops copies that have not started yet.

- The agent names the site's hosts, at most 8, such as `admin.shopify.com`. It receives cookies the browser would send to those hosts and nothing else. A copy fails as a whole if those hosts hold more than 256 cookies or 64 KiB.
- The receiving profile keeps cookies on a parent domain, such as `.shopify.com`, that the copy lacks, because they can belong to other sites.
- Google sign-in is bound to each machine. Sign in on each machine separately.
- Partitioned cookies belong to another site's embeds and stay behind.
- Work that another environment started cannot copy sessions. Its parent runs a computer-use agent on the machine with your logins instead.
- Incognito profiles and profile ids the receiving environment does not know are refused.

Each copy appends a line to `login-imports.jsonl` in the `userdata` directory of both environments. The line records the time, environments, thread, profiles, hosts, counts and outcome, never a cookie name or value. The agent sees only counts.

## Import a profile once

1. Close the source browser.
2. Open **Settings → Browser → Browser profiles → Add profile** and choose a browser under **Import from**.
3. Allow any OS keyring unlock prompt.

This is a one-time copy: later login changes stay separate, and some sites may still require signing in again. Continuous mirroring supports only Chrome on macOS.

### Platform notes

- **macOS (Safari)**: Requires temporary **Full Disk Access** (grant in System Settings, quit/reopen T3 Code if macOS requires it, then reopen the import wizard). You can revoke Full Disk Access afterward.
- **Windows**: Supports Firefox, Zen, and Helium profiles using standard encryption. Other Chromium browsers use app-bound encryption and cannot be imported.
- The one-time importer skips partitioned cookies on all platforms. Administrators can disable cookie import via [managed policy](./managed-policy.md#other-capabilities).

## Agent sign-in from an rbw vault

Agents can fill a login form in the environment's browser from your [rbw](https://github.com/doy/rbw) vault with `preview_fill_credential`. The agent names a vault item and a field (`username`, `password`, or `totp`), and the environment reads the value and types it into the input. The agent never receives the value.

To turn it on, edit the server's `settings.json` in its `userdata` directory. Set `rbwPath` to an rbw executable on that machine, or to a wrapper that runs rbw on the machine that holds the vault:

```json
{
  "credentialVault": { "kind": "local", "rbwPath": "/opt/homebrew/bin/rbw" }
}
```

The vault must be unlocked; Chromeria never prompts for your master password. A wrapper that exits with status 69 is reported as an unreachable vault.

- **Matching sites**: A fill happens only on an https page in the same registrable domain as one of the item's URIs, as Bitwarden matches by default. For example, `www.shopify.com` covers `admin.shopify.com`, but not `evilshopify.com`. Public suffixes such as `github.io` or `co.uk` never widen a match: `a.github.io` does not cover `b.github.io`. IP addresses and single-word hosts such as `localhost` match only exactly. Inside an iframe, the iframe's own origin must match.
- **Tabs**: Fills run only in the server's own browser tabs, not in tabs the desktop app hosts.
- **Linked environments**: Agents started from another linked environment, including runs of schedules it created here, can fill from this vault while that link has **Browser access** and **Use credential vault** on in **Settings > Connections**. **Use credential vault** turns on and off with **Browser access**, including for links made before it existed. Turn it off on its own to let a link's agents drive this browser without your logins. Turning it off stops later fills.
- **Finishing the sign-in**: The agent keeps reading the page while it signs in, so it sees what the site asks after the password, such as a passkey error, a "Try another way" link or a one-time code step, and finishes the sign-in in the same tab. Password fields stay masked in its snapshots and screenshots, and the fill's result never contains the value.
- **Audit**: Each fill that passes the permission checks appends a line to `credential-fills.jsonl` in the `userdata` directory. The line records the time, environment, thread, the links a linked agent acted for, item, field, the vault site that matched and the outcome, but never the value or anything from the page.

This keeps values out of the fill's results and audit line, and stops fills on the wrong site. It does not hide a value the page itself shows, such as a username or one-time code in a visible field, or a value the site copies into its URL or title. It also does not stop an agent from reading a field with a page script (`preview_evaluate`), running `rbw` itself when it has shell access, or using the signed-in session afterwards.
