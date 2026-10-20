# Reuse browser logins

The [Browser guide](browser.md) explains native and streamed tabs, profiles,
and their separate login stores.

On macOS, the Chrome login source is off by default on each machine.
[Configure the Chrome key command](../fork.md#desktop-and-data-isolation) on the source
Mac to enable continuous mirroring. Removing the command stops Chrome observation.

On the enabled Mac, Chromeria continuously mirrors Chrome's logins while the desktop app runs,
including while Chrome is open. Chrome's Default profile uses Chromeria's default
profile; other Chrome profiles get separate profiles, initially named from Chrome.
After creation, your Chromeria profile names take precedence.
Guest, System and Incognito profiles are excluded.
You can add or remove Chromeria profiles normally. Removing a discovered Chrome
profile keeps it removed, including after restarting Chromeria.

Changes reach each connected environment's browser through its matching desktop
partition. Allow roughly a minute for Chrome to save a change and Chromeria to
observe it. Logins made inside one environment's desktop partition stay with that
environment. Cookies created only downstream survive unless the mirror has managed
that same cookie identity.
The paired desktop connection needs **Mirror desktop browser logins** permission
in that environment's Connections settings. Existing connections may need pairing
again with that permission.

When the Mac app is off, environments retain the cookies they last received.
Persistent-cookie removals catch up after reconnect or restart. Session-cookie
sign-outs propagate when Chrome stays open between observations. After Chrome or
Chromeria restarts, vanished session cookies are retained downstream because Chrome
can clear them during startup. An expired or replaced cookie can still end a
session according to the site's own rules.

Some logins cannot move between devices. Google account sign-in on current macOS
Chrome and other observed device-bound sites appear in **Settings > Integrations >
Browser** as **device-bound, not mirrored**. Sign in separately in each environment
for those sites. Cookie convergence alone does not prove a working login.

## One-time imports

The desktop app can import cookies from another browser so you can reuse its signed-in sessions
in the preview browser.

Open **Settings → Browser → Browser profiles → Add profile**, then choose a browser under
**Import from**. Close the source browser before importing, and allow an operating-system keyring
unlock prompt if one appears.

These imports are one-time copies. Continuous mirroring supports only Chrome on macOS.

On macOS, Safari imports need Full Disk Access. Choose **Allow**, drag T3 Code into the
System Settings permission list, and turn access on. **Continue** becomes available when access
is detected. macOS may require you to quit and reopen T3 Code before the grant applies; reopen
the import wizard afterward. You can revoke Full Disk Access once the import is done.

On Windows, import supports Firefox, Zen, and Helium profiles that use standard profile encryption.
Other Chromium-based browsers use app-bound encryption and cannot be imported. Partitioned cookies
are skipped by the one-time importer on all platforms. The continuous Chrome mirror
preserves partitioned cookies.

## Agent sign-in from an rbw vault

Agents can fill a login form in the environment's browser from your
[rbw](https://github.com/doy/rbw) vault with `preview_fill_credential`. The agent names a vault
item and a field (`username`, `password`, or `totp`), and the environment reads the value and
types it into the input. The agent never receives the value.

To turn it on, edit the server's `settings.json` in its `userdata` directory. Set `rbwPath` to an
rbw executable on that machine, or to a wrapper that runs rbw on the machine that holds the vault:

```json
{
  "credentialVault": { "kind": "local", "rbwPath": "/opt/homebrew/bin/rbw" }
}
```

The vault must be unlocked; Chromeria never prompts for your master password. A wrapper that
exits with status 69 is reported as an unreachable vault.

A fill happens only on an https page whose host is one of the item's URIs or a subdomain of
one. For example, `shopify.com` covers `admin.shopify.com`, but not `evilshopify.com`. Inside an
iframe, the iframe's own origin must match.

Fills run only in the server's own browser tabs, not in tabs the desktop app hosts.

A fill starts a login for the whole browser profile the agent's tab uses, even if the fill then
fails. The login uses that profile's sign-ins and cookies. Until it ends, no tab in the profile can
be read: no snapshots, screenshots, page text, scripts, recordings, console or network entries,
downloads, URLs or titles. That includes other agents' tabs in the same profile, which are paused,
and tabs opened meanwhile. Keep logins short. Chromeria shows these tabs as **Login in progress**.

During a login the agent can still fill, click and type by locator, press keys and wait for an
element, URL or text. It ends the login with `preview_end_login`, which closes the profile's
browser, with every tab in it, and waits until it has closed. The next tab opens the profile
again, signed in through its saved cookies. Cookies a site sets without an expiry, which
browsers keep only until they close, do not survive this, so such a site needs a new sign-in. A
login in an incognito profile loses its session when it ends.

You still see the page in the live view.

Each attempt appends a line to `credential-fills.jsonl` in the `userdata` directory. The line
records the time, environment, thread, item, field, the vault site that matched and the outcome,
but never the value or anything from the page.

This keeps values out of agent transcripts and logs, and stops fills on the wrong site. It does
not stop an agent with shell access from running `rbw` itself, or the agent using the signed-in
session afterwards.
