# Reuse browser logins

On macOS, Chromeria continuously mirrors Chrome's logins while the desktop app runs,
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
