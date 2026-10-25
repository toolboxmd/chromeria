# Browser

Open **Browser** from the thread's panel menu, then choose **Default**, a named
profile, or **Incognito**. Each tab belongs to the thread where it was opened.

## Two browsers in one panel

In the desktop app's own environment, tabs you open use the app's built-in
browser. They display natively and use the panel's own logins. Tabs an agent
opens use the environment's agent browser and appear in the panel as a live
stream. The agent browser runs pinned Chrome for Testing 154 in new headless
mode with hardware GPU rendering.

In a thread on another environment, such as a server, tabs stream from that
environment by default. A web or mobile client also streams tabs. In a streamed
tab, `localhost` reaches the environment's machine. On desktop, **Open on this
computer** moves a remote tab to your computer's native browser; `localhost`
then reaches your computer.

For a page you want to use yourself, open it from the Browser menu on your local
desktop. This gives you a native tab instead of an agent stream.

Environment-hosted tabs cannot use passkeys or security keys. These requests fail
promptly so sites can offer another sign-in method. Choose another method when
offered, or use your local desktop's native tab for passkey sign-in.

## Profiles and logins

A profile keeps a set of cookies, sign-ins and site storage. **Default**
(`default`) is the built-in saved profile. Named profiles let you keep other
accounts separate. **Incognito** keeps temporary storage rather than saving
it for later.

In the agent browser, each saved profile has one Chrome process shared by all
threads' tabs in that profile. Those tabs share sign-ins. Incognito tabs share
one Chrome process but have separate cookie jars.

The native panel and agent browser have separate login stores, even for the
same profile name. Mirroring can copy supported cookies from the desktop panel
to the environment's agent browser. To reuse logins from another browser:

- On macOS, continuous Chrome mirroring is off by default. Set
  `chromeCookieKeyCommand` on the source Mac to enable it. Chrome's `Default`
  profile folder maps to Chromeria's `default` profile.
- Sending mirrored logins to another environment requires **Mirror desktop
  browser logins** permission on that connection.
- For a one-time copy, open **Settings → Integrations → Browser profiles →
  Add profile** and choose **Import from**.

See [Reuse browser logins](browser-import.md) for setup and limits, including
logins that cannot move between devices. The agent's rbw login tool,
`preview_fill_credential`, works only in agent-browser tabs. It refuses native
desktop tabs.

## Work together

On an agent tab, choose **Take control** before interacting with the page.
The agent pauses its actions while you control it. Choose **Release control**
to let the agent continue.

You can also open a tab yourself and tell the agent to “look at this”. Agents
can read your tab, but cannot use the rbw login tool there when it is a native
desktop tab.

## Tab lifetime and limits

When a thread settles, its agent tabs close unless you are watching or
controlling them, or a login is in progress in their profile. Tabs you opened
yourself stay open. Agent tabs also close after 30 minutes unused unless someone
is watching them. The agent browser allows up to 8 tabs per agent session and 32 per environment;
agents can close or reuse tabs to make room.

For streaming controls and Linux setup, see
[Browser on a remote environment](remote-access.md#browser-on-a-remote-environment).
