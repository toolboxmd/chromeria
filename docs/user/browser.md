# Browser

Open **Browser** from the thread's panel menu, or click a running dev server in
the thread's **Servers** list, then choose **Default**, a named profile, or
**Incognito**. Each tab belongs to the thread where it was opened.

Press `mod+f` inside a desktop browser tab to find on the page, or use the tab's
**⋮** menu if the page intercepts `mod+f`. On macOS you can also set the desktop
app as your [default web browser](./default-browser.md).

## Two browsers in one panel

In the desktop app's own environment, tabs you open use the app's built-in
browser. They display natively and use the panel's own logins. Tabs an agent
opens use the environment's agent browser and appear in the panel as a live
stream. The agent browser runs pinned Chrome for Testing 158 in new headless
mode with hardware GPU rendering.

With System appearance, streamed tabs use dark mode from 8 PM to 6 AM in the
host's local time. The tab checks the time when it opens or applies an
appearance change.

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

- On macOS, continuous Chrome mirroring is off by default. Quit Chromeria,
  then set `chromeCookieKeyCommand` on the source Mac to enable it. Chrome's `Default`
  profile folder maps to Chromeria's `default` profile.
- Sending mirrored logins to another environment requires **Mirror desktop
  browser logins** permission on that connection.
- A login you make inside an agent tab keeps its cookies until the desktop sends
  a new value for them; signing out on the desktop does not remove them. Anti-bot
  cookies the mirror copied earlier are the exception: they are removed once.
- For a one-time copy, open **Settings → Browser → Browser profiles →
  Add profile** and choose **Import from**.
- An agent on another linked environment can copy one site's session from an
  environment whose **Share browser logins** permission for it is on, with
  `preview_import_login`. The permission is separate from **Browser access**.

See [Reuse browser logins](browser-import.md) for setup and limits, including
logins that cannot move between devices and anti-bot cookies that never move. The agent's rbw login tool,
`preview_fill_credential`, works only in agent-browser tabs. It refuses native
desktop tabs. It fills only on https pages in the same registrable domain as one
of the vault item's URIs, so `www.example.com` covers `portal.example.com`.
An agent on another linked environment can use it only while that link's
**Browser access** and **Use credential vault** permissions are both on.

## Defaults and settings

In **Settings → Browser**, set the **Default browser profile**, **Default browser
viewport** (fill, freeform, or phone and tablet presets with portrait or landscape
rotation), **Default browser zoom** and **Default browser appearance** (system,
light or dark). **Open links in** and **Search engine** are set there too.

**Agent browser access** turns the agents' `preview_*` tools on or off per
environment or project; a change applies when the next agent session starts. By
default, tabs an agent opens float over the chat; turn off **Auto-show floating
preview** to open them in the right panel instead.

## Pick elements, annotate and record

Use the element picker or annotate a region of the page to attach it to the
composer as an inline context chip. On desktop, a tab you open for a remote
environment loads locally over the
[preview tunnel](./remote-access.md#browser-on-a-remote-environment), so the picker
and annotations work there too.

Record browser interactions from the tab toolbar. **Settings → Browser** sets
**Browser recording frame rate**, **Show key presses in recordings** and **Show
mouse presses in recordings**.

## Values an agent sees on a page

When an agent shows a value such as an API key in an agent-browser tab, it can
save it with `preview_save_secret` instead of asking you to copy it. The value
goes to a private file on the environment's machine and never into the
conversation; the agent gets the file's path and reads it inside commands. The
file is deleted after 24 hours.

The tool reads only https pages in agent-browser tabs, and refuses calls from an
agent on another linked environment. It keeps the value out of its own answer
only: a snapshot or recording of the page afterwards still shows it.

## Work together

On an agent tab, choose **Take control** before interacting with the page.
Clicking inside the page does not take control. The agent pauses its actions
while you control it. Choose **Release control** to let the agent continue.
If either step fails, for example because the link lost browser access, an
error says so and control stays where it was.
Losing the connection, closing the panel or switching threads does not return
control to the agent. You keep control, and another open panel on that tab, or
the next one you open, shows **Release control**. If the browser rejects an input backlog, it ends that
viewer's control. Wait for pending input to finish, then reconnect and take
control again.

You can also open a tab yourself and tell the agent to “look at this”. Agents
can read your tab, but cannot use the rbw login tool there when it is a native
desktop tab.

## Anti-bot checks

When a site answers an agent with an anti-bot check or block (DataDome,
Cloudflare, Akamai, HUMAN or Imperva), agents stop acting on that site in that
browser profile, and the tab opens for you with a banner. Agents never solve
checks. To pass one, choose **Take control**, complete the check, choose
**Release control**, and tell the agent to continue. Releasing on a page that
still shows the check keeps the hold, and so does releasing while another tab
of that profile meets the check again: reload the page you passed the check
on, then release again.

Choose **Done** on the banner when you passed the check without taking
control, or **Clear hold** after a block when the site works again; either one
ends every hold of that tab.
Until the hold ends, agents cannot open or act on any page of the site in that
profile, including from a new incognito tab. A link on another site that leads
into the held site, or a request already on its way, can still reach it once. The tab stays open for you: agents
cannot close it and a settling thread leaves it. Closing it yourself or
restarting the server also ends the hold; the next check starts a new one.

## Tab lifetime and limits

When a thread settles, its agent tabs close unless you are watching or
controlling them, or a page watch is active. Tabs you opened yourself stay open. Agent tabs also close
after 30 minutes unused unless someone is watching them or a page watch is active.
The agent browser allows up to 8 tabs per agent session and 32 per environment;
agents can close or reuse tabs to make room.

For streaming controls and Linux setup, see
[Browser on a remote environment](remote-access.md#browser-on-a-remote-environment).

## Subscribe to page changes

An authorized agent can call `preview_watch_start` with an existing owned server
`tabId` and a JavaScript `subscription` function `(emit) => cleanup`. The function
runs once in the main page and returns a synchronous cleanup function. It can
listen to page events, observe elements or subscribe to an accessible application
store. Chromeria does not poll, refresh the page or start a model. The caller
emits its own initial baseline. For example, pass this string as `subscription`:

```js
const subscription = `(emit) => {
  const listener = () => emit({ selected: window.appStore.selected });
  const unsubscribe = window.appStore.subscribe(listener);
  listener();
  return unsubscribe;
}`;
```

Save the returned `watchId` and `epoch`. The start `cursor` is always zero, before
any synchronous baseline. Call `preview_watch_read` with the host `threadId`,
`watchId`, `epoch`, and `after: 0` to read retained events. An optional `timeoutMs`
waits for the queue, at most 30 seconds; zero returns immediately. This read never
inspects the page. Repeating `after` replays the same events without consuming or
acknowledging them. Commit your processed cursor with your own database work;
retry the same cursor after a failed transaction. A reply's cursor names its last
returned event, and an empty active reply keeps your supplied cursor.

A page watch survives turn completion and thread settlement. Explicit close,
navigation (including route changes), human takeover, thread archive/deletion,
and revoked authority end it. Stop an exact watch with
`preview_watch_stop`. The event queue lives in the server process: restart,
unknown watches, invalid cursors, terminal state and overflow produce explicit
errors. Reconcile your application's state before registering again; a new
baseline cannot recover every transient change from a lost stream.

JSON is preserved without truncation. Each value may be at most 1 MiB; each watch
retains at most 256 events or 8 MiB. The server allows 128 active watches and
64 MiB of retained and pending payloads, with 256 bounded watch records including
terminal records. Reads return up to 64 events or 2 MiB, leaving later events for
the next cursor. Non-JSON values or exhausted retention end the watch visibly.
Stop watches you no longer need so their tabs can close normally.

An outside OAuth client with existing `read` permission can read an exact named
watch only in an undeleted thread of its approved project. It cannot start or stop
watches, inspect a page, navigate or sign in. Provision the subscription through
an authorized thread; this capability does not change existing grants.
