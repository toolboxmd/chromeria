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
controlling them, a page watch is active, or a login is in progress in their
profile. Tabs you opened yourself stay open. Agent tabs also close
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
login quarantine and revoked authority end it. Stop an exact watch with
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
