import type { Session, WebContents } from "electron";

import {
  chromeIdentity,
  usesChromeIdentity,
  type ChromeIdentity,
  type ChromeIdentityHost,
} from "./ChromeUserAgent.ts";

export const installChromeIdentity = (
  session: {
    readonly getUserAgent: Session["getUserAgent"];
    readonly webRequest: {
      readonly onBeforeRequest: (
        ...args: Parameters<Session["webRequest"]["onBeforeRequest"]>
      ) => void;
    };
  },
  host: ChromeIdentityHost,
  configure: (contents: WebContents, identity: ChromeIdentity | null) => Promise<void>,
) => {
  const nativeUserAgent = session.getUserAgent();
  const observed = new WeakMap<
    WebContents,
    {
      readonly start: (url: string) => void;
      readonly restore: () => Promise<void>;
    }
  >();
  session.webRequest.onBeforeRequest((details, callback) => {
    const contents = details.webContents;
    if (details.resourceType !== "mainFrame" || !contents) {
      callback({});
      return;
    }
    let controller = observed.get(contents);
    if (!controller) {
      let committedUrl = contents.getURL();
      let pendingUrl: string | undefined;
      let disposed = false;
      // A fresh WebContents starts on Electron's native identity, so non-listed pages need no command.
      let appliedListed: boolean | undefined = false;
      let debuggerGeneration = 0;
      let pending = Promise.resolve();
      const apply = (force = false) => {
        // Read the destination when the command runs, so a superseded navigation queued behind a
        // held command never applies its identity to the page that replaced it.
        const command = async () => {
          const listed = usesChromeIdentity(pendingUrl ?? committedUrl);
          if (disposed || contents.isDestroyed() || (!force && appliedListed === listed)) return;
          // A failed transport call may still have changed Chromium, so invalidate before sending.
          appliedListed = undefined;
          const generation = debuggerGeneration;
          await configure(contents, listed ? chromeIdentity(nativeUserAgent, host) : null);
          if (generation === debuggerGeneration) appliedListed = listed;
        };
        // The Manager reports failures; a later abort or debugger restore must still run.
        pending = pending.then(command, command);
        void pending.catch(() => {});
        return pending;
      };
      const committed = (
        _event: Electron.Event,
        url: string,
        _code: number,
        _status: string,
        isMainFrame: boolean,
      ) => {
        if (!isMainFrame) return;
        // Store the document even if DevTools currently prevents its emulation command.
        committedUrl = url;
        if (pendingUrl === url) pendingUrl = undefined;
        return apply();
      };
      const stopped = () => {
        if (pendingUrl === undefined) return;
        pendingUrl = undefined;
        return apply();
      };
      const failed = (
        _event: Electron.Event,
        _code: number,
        _description: string,
        url: string,
        isMainFrame: boolean,
      ) => {
        if (isMainFrame && (!url || url === pendingUrl)) return stopped();
      };
      const invalidated = () => {
        debuggerGeneration++;
        appliedListed = undefined;
      };
      const destroyed = () => {
        disposed = true;
        contents.off("did-frame-navigate", committed);
        contents.off("did-fail-load", failed);
        contents.off("did-fail-provisional-load", failed);
        contents.off("did-stop-loading", stopped);
        contents.off("devtools-opened", invalidated);
        contents.off("render-process-gone", invalidated);
        contents.off("destroyed", destroyed);
        observed.delete(contents);
      };
      contents.on("did-frame-navigate", committed);
      contents.on("did-fail-load", failed);
      contents.on("did-fail-provisional-load", failed);
      contents.on("did-stop-loading", stopped);
      contents.on("devtools-opened", invalidated);
      contents.on("render-process-gone", invalidated);
      contents.on("destroyed", destroyed);
      controller = {
        start: (url) => {
          pendingUrl = url;
          void apply();
        },
        restore: () => apply(true),
      };
      observed.set(contents, controller);
    }
    // Never hold the request on the command: Chromium suspends renderer-bound CDP messages during
    // navigation, and Emulation.setUserAgentOverride reaches the renderer, so its reply cannot arrive.
    // This request's headers were fixed already; later requests and the document get the identity.
    callback({});
    controller.start(details.url);
  });
  return (contents: WebContents) => observed.get(contents)?.restore() ?? Promise.resolve();
};
