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
  const observed = new WeakMap<WebContents, (url: string) => void>();
  session.webRequest.onBeforeRequest((details, callback) => {
    const contents = details.webContents;
    if (details.resourceType === "mainFrame" && contents) {
      let started = observed.get(contents);
      if (!started) {
        let pendingListed = false;
        let disposed = false;
        let pending = Promise.resolve();
        const apply = (identity: ChromeIdentity | null) => {
          const command = async () => {
            if (disposed || contents.isDestroyed()) return;
            await configure(contents, identity);
          };
          // A failed command is reported by the Manager; a later clear must still run.
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
          pendingListed = false;
          return apply(usesChromeIdentity(url) ? chromeIdentity(nativeUserAgent, host) : null);
        };
        const stopped = () => {
          if (!pendingListed) return;
          pendingListed = false;
          return apply(null);
        };
        const failed = (
          _event: Electron.Event,
          _code: number,
          _description: string,
          _url: string,
          isMainFrame: boolean,
        ) => {
          if (isMainFrame) return stopped();
        };
        const destroyed = () => {
          disposed = true;
          contents.off("did-frame-navigate", committed);
          contents.off("did-fail-load", failed);
          contents.off("did-fail-provisional-load", failed);
          contents.off("did-stop-loading", stopped);
          contents.off("destroyed", destroyed);
          observed.delete(contents);
        };
        contents.on("did-frame-navigate", committed);
        contents.on("did-fail-load", failed);
        contents.on("did-fail-provisional-load", failed);
        contents.on("did-stop-loading", stopped);
        contents.on("destroyed", destroyed);
        started = (url) => {
          pendingListed = usesChromeIdentity(url);
        };
        observed.set(contents, started);
      }
      started(details.url);
    }
    // Observe the navigation only. Chromium alone pairs the main frame's JS and headers.
    callback({});
  });
};
