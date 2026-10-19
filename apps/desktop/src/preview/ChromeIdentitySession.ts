import type { Session, WebContents } from "electron";

import {
  chromeIdentity,
  chromeRequestHeaders,
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
      readonly onBeforeSendHeaders: (
        ...args: Parameters<Session["webRequest"]["onBeforeSendHeaders"]>
      ) => void;
    };
  },
  host: ChromeIdentityHost,
  configure: (contents: WebContents, identity: ChromeIdentity | null) => Promise<void>,
) => {
  const nativeUserAgent = session.getUserAgent();
  const overridden = new WeakSet<WebContents>();
  const pending = new WeakMap<WebContents, Promise<void>>();
  session.webRequest.onBeforeRequest((details, callback) => {
    const contents = details.webContents;
    const listed = usesChromeIdentity(details.url);
    if (
      details.resourceType !== "mainFrame" ||
      !contents ||
      (!listed && !overridden.has(contents) && !pending.has(contents))
    ) {
      callback({});
      return;
    }
    // A redirect or a second navigation cannot overtake an in-flight override.
    const update = (pending.get(contents) ?? Promise.resolve())
      .catch(() => {})
      .then(async () => {
        if (listed) {
          // Even a rejected transport call may have reached Chromium; navigation away must reset it.
          overridden.add(contents);
          await configure(contents, chromeIdentity(nativeUserAgent, host));
        } else if (overridden.has(contents)) {
          await configure(contents, null);
          overridden.delete(contents);
        }
      });
    pending.set(contents, update);
    const finish = (cancel: boolean) => {
      if (pending.get(contents) === update) pending.delete(contents);
      callback({ cancel });
    };
    // Failed CDP setup must not send a request with a half-configured identity.
    void update.then(
      () => finish(false),
      () => finish(true),
    );
  });
  session.webRequest.onBeforeSendHeaders((details, callback) => {
    if (usesChromeIdentity(details.url)) {
      try {
        callback({
          requestHeaders: chromeRequestHeaders(
            details.requestHeaders,
            chromeIdentity(nativeUserAgent, host),
          ),
        });
      } catch {
        callback({ cancel: true });
      }
      return;
    }
    if (!details.webContents || !overridden.has(details.webContents)) {
      callback({ requestHeaders: details.requestHeaders });
      return;
    }
    // Third-party requests from a listed tab must not inherit its Chrome product.
    const requestHeaders: Record<string, string> = {};
    for (const [name, value] of Object.entries(details.requestHeaders)) {
      const key = name.toLowerCase();
      requestHeaders[name] =
        key === "user-agent"
          ? nativeUserAgent
          : key === "sec-ch-ua" || key === "sec-ch-ua-full-version-list"
            ? value
                .split(",")
                .filter((brand) => !brand.includes('"Google Chrome"'))
                .join(",")
            : value;
    }
    callback({ requestHeaders });
  });
};
