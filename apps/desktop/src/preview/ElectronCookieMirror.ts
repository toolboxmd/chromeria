import { WebContentsView, type Session } from "electron";
import { cdpCookieMirrorStore, type CookieMirrorStore } from "@t3tools/shared/browserCookieMirror";

/** An unexposed blank target avoids taking a preview tab's debugger connection. */
export function electronCookieMirrorStore(
  session: Session,
): CookieMirrorStore & { readonly close: () => void } {
  const target = new WebContentsView({
    webPreferences: { session, sandbox: true, contextIsolation: true },
  });
  target.webContents.debugger.attach("1.3");
  // Storage.* selects Electron's default session even on a partition's target.
  // Network.* binds to this target and carries the same full CDP partition key.
  const store = cdpCookieMirrorStore(
    { send: target.webContents.debugger.sendCommand.bind(target.webContents.debugger) },
    "target",
  );
  return { ...store, close: () => target.webContents.close({ waitForBeforeUnload: false }) };
}
