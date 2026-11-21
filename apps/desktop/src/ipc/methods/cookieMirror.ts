import { CookieMirrorConnectionInput, CookieMirrorConnectionStatus } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as DesktopCookieMirror from "../../preview/DesktopCookieMirror.ts";
import * as DesktopIpc from "../DesktopIpc.ts";
import * as IpcChannels from "../channels.ts";

export const cookieMirrorConnection = DesktopIpc.makeIpcMethod({
  channel: IpcChannels.COOKIE_MIRROR_CONNECTION_CHANNEL,
  payload: CookieMirrorConnectionInput,
  result: CookieMirrorConnectionStatus,
  handler: (input) =>
    Effect.flatMap(DesktopCookieMirror.DesktopCookieMirror, (mirror) => mirror.connection(input)),
});
