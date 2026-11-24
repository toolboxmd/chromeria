import { ThreadId, WightMode, type ServerSettings } from "@t3tools/contracts";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

const decodeLegacy = Schema.decodeUnknownOption(Schema.Record(ThreadId, WightMode));

/** Moves Wight activations saved before thread idle modes into them; a newer entry wins. */
export function foldLegacyWightModes(settings: ServerSettings): ServerSettings {
  if (settings.wightModes === undefined) return settings;
  const { wightModes, ...rest } = settings;
  const legacy = Option.getOrElse(decodeLegacy(wightModes), () => ({}));
  const threadIdleModes = { ...settings.threadIdleModes };
  for (const [threadId, mode] of Object.entries(legacy)) {
    threadIdleModes[ThreadId.make(threadId)] ??= { type: "wight", ...mode };
  }
  return { ...rest, threadIdleModes };
}
