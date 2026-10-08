import * as Schema from "effect/Schema";

import { CommandId, MessageId, NonNegativeInt, ThreadId } from "./baseSchemas.ts";

/** Server-authored transcript rows never start a provider run. */
export const SpectrumTranscriptAppend = Schema.Struct({
  type: Schema.Literal("spectrum.transcript.append"),
  commandId: CommandId,
  threadId: ThreadId,
  generation: NonNegativeInt,
  revision: NonNegativeInt,
  messageId: MessageId,
  role: Schema.Literals(["user", "assistant"]),
  text: Schema.String,
});
export type SpectrumTranscriptAppend = typeof SpectrumTranscriptAppend.Type;
