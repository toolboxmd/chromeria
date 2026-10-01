import { PrismLane, PrismRole, TrimmedNonEmptyString } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const SpectrumColor = Schema.Struct({
  label: TrimmedNonEmptyString,
  role: Schema.optional(PrismRole),
  lane: Schema.optional(PrismLane),
  instanceId: Schema.optional(TrimmedNonEmptyString),
  model: Schema.optional(TrimmedNonEmptyString),
  effort: Schema.optional(TrimmedNonEmptyString),
});
export const StartSpectrumInput = Schema.Struct({
  question: TrimmedNonEmptyString,
  colors: Schema.Array(SpectrumColor).check(Schema.isMinLength(2), Schema.isMaxLength(8)),
  mode: Schema.Literals(["council", "free"]),
  limit: Schema.optional(
    Schema.Number.check(Schema.isInt(), Schema.isBetween({ minimum: 1, maximum: 100 })),
  ),
  moderator: Schema.optional(Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))),
  title: Schema.optional(TrimmedNonEmptyString),
});
export type StartSpectrumInput = typeof StartSpectrumInput.Type;
export const StartSpectrumResult = Schema.Struct({
  threadId: Schema.String,
  callerThreadId: Schema.String,
  mode: Schema.Literals(["council", "free"]),
  participants: Schema.Array(
    Schema.Struct({
      threadId: Schema.String,
      label: Schema.String,
      instanceId: Schema.String,
      model: Schema.String,
    }),
  ),
});
