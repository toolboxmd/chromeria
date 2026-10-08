import * as Schema from "effect/Schema";
import { IsoDateTime } from "./baseSchemas.ts";

/** A new enabledAt identifies an explicit activation; the wall-clock timer includes pauses. */
export const WightMode = Schema.Struct({
  enabledAt: IsoDateTime,
  expiresAt: Schema.NullOr(
    Schema.Number.check(Schema.isFinite(), Schema.isGreaterThanOrEqualTo(0)),
  ),
});
export type WightMode = typeof WightMode.Type;
export const DEFAULT_WIGHT_LIMIT_PERCENT = 80;
export const WightLimitPercent = Schema.Number.check(
  Schema.isFinite(),
  Schema.isInt(),
  Schema.isBetween({ minimum: 0, maximum: 100 }),
);
