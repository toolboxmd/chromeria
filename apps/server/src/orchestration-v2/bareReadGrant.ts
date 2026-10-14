import * as Schema from "effect/Schema";

export const BareReadGrant = Schema.Union([
  Schema.Struct({ type: Schema.Literal("all") }),
  Schema.Struct({ type: Schema.Literal("none") }),
  Schema.Struct({ type: Schema.Literal("roots"), roots: Schema.Array(Schema.String) }),
]);
export type BareReadGrant = typeof BareReadGrant.Type;
