import * as Schema from "effect/Schema";

// Preserve deferred feature data through settings decode/encode, without
// importing its schemas or enabling its implementation.
export const forkOpaqueSettingsFields = {
  prismRoles: Schema.optionalKey(Schema.Unknown),
  wightModes: Schema.optionalKey(Schema.Unknown),
};
