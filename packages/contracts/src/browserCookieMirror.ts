import * as Schema from "effect/Schema";

import { EnvironmentId } from "./baseSchemas.ts";
import { BrowserProfileId, BrowserProfileName } from "./browserProfile.ts";

export const COOKIE_MIRROR_PATH = "/api/browser-cookie-mirror";
export const COOKIE_MIRROR_GRANT_PATH = `${COOKIE_MIRROR_PATH}/grant`;
export const COOKIE_MIRROR_MAX_MUTATIONS = 256;

export const ChromeCookieKeyCommand = Schema.Array(Schema.String).check(Schema.isMaxLength(128));
export type ChromeCookieKeyCommand = typeof ChromeCookieKeyCommand.Type;

export const CookieMirrorProfile = Schema.Struct({
  id: BrowserProfileId.check(Schema.isPattern(/^(?!incognito$)/)),
  name: BrowserProfileName,
  kind: Schema.Literal("persistent"),
});
export type CookieMirrorProfile = typeof CookieMirrorProfile.Type;

export const CookieMirrorPartitionKey = Schema.Struct({
  topLevelSite: Schema.String.check(Schema.isPattern(/^https?:\/\/[^/]+$/)),
  hasCrossSiteAncestor: Schema.Boolean,
});

export const CookieMirrorKey = Schema.Struct({
  name: Schema.String,
  domain: Schema.String.check(Schema.isMinLength(1)),
  path: Schema.String.check(Schema.isPattern(/^\//)),
  partitionKey: Schema.optionalKey(CookieMirrorPartitionKey),
});
export type CookieMirrorKey = typeof CookieMirrorKey.Type;

export const MirrorCookie = Schema.Struct({
  ...CookieMirrorKey.fields,
  value: Schema.String,
  secure: Schema.Boolean,
  httpOnly: Schema.Boolean,
  sameSite: Schema.Literals(["unspecified", "no_restriction", "lax", "strict"]),
  expires: Schema.optionalKey(Schema.Finite.check(Schema.isGreaterThan(0))),
});
export type MirrorCookie = typeof MirrorCookie.Type;

export const CookieMirrorMutation = Schema.Union([
  Schema.Struct({ type: Schema.Literal("set"), cookie: MirrorCookie }),
  Schema.Struct({ type: Schema.Literal("remove"), key: CookieMirrorKey }),
]);
export type CookieMirrorMutation = typeof CookieMirrorMutation.Type;

export const CookieMirrorBatch = Schema.Struct({
  environmentId: EnvironmentId,
  sequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  profile: CookieMirrorProfile,
  mutations: Schema.Array(CookieMirrorMutation).check(
    Schema.isMaxLength(COOKIE_MIRROR_MAX_MUTATIONS),
  ),
});
export type CookieMirrorBatch = typeof CookieMirrorBatch.Type;

export const CookieMirrorGrant = Schema.Struct({
  ticket: Schema.String,
  environmentId: EnvironmentId,
});

export type CookieMirrorGrant = typeof CookieMirrorGrant.Type;

export const CookieMirrorReply = Schema.Union([
  Schema.Struct({ type: Schema.Literal("ready"), environmentId: EnvironmentId }),
  Schema.Struct({ type: Schema.Literal("ack"), sequence: Schema.Int }),
  Schema.Struct({ type: Schema.Literal("resync"), profileId: BrowserProfileId }),
]);

export const CookieMirrorConnectionInput = Schema.Union([
  Schema.Struct({
    action: Schema.Literal("connect"),
    environmentId: EnvironmentId,
  }),
  Schema.Struct({
    action: Schema.Literal("status"),
    environmentId: Schema.optionalKey(EnvironmentId),
  }),
  Schema.Struct({
    action: Schema.Literal("disconnect"),
    environmentId: EnvironmentId,
    generation: Schema.Int,
  }),
]);
export type CookieMirrorConnectionInput = typeof CookieMirrorConnectionInput.Type;
export const CookieMirrorSiteLimitation = Schema.Struct({
  host: Schema.String,
  reason: Schema.Literal("device-bound, not mirrored"),
});
export type CookieMirrorSiteLimitation = typeof CookieMirrorSiteLimitation.Type;
export const CookieMirrorConnectionStatus = Schema.Struct({
  connected: Schema.Boolean,
  generation: Schema.Int,
  profiles: Schema.Array(CookieMirrorProfile),
  limitations: Schema.Array(CookieMirrorSiteLimitation),
});
export type CookieMirrorConnectionStatus = typeof CookieMirrorConnectionStatus.Type;

export class CookieMirrorError extends Schema.TaggedError<CookieMirrorError>()(
  "CookieMirrorError",
  {
    reason: Schema.Literals(["forbidden", "unavailable"]),
  },
) {
  override get message() {
    return `Browser cookie mirror ${this.reason}.`;
  }
}

export function cookieMirrorKey(cookie: CookieMirrorKey): string {
  return JSON.stringify([
    cookie.domain,
    cookie.path,
    cookie.name,
    cookie.partitionKey?.topLevelSite ?? null,
    cookie.partitionKey?.hasCrossSiteAncestor ?? null,
  ]);
}
