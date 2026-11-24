import * as Schema from "effect/Schema";

import { EnvironmentId } from "./baseSchemas.ts";
import { BrowserProfileId } from "./browserProfile.ts";
import { MirrorCookie } from "./browserCookieMirror.ts";

const LOGIN_IMPORT_MAX_HOSTS = 8;
export const LOGIN_IMPORT_MAX_COOKIES = 256;
/** Summed UTF-8 bytes of every cookie's name and value. */
export const LOGIN_IMPORT_MAX_BYTES = 64 * 1024;

/** A bare DNS host such as `admin.shopify.com`: no scheme, port, path or wildcard. */
export const LoginImportHost = Schema.String.check(
  Schema.isMaxLength(253),
  Schema.isPattern(
    /^(?=.*[a-z])[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/,
  ),
).annotate({ description: "A lowercase host the site uses, such as admin.shopify.com." });

const LoginImportHosts = Schema.Array(LoginImportHost).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(LOGIN_IMPORT_MAX_HOSTS),
);

export const PreviewLoginImportInput = Schema.Struct({
  hosts: LoginImportHosts.annotate({
    description:
      "1 to 8 hosts the logged-out site itself uses. Never identity-provider hosts such as accounts.google.com.",
  }),
  profileId: BrowserProfileId.annotate({
    description: "The browser profile here that receives the session; open it afterwards.",
  }),
  sourceEnvironmentId: Schema.optional(EnvironmentId).annotate({
    description: "The linked environment to copy from. Needed only when several share logins.",
  }),
  sourceProfileId: Schema.optional(BrowserProfileId).annotate({
    description: "The source environment's profile. Defaults to profileId.",
  }),
});
export type PreviewLoginImportInput = typeof PreviewLoginImportInput.Type;

const Count = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/** Counts only: cookie names and values never reach the agent. */
export const PreviewLoginImportResult = Schema.Struct({
  profileId: BrowserProfileId,
  imported: Count,
  skipped: Schema.Struct({
    deviceBound: Count,
    partitioned: Count,
    sharedDomain: Count,
  }),
});
export type PreviewLoginImportResult = typeof PreviewLoginImportResult.Type;

/** Peer-only: a source environment's answer to `browser_login_export`. */
export const PreviewLoginExportInput = Schema.Struct({
  hosts: LoginImportHosts,
  profileId: BrowserProfileId,
});
export type PreviewLoginExportInput = typeof PreviewLoginExportInput.Type;

export const PreviewLoginExportResult = Schema.Struct({
  cookies: Schema.Array(MirrorCookie).check(Schema.isMaxLength(LOGIN_IMPORT_MAX_COOKIES)),
  skipped: Schema.Struct({ deviceBound: Count, partitioned: Count }),
});
export type PreviewLoginExportResult = typeof PreviewLoginExportResult.Type;
