import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as ServerConfig from "../config.ts";

export const LOGIN_IMPORT_AUDIT_FILE = "login-imports.jsonl";

/**
 * One line per import on the destination and per export on the source, so the
 * user can see which agent copied which site's session. Never a cookie name or value.
 */
const LoginImportAuditLine = Schema.Struct({
  time: Schema.String,
  side: Schema.Literals(["import", "export"]),
  environmentId: Schema.String,
  peerEnvironmentId: Schema.NullOr(Schema.String),
  threadId: Schema.NullOr(Schema.String),
  profileId: Schema.String,
  sourceProfileId: Schema.String,
  hosts: Schema.Array(Schema.String),
  cookies: Schema.NullOr(Schema.Int),
  skipped: Schema.NullOr(
    Schema.Struct({
      deviceBound: Schema.Int,
      partitioned: Schema.Int,
      sharedDomain: Schema.optional(Schema.Int),
    }),
  ),
  /** `imported`, `exported`, or a refusal reason such as `grant_off`. */
  result: Schema.String,
});
export type LoginImportAuditEntry = Omit<typeof LoginImportAuditLine.Type, "time">;
const encodeLine = Schema.encodeSync(Schema.fromJsonString(LoginImportAuditLine));

/** The reason a refusal message leads with, or the error's own code. */
export const auditReason = (error: { readonly message: string; readonly _tag: string }) =>
  /^([a-z_]+):/.exec(error.message)?.[1] ??
  ("code" in error && typeof error.code === "string" ? error.code : error._tag);

export const recordLoginImport = Effect.fn("LoginImportAudit.record")(
  function* (entry: LoginImportAuditEntry) {
    const config = yield* ServerConfig.ServerConfig;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const file = path.join(config.stateDir, LOGIN_IMPORT_AUDIT_FILE);
    const time = DateTime.formatIso(yield* DateTime.now);
    yield* fileSystem.writeFileString(file, `${encodeLine({ time, ...entry })}\n`, {
      flag: "a",
      mode: 0o600,
    });
    // `mode` applies only when the file is created.
    yield* fileSystem.chmod(file, 0o600);
  },
  Effect.catch((cause) => Effect.logWarning("login import audit line not written", { cause })),
);
