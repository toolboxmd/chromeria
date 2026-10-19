import { PreviewCredentialField } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as ServerConfig from "../config.ts";

export const CREDENTIAL_AUDIT_FILE = "credential-fills.jsonl";

/**
 * One line per fill attempt, so the user can see which agent logged in where. It
 * holds no value and no page data: the site is the vault host the page matched.
 */
const CredentialAuditLine = Schema.Struct({
  time: Schema.String,
  environmentId: Schema.String,
  threadId: Schema.String,
  item: Schema.String,
  field: PreviewCredentialField,
  site: Schema.NullOr(Schema.String),
  /** `filled`, a PreviewCredentialError reason, or another error's tag. */
  result: Schema.String,
});
export type CredentialAuditEntry = Omit<typeof CredentialAuditLine.Type, "time">;
const encodeLine = Schema.encodeSync(Schema.fromJsonString(CredentialAuditLine));

export const recordCredentialFill = Effect.fn("CredentialAudit.record")(
  function* (entry: CredentialAuditEntry) {
    const config = yield* ServerConfig.ServerConfig;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const file = path.join(config.stateDir, CREDENTIAL_AUDIT_FILE);
    const time = DateTime.formatIso(yield* DateTime.now);
    yield* fileSystem.writeFileString(file, `${encodeLine({ time, ...entry })}\n`, {
      flag: "a",
      mode: 0o600,
    });
    // `mode` applies only when the file is created.
    yield* fileSystem.chmod(file, 0o600);
  },
  Effect.catch((cause) => Effect.logWarning("credential fill audit line not written", { cause })),
);
