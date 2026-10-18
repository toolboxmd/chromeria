import { AuthSessionId, AuthRenewalCommitment, AuthStandardClientScopes } from "@t3tools/contracts";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as References from "effect/References";
import { Argument, Command, Flag, GlobalFlag } from "effect/cli";

import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";

import {
  formatIssuedPairingCredential,
  formatIssuedSession,
  formatPairingCredentialList,
  formatSessionList,
} from "../cliAuthFormat.ts";
import * as ServerConfig from "../config.ts";
import { authScopesFlag } from "./authScopes.ts";
import {
  authLocationFlags,
  type CliAuthLocationFlags,
  DurationFromString,
  resolveCliAuthConfig,
} from "./config.ts";

const runWithEnvironmentAuth = Effect.fn("auth.runWithEnvironmentAuth")(function* <A, E>(
  flags: CliAuthLocationFlags,
  run: (environmentAuth: EnvironmentAuth.EnvironmentAuth["Service"]) => Effect.Effect<A, E>,
  options?: {
    readonly quietLogs?: boolean;
  },
) {
  const logLevel = yield* GlobalFlag.LogLevel;
  const config = yield* resolveCliAuthConfig(flags, logLevel);
  const minimumLogLevel = options?.quietLogs ? "Error" : config.logLevel;
  return yield* Effect.gen(function* () {
    const environmentAuth = yield* EnvironmentAuth.EnvironmentAuth;
    return yield* run(environmentAuth);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(EnvironmentAuth.layerRuntime).pipe(
        Layer.provide(ServerConfig.layer(config)),
        Layer.provide(Layer.succeed(References.MinimumLogLevel, minimumLogLevel)),
      ),
    ),
  );
});

const ttlFlag = Flag.String("ttl").pipe(
  Flag.withSchema(DurationFromString),
  Flag.withDescription("TTL, for example `5m`, `1h`, `30d`, or `15 minutes`."),
  Flag.optional,
);

const jsonFlag = Flag.Boolean("json").pipe(
  Flag.withDescription("Emit JSON instead of human-readable output."),
  Flag.withDefault(false),
);

const labelFlag = Flag.String("label").pipe(
  Flag.withDescription("Optional human-readable label."),
  Flag.optional,
);

const subjectFlag = Flag.String("subject").pipe(
  Flag.withDescription("Optional session subject."),
  Flag.optional,
);

const baseUrlFlag = Flag.String("base-url").pipe(
  Flag.withDescription("Optional public base URL used to print a ready `/pair#token=...` link."),
  Flag.optional,
);

const tokenOnlyFlag = Flag.Boolean("token-only").pipe(
  Flag.withDescription("Print only the issued bearer token."),
  Flag.withDefault(false),
);

const pairingCreateCommand = Command.make("create", {
  ...authLocationFlags,
  scopes: authScopesFlag(AuthStandardClientScopes),
  ttl: ttlFlag,
  label: labelFlag,
  baseUrl: baseUrlFlag,
  json: jsonFlag,
}).pipe(
  Command.withDescription("Issue a new client pairing token."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(
      flags,
      (environmentAuth) =>
        Effect.gen(function* () {
          const issued = yield* environmentAuth.createPairingLink({
            scopes: flags.scopes,
            subject: "one-time-token",
            ...(Option.isSome(flags.ttl) ? { ttl: flags.ttl.value } : {}),
            ...(Option.isSome(flags.label) ? { label: flags.label.value } : {}),
          });
          const output = formatIssuedPairingCredential(issued, {
            json: flags.json,
            ...(Option.isSome(flags.baseUrl) ? { baseUrl: flags.baseUrl.value } : {}),
          });
          yield* Console.log(output);
        }),
      {
        quietLogs: flags.json,
      },
    ),
  ),
);

const pairingListCommand = Command.make("list", {
  ...authLocationFlags,
  json: jsonFlag,
}).pipe(
  Command.withDescription("List active client pairing tokens without revealing their secrets."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(
      flags,
      (environmentAuth) =>
        Effect.gen(function* () {
          const pairingLinks = yield* environmentAuth.listPairingLinks({
            excludeSubjects: [EnvironmentAuth.INTERNAL_ADMINISTRATIVE_BOOTSTRAP_SUBJECT],
          });
          yield* Console.log(formatPairingCredentialList(pairingLinks, { json: flags.json }));
        }),
      {
        quietLogs: flags.json,
      },
    ),
  ),
);

const pairingRevokeCommand = Command.make("revoke", {
  ...authLocationFlags,
  id: Argument.String("id").pipe(Argument.withDescription("Pairing credential id to revoke.")),
}).pipe(
  Command.withDescription("Revoke an active client pairing token."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(flags, (environmentAuth) =>
      Effect.gen(function* () {
        const revoked = yield* environmentAuth.revokePairingLink(flags.id);
        yield* Console.log(
          revoked
            ? `Revoked pairing credential ${flags.id}.\n`
            : `No active pairing credential found for ${flags.id}.\n`,
        );
      }),
    ),
  ),
);

const pairingCommand = Command.make("pairing").pipe(
  Command.withDescription("Manage one-time client pairing tokens."),
  Command.withSubcommands([pairingCreateCommand, pairingListCommand, pairingRevokeCommand]),
);

const sessionIssueCommand = Command.make("issue", {
  ...authLocationFlags,
  scopes: authScopesFlag(AuthStandardClientScopes),
  ttl: ttlFlag,
  label: labelFlag,
  subject: subjectFlag,
  tokenOnly: tokenOnlyFlag,
  json: jsonFlag,
}).pipe(
  Command.withDescription("Issue a scoped bearer access token for headless or remote clients."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(
      flags,
      (environmentAuth) =>
        Effect.gen(function* () {
          const issued = yield* environmentAuth.issueSession({
            scopes: flags.scopes,
            ...(Option.isSome(flags.ttl) ? { ttl: flags.ttl.value } : {}),
            ...(Option.isSome(flags.label) ? { label: flags.label.value } : {}),
            ...(Option.isSome(flags.subject) ? { subject: flags.subject.value } : {}),
          });
          yield* Console.log(
            formatIssuedSession(issued, {
              json: flags.json,
              tokenOnly: flags.tokenOnly,
            }),
          );
        }),
      {
        quietLogs: flags.json || flags.tokenOnly,
      },
    ),
  ),
);

const sessionListCommand = Command.make("list", {
  ...authLocationFlags,
  json: jsonFlag,
}).pipe(
  Command.withDescription("List active sessions without revealing bearer tokens."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(
      flags,
      (environmentAuth) =>
        Effect.gen(function* () {
          const sessions = yield* environmentAuth.listSessions();
          yield* Console.log(formatSessionList(sessions, { json: flags.json }));
        }),
      {
        quietLogs: flags.json,
      },
    ),
  ),
);

const sessionRevokeCommand = Command.make("revoke", {
  ...authLocationFlags,
  sessionId: Argument.String("session-id").pipe(
    Argument.withDescription("Session id to revoke."),
    Argument.withSchema(AuthSessionId),
  ),
}).pipe(
  Command.withDescription("Revoke an active session."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(flags, (environmentAuth) =>
      Effect.gen(function* () {
        const revoked = yield* environmentAuth.revokeSession(flags.sessionId);
        yield* Console.log(
          revoked
            ? `Revoked session ${flags.sessionId}.\n`
            : `No active session found for ${flags.sessionId}.\n`,
        );
      }),
    ),
  ),
);

const sessionCommand = Command.make("session").pipe(
  Command.withDescription("Manage bearer sessions."),
  Command.withSubcommands([sessionIssueCommand, sessionListCommand, sessionRevokeCommand]),
);

class RenewalDiagnosticError extends Schema.TaggedError<RenewalDiagnosticError>()(
  "RenewalDiagnosticError",
  {},
) {
  override get message() {
    return "Desktop renewal diagnostic directory mismatch.";
  }
}
const renewalListCommand = Command.make("list", { ...authLocationFlags, json: jsonFlag }).pipe(
  Command.withDescription("List device renewal enrollments and their public commitments."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(
      flags,
      (auth) =>
        Effect.gen(function* () {
          const entries = (yield* auth.listRenewalEnrollments()).map((entry) => ({
            ...entry,
            fingerprint: entry.fingerprint.slice(0, 16),
          }));
          if (flags.json)
            return yield* Console.log(
              yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(entries),
            );
          for (const entry of entries) {
            const clean = (value: string | null) =>
              value?.replace(/[\x00-\x1f\x7f]/g, " ").slice(0, 256) ?? "unknown";
            yield* Console.log(
              `${entry.enrollmentId}  ${entry.state}\n  commitment: ${entry.fingerprint}\n  session: ${entry.sessionId}  expires: ${entry.expiresAt}\n  client: ${clean(entry.client)} | ${entry.deviceType} | ${clean(entry.os)}\n  route: ${clean(entry.route)}  ip: ${clean(entry.ipAddress)}\n  requested: ${entry.requestedAt}  last seen: ${entry.lastSeenAt}  last connected: ${entry.lastConnectedAt ?? "unknown"}`,
            );
          }
        }),
      { quietLogs: flags.json },
    ),
  ),
);
const renewalApproveCommand = Command.make("approve", {
  ...authLocationFlags,
  id: Argument.String("enrollment-id"),
  commitment: Flag.String("commitment").pipe(
    Flag.withSchema(AuthRenewalCommitment),
    Flag.withDescription("Full commitment read independently from the intended client."),
  ),
}).pipe(
  Command.withDescription("Approve the exact verified device commitment without pairing again."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(flags, (auth) =>
      auth
        .approveRenewalEnrollment(flags.id, flags.commitment)
        .pipe(Effect.andThen(Console.log(`Approved renewal enrollment ${flags.id}.`))),
    ),
  ),
);
const renewalRejectCommand = Command.make("reject", {
  ...authLocationFlags,
  id: Argument.String("enrollment-id"),
}).pipe(
  Command.withDescription("Reject a pending renewal commitment."),
  Command.withHandler((flags) =>
    runWithEnvironmentAuth(flags, (auth) =>
      auth
        .rejectRenewalEnrollment(flags.id)
        .pipe(Effect.andThen(Console.log(`Rejected renewal enrollment ${flags.id}.`))),
    ),
  ),
);
const renewalClientCommitments = Command.make("client-commitments", {
  stateDir: Flag.String("desktop-state-dir").pipe(
    Flag.withDescription(
      "The desktop app's actual state directory; no server-home default is assumed.",
    ),
  ),
  json: jsonFlag,
}).pipe(
  Command.withDescription("Read only public commitments from this desktop's diagnostic file."),
  Command.withHandler((flags) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.realPath(path.resolve(flags.stateDir));
      const document = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(
          Schema.Struct({
            stateDir: Schema.String,
            entries: Schema.Array(
              Schema.Struct({
                key: Schema.String,
                environmentId: Schema.String,
                origin: Schema.String,
                epoch: Schema.String,
                commitment: AuthRenewalCommitment,
              }),
            ),
          }),
        ),
      )(yield* fs.readFileString(path.join(directory, "renewal-commitments.json")));
      if ((yield* fs.realPath(path.resolve(document.stateDir))) !== directory)
        return yield* new RenewalDiagnosticError({});
      yield* Console.log(
        flags.json
          ? yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(document.entries)
          : document.entries
              .map((entry) => `${entry.environmentId} ${entry.origin}\n  ${entry.commitment}`)
              .join("\n"),
      );
    }),
  ),
);
const renewalCommand = Command.make("renewal").pipe(
  Command.withDescription("Approve or reject device renewal authority headlessly."),
  Command.withSubcommands([
    renewalListCommand,
    renewalApproveCommand,
    renewalRejectCommand,
    renewalClientCommitments,
  ]),
);

export const authCommand = Command.make("auth").pipe(
  Command.withDescription("Manage the local auth control plane for headless deployments."),
  Command.withSubcommands([pairingCommand, sessionCommand, renewalCommand]),
);
