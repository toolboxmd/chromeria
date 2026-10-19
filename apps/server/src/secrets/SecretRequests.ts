/**
 * The user's answer goes straight to private files on the server, one per
 * field, that the agent's tools can read by path; a single-field answer is
 * also kept under a one-use SecretRef for tools that take one. Orchestration
 * only records the request and its status, so the value never reaches the
 * transcript, projections, clients, or the tool result.
 */
import { pipe } from "effect/Function";
import {
  CommandId,
  SECRET_REQUEST_SINGLE_FIELD_NAME,
  SecretRef,
  SecretRequestError,
  type ProjectId,
  type SecretRequestAnswerInput,
  type ThreadId,
} from "@t3tools/contracts";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect's Crypto has no createHmac.
import * as NodeCrypto from "node:crypto";

import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerConfig from "../config.ts";
import * as Metrics from "../observability/Metrics.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";

const SECRET_REF_PREFIX = "secret-ref:";
const storeName = (ref: SecretRef) => `secret-request-${ref.slice(SECRET_REF_PREFIX.length)}`;
const REF_PATTERN = /^secret-ref:[0-9a-f]{32}$/;
const SECRET_REF_TTL_MS = 24 * 60 * 60 * 1000;
const STORE_NAME_PATTERN = /^(secret-request-[0-9a-f]{32})\.bin$/;
/** A request's file directory, or one a save was still writing when it stopped. */
const FILES_DIR_PATTERN = /^[0-9a-f]{32}(\.tmp-[0-9a-f]+)?$/;
const FIELD_FILE_PATTERN = /^[a-z0-9_-]+$/;

const refFor = (salt: string, threadId: ThreadId, turnItemId: string) =>
  SecretRef.make(
    `${SECRET_REF_PREFIX}${NodeCrypto.createHmac("sha256", salt)
      .update(`${threadId}\u0000${turnItemId}`)
      .digest("hex")
      .slice(0, 32)}`,
  );

const StoredSecret = Schema.fromJsonString(
  Schema.Struct({ projectId: Schema.String, value: Schema.String, savedAt: Schema.Number }),
);
const encodeStored = Schema.encodeEffect(StoredSecret);
const decodeStored = Schema.decodeUnknownOption(StoredSecret);

/** What the tool that asked receives for a saved request. Paths, never values. */
export interface SavedSecret {
  /** Absolute path per field name that still has its file. */
  readonly files: Record<string, string>;
  /** Present for a single-field request until a tool consumes it. */
  readonly secretRef?: SecretRef;
}

export class SecretRequests extends Context.Service<
  SecretRequests,
  {
    /**
     * Answers a pending request in a thread. Saving writes every field's
     * value to its own file, all or none, before the card says saved.
     */
    readonly answer: (input: SecretRequestAnswerInput) => Effect.Effect<void, SecretRequestError>;
    /** The files and ref a saved request left, for the tool that asked. */
    readonly saved: (input: {
      readonly threadId: ThreadId;
      readonly turnItemId: string;
    }) => Effect.Effect<Option.Option<SavedSecret>>;
    /**
     * Reads and deletes a ref's value. Fails for unknown or used refs, and
     * for refs entered in another project.
     */
    readonly consume: (input: {
      readonly ref: SecretRef;
      readonly projectId: ProjectId;
    }) => Effect.Effect<string, SecretRequestError>;
  }
>()("t3/secrets/SecretRequests") {}

const make = Effect.gen(function* () {
  const store = yield* ServerSecretStore.ServerSecretStore;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;

  const salt = Buffer.from(
    yield* store.getOrCreateRandom("secret-request-salt", 32).pipe(Effect.orDie),
  ).toString("hex");

  // Under the environment's own state, never a project worktree; provider
  // CLIs run on this host, so agents read the files by path.
  const filesRoot = path.join(serverConfig.stateDir, "secret-requests");
  const filesDir = (ref: SecretRef) => path.join(filesRoot, ref.slice(SECRET_REF_PREFIX.length));

  /**
   * Writes one 0600 file per value into the request's 0700 directory. The
   * files appear together by renaming a finished temp directory, so the agent
   * never sees half a save. Answers run one at a time, so a directory already
   * there is an earlier save of this request whose record failed; it is
   * replaced, so the files always hold this save's values.
   */
  const writeFiles = (ref: SecretRef, values: Record<string, string>) =>
    Effect.gen(function* () {
      const target = filesDir(ref);
      yield* fileSystem.makeDirectory(filesRoot, { recursive: true, mode: 0o700 });
      yield* fileSystem.chmod(filesRoot, 0o700);
      const temp = `${target}.tmp-${NodeCrypto.randomBytes(8).toString("hex")}`;
      yield* Effect.gen(function* () {
        yield* fileSystem.makeDirectory(temp, { mode: 0o700 });
        yield* fileSystem.chmod(temp, 0o700);
        for (const [name, value] of Object.entries(values)) {
          const file = path.join(temp, name);
          yield* fileSystem.writeFile(file, new TextEncoder().encode(value), {
            flag: "wx",
            mode: 0o600,
          });
          yield* fileSystem.chmod(file, 0o600);
        }
        yield* fileSystem.remove(target, { recursive: true, force: true });
        yield* fileSystem.rename(temp, target);
      }).pipe(
        Effect.tapError(() =>
          fileSystem.remove(temp, { recursive: true, force: true }).pipe(Effect.ignore),
        ),
      );
    });

  /** Removes a request's files; a failure is logged, and the sweep retries it. */
  const removeFiles = (ref: SecretRef) =>
    fileSystem
      .remove(filesDir(ref), { recursive: true, force: true })
      .pipe(
        Effect.catch((error) =>
          Effect.logWarning("Could not delete secret request files", { errorTag: error._tag }),
        ),
      );

  /**
   * Keeps a one-field value under its ref. Set, not create: answers run one at
   * a time, so a value already there is an earlier save of this request whose
   * record failed, and this save's value replaces it to match the file.
   */
  const storeSingle = (ref: SecretRef, projectId: string, value: string) =>
    Effect.gen(function* () {
      const encoded = yield* encodeStored({
        projectId,
        value,
        savedAt: yield* Clock.currentTimeMillis,
      }).pipe(Effect.orDie);
      yield* store
        .set(storeName(ref), new TextEncoder().encode(encoded))
        .pipe(
          Effect.mapError((cause) => new SecretRequestError({ reason: "store_failed", cause })),
        );
    });

  /** Every field's value from a save, or none when a field is missing or extra. */
  const answerValues = (
    item: { readonly fields?: ReadonlyArray<{ readonly name: string }> | undefined },
    answer: Extract<SecretRequestAnswerInput["answer"], { readonly type: "save" }>,
  ): Record<string, string> | undefined => {
    const names = item.fields?.map((field) => field.name) ?? [SECRET_REQUEST_SINGLE_FIELD_NAME];
    const values =
      "secret" in answer ? { [SECRET_REQUEST_SINGLE_FIELD_NAME]: answer.secret } : answer.secrets;
    const keys = Object.keys(values);
    return keys.length === names.length && names.every((name) => Object.hasOwn(values, name))
      ? values
      : undefined;
  };

  const answerLock = yield* Semaphore.make(1);
  const answer: SecretRequests["Service"]["answer"] = (input) =>
    Effect.gen(function* () {
      yield* Effect.annotateCurrentSpan({
        "orchestration_v2.thread_id": input.threadId,
        "secret_request.answer": input.answer.type,
      });
      const records = yield* threadManagement
        .getThreadRecords(input.threadId, ["runs", "turnItems"], {
          turnItemTypes: ["secret_request"],
          messageRoles: [],
        })
        .pipe(Effect.mapError((cause) => new SecretRequestError({ reason: "load_failed", cause })));
      const item = records.turnItems.find((candidate) => candidate.id === input.turnItemId);
      if (item?.type !== "secret_request" || item.runId === null || item.nodeId === null) {
        return yield* new SecretRequestError({ reason: "not_found" });
      }
      if (item.secretStatus !== "pending") {
        return yield* new SecretRequestError({ reason: "already_answered" });
      }
      const run = records.runs.find((candidate) => candidate.id === item.runId);
      if (run === undefined || ThreadManagementService.isTerminalRunStatus(run.status)) {
        return yield* new SecretRequestError({ reason: "agent_stopped" });
      }
      const ref = refFor(salt, input.threadId, item.id);
      // Store first: the card only says saved once the value is kept.
      if (input.answer.type === "save") {
        const values = answerValues(item, input.answer);
        if (values === undefined) {
          return yield* new SecretRequestError({ reason: "fields_mismatch" });
        }
        // A ref carries one value, so only a one-field request gets one.
        const [single, ...rest] = Object.values(values);
        if (single !== undefined && rest.length === 0) {
          yield* storeSingle(ref, records.thread.projectId, single);
        }
        yield* writeFiles(ref, values).pipe(
          Effect.mapError(
            (error) => new SecretRequestError({ reason: "store_failed", cause: error }),
          ),
          // No ref without its file: the card stays pending for another save.
          Effect.tapError(() => removeLogged(storeName(ref))),
        );
      }
      const secretStatus = input.answer.type === "save" ? "saved" : "declined";
      yield* threadManagement
        .dispatch({
          type: "secret_request.record",
          commandId: CommandId.make(`secret-request:${item.id}:${secretStatus}`),
          threadId: input.threadId,
          runId: item.runId,
          nodeId: item.nodeId,
          turnItemId: item.id,
          label: item.label,
          reason: item.reason,
          ...(item.placeholder === undefined ? {} : { placeholder: item.placeholder }),
          ...(item.delivery === undefined ? {} : { delivery: item.delivery }),
          ...(item.fields === undefined ? {} : { fields: item.fields }),
          secretStatus,
        })
        .pipe(
          Effect.mapError((cause) => new SecretRequestError({ reason: "record_failed", cause })),
          // Clear failed saves so the pending card can be answered again.
          Effect.tapError(() =>
            input.answer.type === "save"
              ? Effect.andThen(removeLogged(storeName(ref)), removeFiles(ref))
              : Effect.void,
          ),
        );
      if (input.answer.type !== "save") return;
      // The wait may have closed the card; delete unrecorded answers because no one receives their refs.
      const recorded = yield* threadManagement
        .getThreadRecords(input.threadId, ["turnItems"], {
          turnItemTypes: ["secret_request"],
          messageRoles: [],
        })
        .pipe(
          Effect.mapError((cause) => new SecretRequestError({ reason: "record_failed", cause })),
        );
      const card = recorded.turnItems.find((candidate) => candidate.id === item.id);
      if (card?.type === "secret_request" && card.secretStatus !== "saved") {
        yield* removeLogged(storeName(ref));
        yield* removeFiles(ref);
        return yield* new SecretRequestError({ reason: "agent_stopped" });
      }
    }).pipe(
      // One answer at a time, across every request: two clients answering the
      // same card see one answer win and the other already answered, and an
      // answer's cleanup can never delete what another answer saved. Answers
      // are rare human actions, so one lock costs nothing.
      answerLock.withPermits(1),
      Effect.withSpan("SecretRequests.answer"),
    );

  const saved: SecretRequests["Service"]["saved"] = (input) =>
    Effect.gen(function* () {
      const ref = refFor(salt, input.threadId, input.turnItemId);
      const stored = yield* store.get(storeName(ref)).pipe(Effect.orElseSucceed(Option.none));
      const dir = filesDir(ref);
      // Only field files: anything else an agent left in the directory is not a field.
      const names = (yield* fileSystem
        .readDirectory(dir)
        .pipe(Effect.orElseSucceed((): Array<string> => []))).filter((name) =>
        FIELD_FILE_PATTERN.test(name),
      );
      if (Option.isNone(stored) && names.length === 0) return Option.none();
      return Option.some({
        files: Object.fromEntries(names.toSorted().map((name) => [name, path.join(dir, name)])),
        ...(Option.isSome(stored) ? { secretRef: ref } : {}),
      });
    });

  const removeLogged = (name: string) =>
    store.remove(name).pipe(
      Effect.as(true),
      Effect.catch((error) =>
        Effect.logWarning("Could not delete a secret request value", {
          errorTag: error._tag,
        }).pipe(Effect.as(false)),
      ),
    );

  // Serialize get/remove so concurrent consumers cannot read the same one-use ref.
  const consumeLock = yield* Semaphore.make(1);
  const consume: SecretRequests["Service"]["consume"] = (input) =>
    consumeRef(input).pipe(
      consumeLock.withPermits(1),
      Effect.tap(() => Metrics.increment(Metrics.secretRefsConsumedTotal, { result: "used" })),
      Effect.tapError(() =>
        Metrics.increment(Metrics.secretRefsConsumedTotal, { result: "rejected" }),
      ),
      Effect.withSpan("SecretRequests.consume"),
    );

  /** A live ref's value, left in the store. */
  const readRef = Effect.fn("SecretRequests.readRef")(function* (input: {
    readonly ref: SecretRef;
    readonly projectId: ProjectId;
  }) {
    if (!REF_PATTERN.test(input.ref))
      return yield* new SecretRequestError({ reason: "invalid_ref" });
    const stored = yield* store
      .get(storeName(input.ref))
      .pipe(Effect.mapError((cause) => new SecretRequestError({ reason: "read_failed", cause })));
    const decoded = Option.flatMap(stored, (bytes) =>
      decodeStored(new TextDecoder().decode(bytes)),
    );
    if (Option.isNone(decoded) || decoded.value.projectId !== input.projectId) {
      return yield* new SecretRequestError({ reason: "ref_unavailable" });
    }
    if ((yield* Clock.currentTimeMillis) - decoded.value.savedAt > SECRET_REF_TTL_MS) {
      yield* removeLogged(storeName(input.ref));
      return yield* new SecretRequestError({ reason: "ref_expired" });
    }
    return decoded.value.value;
  });

  const consumeRef = Effect.fn("SecretRequests.consumeRef")(function* (input: {
    readonly ref: SecretRef;
    readonly projectId: ProjectId;
  }) {
    const value = yield* readRef(input);
    // One use: the value moves into whatever consumed it. If it cannot be
    // deleted, it is not handed out, so a ref is never used twice.
    if (!(yield* removeLogged(storeName(input.ref)))) {
      return yield* new SecretRequestError({ reason: "consume_failed" });
    }
    return value;
  });

  const sweepExpired = Effect.gen(function* () {
    if (store.directory === undefined) return;
    const now = yield* Clock.currentTimeMillis;
    const names = (yield* fileSystem.readDirectory(store.directory)).flatMap((file) => {
      const match = STORE_NAME_PATTERN.exec(file);
      return match?.[1] === undefined ? [] : [match[1]];
    });
    let removed = 0;
    for (const name of names) {
      const stored = yield* store.get(name).pipe(Effect.orElseSucceed(Option.none));
      const decoded = Option.flatMap(stored, (bytes) =>
        decodeStored(new TextDecoder().decode(bytes)),
      );
      if (Option.isSome(decoded) && now - decoded.value.savedAt <= SECRET_REF_TTL_MS) continue;
      if (yield* removeLogged(name)) removed += 1;
    }
    yield* Effect.annotateCurrentSpan({ "secret_request.expired_removed": removed });
  }).pipe(
    Effect.catchCause((cause) => Effect.logWarning("Could not sweep expired secret refs", cause)),
    Effect.withSpan("SecretRequests.sweepExpired"),
  );
  /**
   * Drops request files past the TTL, by their own write time, and request
   * directories the agent already emptied. A save stopped midway leaves a
   * temp directory that expires the same way.
   */
  const sweepExpiredFiles = Effect.gen(function* () {
    if (!(yield* fileSystem.exists(filesRoot))) return;
    const now = yield* Clock.currentTimeMillis;
    let removed = 0;
    for (const entry of yield* fileSystem.readDirectory(filesRoot)) {
      const match = FILES_DIR_PATTERN.exec(entry);
      if (match === null) continue;
      const dir = path.join(filesRoot, entry);
      // One directory that cannot be read or removed must not keep the
      // sweep from the rest; it is retried on the next sweep.
      removed += yield* Effect.gen(function* () {
        const expired = yield* Effect.gen(function* () {
          const names = yield* fileSystem.readDirectory(dir);
          if (names.length === 0 && match[1] === undefined) return true;
          // An emptied temp directory has only its own time to go by.
          const times = yield* Effect.forEach(names.length > 0 ? names : [""], (name) =>
            fileSystem
              .stat(path.join(dir, name))
              .pipe(
                Effect.map((info) => Option.getOrElse(Option.map(info.mtime, Number), () => 0)),
              ),
          );
          return now - Math.max(...times) > SECRET_REF_TTL_MS;
        });
        if (!expired) return 0;
        yield* fileSystem.remove(dir, { recursive: true, force: true });
        return 1;
      }).pipe(
        Effect.catch((error) =>
          Effect.logWarning("Could not sweep a secret request directory", {
            errorTag: error._tag,
          }).pipe(Effect.as(0)),
        ),
      );
    }
    yield* Effect.annotateCurrentSpan({ "secret_request.expired_files_removed": removed });
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("Could not sweep expired secret request files", cause),
    ),
    Effect.withSpan("SecretRequests.sweepExpiredFiles"),
  );
  const sweep = Effect.andThen(sweepExpired, sweepExpiredFiles);
  yield* sweep;
  yield* sweep.pipe(
    Effect.delay("1 hour"),
    Effect.repeat(Schedule.spaced("1 hour")),
    Effect.forkScoped,
  );

  return SecretRequests.of({ answer, saved, consume });
});

export const layer = Layer.effect(SecretRequests, make);
