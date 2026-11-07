import * as RemoteSecretRelay from "./RemoteSecretRelay.ts";
import type { AuthSessionId } from "@t3tools/contracts";
/**
 * The user's answer goes to private per-field files the agent reads by path, and a single-field
 * answer also to a one-use SecretRef. Orchestration records only the request and its status, so the
 * value never reaches the transcript, projections, clients or the tool result.
 */
import {
  CommandId,
  SECRET_REQUEST_SINGLE_FIELD_NAME,
  SecretRef,
  SecretRequestError,
  type ProjectId,
  type SecretReference,
  type SecretRequestAnswerInput,
  type ThreadId,
} from "@t3tools/contracts";
import * as SecretResolver from "@t3tools/secret-provider-core/server/SecretResolver";

import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Mac from "effect/crypto/Mac";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
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
    readonly answer: (
      input: SecretRequestAnswerInput,
      clientSessionId?: AuthSessionId,
    ) => Effect.Effect<void, SecretRequestError>;
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
    /** Moves a saved `file` request's value to an owner-only file and returns its path. */
    readonly saveToFile: (input: {
      readonly threadId: ThreadId;
      readonly turnItemId: string;
    }) => Effect.Effect<string, SecretRequestError>;
    /** Writes a secret the user pasted in chat to an owner-only file and returns its path. */
    readonly savePasted: (input: {
      readonly threadId: ThreadId;
      /** The provider session's namespace, so two sessions' keys never share a file. */
      readonly requestNamespace: string;
      readonly requestKey: string;
      readonly value: string;
    }) => Effect.Effect<string, SecretRequestError>;
  }
>()("t3/secrets/SecretRequests") {}

const make = Effect.gen(function* () {
  const store = yield* ServerSecretStore.ServerSecretStore;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const threadManagement = yield* ThreadManagementService.ThreadManagementService;
  const resolver = yield* SecretResolver.SecretResolver;

  const mac = yield* Mac.Mac;
  const random = yield* SecureRandom.SecureRandom;
  // Keyed by the salt's hex text, as refs saved by earlier versions were.
  const refKey = yield* Mac.key(
    Mac.hmacSha256,
    new TextEncoder().encode(
      Hex.encode(yield* store.getOrCreateRandom("secret-request-salt", 32).pipe(Effect.orDie)),
    ),
  ).pipe(Effect.orDie);
  const refFor = (threadId: ThreadId, turnItemId: string) =>
    mac.sign(refKey, new TextEncoder().encode(`${threadId}\u0000${turnItemId}`)).pipe(
      Effect.orDie,
      Effect.map((tag) => SecretRef.make(`${SECRET_REF_PREFIX}${Hex.encode(tag).slice(0, 32)}`)),
    );

  const readReference = Effect.fn("SecretRequests.readReference")(function* (
    reference: SecretReference,
  ) {
    // The resolver keeps failed reads for the server's life; a retry after unlocking must re-read.
    yield* resolver.invalidate([reference]);
    const [value] = yield* resolver.read([reference]);
    if (value === undefined || value.trim() === "") {
      return yield* new SecretRequestError({
        reason: "reference_unreadable",
        provider: yield* resolver.label(reference.provider),
      });
    }
    return value;
  });

  // Under the environment's own state, never a project worktree; provider
  // CLIs run on this host, so agents read the files by path.
  const filesRoot = path.join(serverConfig.stateDir, "secret-requests");
  const filesDir = (ref: SecretRef) => path.join(filesRoot, ref.slice(SECRET_REF_PREFIX.length));

  /**
   * Writes one 0600 file per value into the request's 0700 directory, renamed in from a finished temp
   * directory so the agent never sees half a save. An existing directory is an earlier save whose
   * record failed, so it is replaced.
   */
  const writeFiles = Effect.fn("SecretRequests.writeFiles")(function* (
    ref: SecretRef,
    values: Record<string, string>,
  ) {
    const target = filesDir(ref);
    yield* fileSystem.makeDirectory(filesRoot, { recursive: true, mode: 0o700 });
    yield* fileSystem.chmod(filesRoot, 0o700);
    const temp = `${target}.tmp-${Hex.encode(yield* random.randomBytes(8))}`;
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
  const storeSingle = Effect.fn("SecretRequests.storeSingle")(function* (
    ref: SecretRef,
    projectId: string,
    value: string,
  ) {
    const encoded = yield* encodeStored({
      projectId,
      value,
      savedAt: yield* Clock.currentTimeMillis,
    }).pipe(Effect.orDie);
    yield* store
      .set(storeName(ref), new TextEncoder().encode(encoded))
      .pipe(Effect.mapError((cause) => new SecretRequestError({ reason: "store_failed", cause })));
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
  const answer: SecretRequests["Service"]["answer"] = Effect.fn("SecretRequests.answer")(
    function* (input, clientSessionId) {
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
      if (input.answer.type === "reference" && item.fields !== undefined)
        return yield* new SecretRequestError({ reason: "fields_mismatch" });
      const relay = yield* Effect.serviceOption(RemoteSecretRelay.RemoteSecretRelay);
      const remote = item.destination !== undefined;
      if (remote && input.answer.type === "reference")
        return yield* new SecretRequestError({ reason: "reference_unreadable" });
      if (remote && (clientSessionId === undefined || Option.isNone(relay)))
        return yield* new SecretRequestError({ reason: "store_failed" });
      const ref = yield* refFor(input.threadId, item.id);
      // Store first: the card only says saved once the value is kept.
      if (remote && Option.isSome(relay) && clientSessionId !== undefined) {
        if (input.answer.type === "save" && answerValues(item, input.answer) === undefined)
          return yield* new SecretRequestError({ reason: "fields_mismatch" });
        yield* relay.value
          .answer(input, clientSessionId)
          .pipe(Effect.mapError(() => new SecretRequestError({ reason: "store_failed" })));
      } else if (input.answer.type !== "decline") {
        const values =
          input.answer.type === "reference"
            ? { [SECRET_REQUEST_SINGLE_FIELD_NAME]: yield* readReference(input.answer.reference) }
            : answerValues(item, input.answer);
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
      const secretStatus = input.answer.type === "decline" ? "declined" : "saved";
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
          ...(item.destination === undefined ? {} : { destination: item.destination }),
          secretStatus,
        })
        .pipe(
          Effect.mapError((cause) => new SecretRequestError({ reason: "record_failed", cause })),
          // Clear failed saves so the pending card can be answered again.
          Effect.tapError(() =>
            !remote && input.answer.type !== "decline"
              ? Effect.andThen(removeLogged(storeName(ref)), removeFiles(ref))
              : Effect.void,
          ),
        );
      if (input.answer.type === "decline") return;
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
        if (remote && Option.isSome(relay))
          yield* relay.value
            .retire(input.threadId, item.id)
            .pipe(Effect.mapError(() => new SecretRequestError({ reason: "record_failed" })));
        else {
          yield* removeLogged(storeName(ref));
          yield* removeFiles(ref);
        }
        return yield* new SecretRequestError({ reason: "agent_stopped" });
      }
    },
    answerLock.withPermits(1),
    Effect.withSpan("SecretRequests.answer"),
  );

  const saved: SecretRequests["Service"]["saved"] = Effect.fn("SecretRequests.saved")(
    function* (input) {
      const ref = yield* refFor(input.threadId, input.turnItemId);
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
    },
  );

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

  const pastedFilesDir =
    store.directory === undefined ? undefined : path.join(store.directory, "files");
  const writeSecretFile = (dir: string, file: string, value: string) => {
    const partial = `${file}.partial`;
    return fileSystem.makeDirectory(dir, { recursive: true, mode: 0o700 }).pipe(
      Effect.andThen(fileSystem.chmod(dir, 0o700)),
      Effect.andThen(fileSystem.remove(partial, { force: true })),
      // `wx` never follows or replaces an existing path.
      Effect.andThen(
        fileSystem.writeFile(partial, new TextEncoder().encode(value), {
          flag: "wx",
          mode: 0o600,
        }),
      ),
      Effect.andThen(fileSystem.rename(partial, file)),
      Effect.mapError((cause) => new SecretRequestError({ reason: "store_failed", cause })),
    );
  };

  const savePasted: SecretRequests["Service"]["savePasted"] = Effect.fn(
    "SecretRequests.savePasted",
  )(function* (input) {
    if (store.directory === undefined)
      return yield* new SecretRequestError({ reason: "store_failed" });
    const ref = yield* refFor(
      input.threadId,
      `pasted\u0000${input.requestNamespace}\u0000${input.requestKey}`,
    );
    const file = path.join(pastedFilesDir!, ref.slice(SECRET_REF_PREFIX.length));
    // A retry with the same clientRequestId gets the file the first call wrote, unchanged, so a
    // command already holding that path never reads another value.
    return yield* Effect.gen(function* () {
      if (yield* fileSystem.exists(file).pipe(Effect.orElseSucceed(() => false))) return file;
      yield* writeSecretFile(pastedFilesDir!, file, input.value);
      return file;
    }).pipe(consumeLock.withPermits(1));
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
  const sweepPastedFiles = Effect.gen(function* () {
    if (pastedFilesDir === undefined || !(yield* fileSystem.exists(pastedFilesDir))) return;
    const now = yield* Clock.currentTimeMillis;
    for (const name of yield* fileSystem.readDirectory(pastedFilesDir)) {
      if (!/^[0-9a-f]{32}(?:\.tmp)?$/.test(name)) continue;
      const file = path.join(pastedFilesDir, name);
      yield* fileSystem.stat(file).pipe(
        Effect.flatMap((info) =>
          Option.isSome(info.mtime) && now - info.mtime.value.getTime() > SECRET_REF_TTL_MS
            ? fileSystem.remove(file, { force: true })
            : Effect.void,
        ),
        Effect.catchCause((cause) =>
          Effect.logWarning("Could not sweep a pasted secret file", cause),
        ),
      );
    }
  });
  const sweep = Effect.andThen(Effect.andThen(sweepExpired, sweepExpiredFiles), sweepPastedFiles);
  yield* sweep;
  yield* sweep.pipe(
    Effect.delay("1 hour"),
    Effect.repeat(Schedule.spaced("1 hour")),
    Effect.forkScoped,
  );

  return SecretRequests.of({
    answer,
    saved,
    consume,
    savePasted,
    saveToFile: Effect.fn("SecretRequests.saveToFile")((input) =>
      saved(input).pipe(
        Effect.flatMap((result) =>
          Option.isSome(result) && result.value.files.secret !== undefined
            ? Effect.succeed(result.value.files.secret)
            : Effect.fail(new SecretRequestError({ reason: "store_failed" })),
        ),
      ),
    ),
  });
});

export const layer = Layer.effect(SecretRequests, make);
