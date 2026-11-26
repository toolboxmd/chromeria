import { AuthRenewalLocalRecord } from "@t3tools/contracts";
import { renewalCommitment } from "@t3tools/shared/sessionRenewal";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Base64 from "effect/encoding/Base64";
import * as DesktopEnvironment from "./DesktopEnvironment.ts";
import * as ElectronSafeStorage from "../electron/ElectronSafeStorage.ts";

export class DesktopRenewalStorageError extends Schema.TaggedError<DesktopRenewalStorageError>()(
  "DesktopRenewalStorageError",
  { cause: Schema.Defect() },
) {}
export class DesktopRenewalStore extends Context.Service<
  DesktopRenewalStore,
  {
    readonly read: (
      key: string,
    ) => Effect.Effect<AuthRenewalLocalRecord | null, DesktopRenewalStorageError>;
    readonly compareAndSet: (input: {
      key: string;
      revision: number | null;
      value: AuthRenewalLocalRecord;
    }) => Effect.Effect<boolean, DesktopRenewalStorageError>;
  }
>()("@t3tools/desktop/app/DesktopRenewalStore") {}

const CommitmentEntries = Schema.Array(
  Schema.Struct({
    key: Schema.String,
    environmentId: Schema.String,
    origin: Schema.String,
    epoch: Schema.String,
    commitment: Schema.String,
  }),
);
const Commitments = Schema.Struct({ stateDir: Schema.String, entries: CommitmentEntries });
const RenewalRecordJson = Schema.fromJsonString(AuthRenewalLocalRecord);
const decodeRenewalRecord = Schema.decodeEffect(RenewalRecordJson);
const encodeRenewalRecord = Schema.encodeEffect(RenewalRecordJson);
const CommitmentsJson = Schema.fromJsonString(Commitments);
const decodeCommitments = Schema.decodeEffect(CommitmentsJson);
const encodeCommitments = Schema.encodeEffect(CommitmentsJson);
const make = Effect.gen(function* () {
  const { stateDir, path } = yield* DesktopEnvironment.DesktopEnvironment;
  const fs = yield* FileSystem.FileSystem;
  const safe = yield* ElectronSafeStorage.ElectronSafeStorage;
  const lock = yield* Semaphore.make(1);
  const directory = path.join(stateDir, "session-renewal");
  const publicPath = path.join(stateDir, "renewal-commitments.json");
  const read = Effect.fn("DesktopRenewalStore.read")(
    function* (key: string) {
      const file = path.join(directory, `${renewalCommitment(key)}.json`);
      if (!(yield* fs.exists(file))) return null;
      const encrypted = yield* fs.readFileString(file);
      const bytes = yield* Effect.fromResult(Base64.decode(encrypted));
      return yield* decodeRenewalRecord(yield* safe.decryptString(bytes));
    },
    Effect.mapError((cause) => new DesktopRenewalStorageError({ cause })),
  );
  const compareAndSet = (input: {
    key: string;
    revision: number | null;
    value: AuthRenewalLocalRecord;
  }) =>
    lock.withPermit(
      Effect.gen(function* () {
        const old = yield* read(input.key);
        if ((old?.revision ?? null) !== input.revision) return false;
        if (!(yield* safe.isEncryptionAvailable))
          return yield* new DesktopRenewalStorageError({ cause: "encryption unavailable" });
        const raw = yield* encodeRenewalRecord(input.value);
        const bytes = yield* safe.encryptString(raw);
        yield* fs.makeDirectory(directory, { recursive: true, mode: 0o700 });
        const file = path.join(directory, `${renewalCommitment(input.key)}.json`);
        const temp = `${file}.tmp`;
        yield* fs.writeFileString(temp, Base64.encode(bytes), { mode: 0o600 });
        yield* fs.rename(temp, file);
        const existing = (yield* fs.exists(publicPath))
          ? yield* decodeCommitments(yield* fs.readFileString(publicPath))
          : { stateDir, entries: [] };
        const slots = [
          ...input.value.provisional,
          ...(input.value.active ? [input.value.active] : []),
        ];
        const commitments = [
          ...existing.entries.filter((row) => row.key !== input.key),
          ...slots.map((slot) => ({
            key: input.key,
            environmentId: slot.environmentId,
            origin: slot.origin,
            epoch: slot.epoch,
            commitment: renewalCommitment(slot.current),
          })),
        ];
        yield* fs.writeFileString(
          `${publicPath}.tmp`,
          yield* encodeCommitments({
            stateDir,
            entries: commitments,
          }),
          { mode: 0o600 },
        );
        yield* fs.rename(`${publicPath}.tmp`, publicPath);
        return true;
      }).pipe(Effect.mapError((cause) => new DesktopRenewalStorageError({ cause }))),
    );
  return DesktopRenewalStore.of({ read, compareAndSet });
});
export const layer = Layer.effect(DesktopRenewalStore, make);
