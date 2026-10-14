import {
  AuthRenewalLocalSlot,
  type AuthRenewalLocalRecord,
  type AuthSessionRenewResult,
  EnvironmentAuthInvalidError,
  EnvironmentOperationForbiddenError,
  EnvironmentHttpConflictError,
} from "@t3tools/contracts";
import { renewalCommitment } from "@t3tools/shared/sessionRenewal";
import * as Base64Url from "effect/encoding/Base64Url";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as Duration from "effect/Duration";
import * as Scope from "effect/Scope";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientError from "effect/http/HttpClientError";
import { fetchRemoteEnvironmentDescriptor } from "../environment/descriptor.ts";
import { makeEnvironmentHttpApiGroupClient } from "../rpc/http.ts";
import * as RenewalRecords from "../connection/renewalRecordStore.ts";
import { ConnectionTransientError } from "../connection/model.ts";

export class RenewalRejected extends Schema.TaggedError<RenewalRejected>()("RenewalRejected", {}) {}
export type RenewalOutcome =
  | { readonly _tag: "Renewed"; readonly result: AuthSessionRenewResult }
  | { readonly _tag: "NotEnrolled" | "Pending" | "Unsupported" };
type Slot = typeof AuthRenewalLocalSlot.Type;

export class SessionRenewal extends Context.Service<
  SessionRenewal,
  {
    readonly stage: (input: {
      key: string;
      environmentId: string;
      origin: string;
      kind: "bearer" | "cookie";
      purpose?: "pairing" | "legacy";
    }) => Effect.Effect<Slot, ConnectionTransientError>;
    readonly complete: (
      key: string,
      epoch: string,
      access: { accessToken?: string; expiresAt: number; renewAfter?: number },
    ) => Effect.Effect<void, ConnectionTransientError>;
    readonly ensure: (
      key: string,
      origin?: string,
    ) => Effect.Effect<RenewalOutcome, RenewalRejected | ConnectionTransientError>;
    readonly enrollExisting: (input: {
      key: string;
      environmentId: string;
      origin: string;
      kind: "bearer" | "cookie";
      bearerToken?: string;
    }) => Effect.Effect<void, ConnectionTransientError>;
    readonly read: RenewalRecords.RenewalRecordStore["Service"]["read"];
  }
>()("@t3tools/client-runtime/authorization/renewal/SessionRenewal") {}

const isForbidden = Schema.is(EnvironmentOperationForbiddenError);
const isConflict = Schema.is(EnvironmentHttpConflictError);
const isInvalid = Schema.is(EnvironmentAuthInvalidError);
const empty: AuthRenewalLocalRecord = { revision: 0, active: null, provisional: [] };
const transient = (_cause: unknown) =>
  new ConnectionTransientError({
    reason: "transport",
    detail: "Session renewal is temporarily unavailable.",
  });

const make = (allowLegacyEnrollment: boolean) =>
  Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient;
    const store = yield* RenewalRecords.RenewalRecordStore;
    const crypto = yield* Crypto.Crypto;
    const scope = yield* Scope.Scope;
    const randomSecret = crypto
      .randomBytes(32)
      .pipe(Effect.map(Base64Url.encode), Effect.mapError(transient));
    const mutate = Effect.fnUntraced(function* (
      key: string,
      change: (record: AuthRenewalLocalRecord) => AuthRenewalLocalRecord,
    ) {
      while (true) {
        const current = yield* store.read(key);
        const next = change(current ?? empty);
        if (next === current || next === empty) return;
        if (
          yield* store.compareAndSet(key, current?.revision ?? null, {
            ...next,
            revision: (current?.revision ?? 0) + 1,
          })
        )
          return;
      }
    });
    const stage: SessionRenewal["Service"]["stage"] = Effect.fn("SessionRenewal.stage")(
      function* (input) {
        const secret = yield* randomSecret;
        const epoch = yield* crypto.randomUUIDv4().pipe(Effect.mapError(transient));
        const slot: Slot = {
          epoch,
          environmentId: input.environmentId,
          origin: input.origin,
          kind: input.kind,
          current: secret,
          generation: 0,
          pending: null,
          accessToken: null,
          expiresAt: null,
          renewAfter: null,
        };
        yield* mutate(input.key, (record) =>
          input.purpose === "legacy" && (record.active !== null || record.provisional.length > 0)
            ? record
            : { ...record, provisional: [...record.provisional, slot] },
        );
        if (input.purpose === "legacy") {
          const saved = yield* store.read(input.key);
          return saved?.active ?? saved?.provisional[0] ?? slot;
        }
        return slot;
      },
    );
    const complete: SessionRenewal["Service"]["complete"] = Effect.fn("SessionRenewal.complete")(
      function* (key, epoch, access) {
        yield* mutate(key, (record) => {
          const slot =
            record.active?.epoch === epoch
              ? record.active
              : record.provisional.find((candidate) => candidate.epoch === epoch);
          if (!slot) return record;
          const updated = {
            ...slot,
            ...(slot.expiresAt === null || access.expiresAt > slot.expiresAt
              ? {
                  accessToken: access.accessToken ?? null,
                  expiresAt: access.expiresAt,
                  renewAfter: access.renewAfter ?? null,
                }
              : {}),
          };
          return {
            ...record,
            active: updated,
            provisional: record.provisional.filter((candidate) => candidate.epoch !== epoch),
          };
        });
      },
    );
    const ensureOne = Effect.fn("SessionRenewal.ensureOne")(function* (
      key: string,
      overrideOrigin: string | undefined,
      epoch: string,
    ) {
      const initial = yield* store.read(key);
      const slot =
        initial?.active?.epoch === epoch
          ? initial.active
          : initial?.provisional.find((candidate) => candidate.epoch === epoch);
      if (!slot) return { _tag: "NotEnrolled" } as const;
      const nextSecret = yield* randomSecret;
      yield* mutate(key, (record) => {
        const apply = (value: Slot) =>
          value.epoch === slot.epoch && value.pending === null
            ? {
                ...value,
                pending: { secret: nextSecret, nextHash: renewalCommitment(nextSecret) },
              }
            : value;
        return {
          ...record,
          active: record.active === null ? null : apply(record.active),
          provisional: record.provisional.map(apply),
        };
      });
      const fresh = yield* store.read(key);
      const sent =
        fresh?.active?.epoch === slot.epoch
          ? fresh.active
          : fresh?.provisional.find((candidate) => candidate.epoch === slot.epoch);
      if (!sent?.pending) return { _tag: "NotEnrolled" } as const;
      const origin = overrideOrigin ?? sent.origin;
      const descriptor = yield* fetchRemoteEnvironmentDescriptor({ httpBaseUrl: origin }).pipe(
        Effect.provideService(HttpClient.HttpClient, httpClient),
        Effect.mapError(transient),
      );
      if (descriptor.environmentId !== sent.environmentId) return yield* transient(undefined);
      const client = yield* makeEnvironmentHttpApiGroupClient(origin, "auth").pipe(
        Effect.provideService(HttpClient.HttpClient, httpClient),
      );
      const response = yield* client
        .renewSession({ payload: { secret: sent.current, next: sent.pending.nextHash } })
        .pipe(
          Effect.timeoutOption("10 seconds"),
          Effect.mapError((cause) => ({ cause })),
          Effect.result,
        );
      if (response._tag === "Failure") {
        const cause = response.failure.cause;
        if (isForbidden(cause) && cause.reason === "renewal_pending_approval")
          return { _tag: "Pending" } as const;
        if (isConflict(cause)) return yield* transient(cause);
        if (HttpClientError.isHttpClientError(cause) && cause.response?.status === 404)
          return { _tag: "Unsupported" } as const;
        if (isInvalid(cause)) {
          if (initial?.active?.epoch === sent.epoch) {
            yield* mutate(key, (record) =>
              record.active?.epoch === sent.epoch ? { ...record, active: null } : record,
            );
            return yield* new RenewalRejected({});
          }
          return { _tag: "NotEnrolled" } as const;
        }
        return yield* transient(cause);
      }
      if (response.success._tag === "None") return yield* transient(undefined);
      const result = response.success.value;
      const apply = (record: AuthRenewalLocalRecord) => {
        const current =
          record.active?.epoch === sent.epoch
            ? record.active
            : record.provisional.find((candidate) => candidate.epoch === sent.epoch);
        if (!current) return record;
        let updated = current;
        const pending = current.pending;
        if (
          result.outcome === "rotated" &&
          result.generation === current.generation + 1 &&
          current.current === sent.current &&
          pending !== null &&
          pending.nextHash === sent.pending?.nextHash
        ) {
          updated = {
            ...current,
            current: pending.secret,
            generation: result.generation,
            pending: null,
          };
        }
        if (updated.expiresAt === null || result.expiresAt > updated.expiresAt) {
          updated = {
            ...updated,
            accessToken: result.accessToken ?? null,
            expiresAt: result.expiresAt,
            renewAfter: result.renewAfter,
          };
        }
        return {
          ...record,
          active: updated,
          provisional: record.provisional.filter((candidate) => candidate.epoch !== sent.epoch),
        };
      };
      // Pending is already durable; a failed final write can replay the exact request after a crash.
      yield* mutate(key, apply).pipe(
        Effect.catch(() =>
          mutate(key, apply).pipe(
            Effect.retry({
              times: 8,
              schedule: Schedule.exponential("1 second").pipe(
                Schedule.modifyDelay(({ duration }) =>
                  Effect.succeed(Duration.min(duration, Duration.seconds(60))),
                ),
              ),
            }),
            Effect.ignore,
            Effect.forkIn(scope),
            Effect.asVoid,
          ),
        ),
      );
      return { _tag: "Renewed", result } as const;
    });
    const ensure: SessionRenewal["Service"]["ensure"] = Effect.fn("SessionRenewal.ensure")(
      function* (key, overrideOrigin) {
        const record = yield* store.read(key);
        const candidates = [
          ...(record?.active ? [record.active] : []),
          ...[...(record?.provisional ?? [])].reverse(),
        ];
        let outcome: RenewalOutcome = { _tag: "NotEnrolled" };
        let failure: RenewalRejected | ConnectionTransientError | undefined;
        for (const candidate of candidates) {
          const attempt = yield* ensureOne(key, overrideOrigin, candidate.epoch).pipe(
            Effect.result,
          );
          if (attempt._tag === "Failure") {
            failure = attempt.failure;
            continue;
          }
          if (attempt.success._tag === "Renewed") return attempt.success;
          if (attempt.success._tag !== "NotEnrolled") outcome = attempt.success;
        }
        if (failure !== undefined && outcome._tag === "NotEnrolled") return yield* failure;
        return outcome;
      },
    );
    const enrollExisting: SessionRenewal["Service"]["enrollExisting"] = Effect.fn(
      "SessionRenewal.enrollExisting",
    )(function* (input) {
      if (!allowLegacyEnrollment) return;
      const saved = yield* store.read(input.key);
      if (saved?.active) return;
      const slot = saved?.provisional[0] ?? (yield* stage({ ...input, purpose: "legacy" }));
      const descriptor = yield* fetchRemoteEnvironmentDescriptor({
        httpBaseUrl: input.origin,
      }).pipe(Effect.provideService(HttpClient.HttpClient, httpClient), Effect.mapError(transient));
      if (descriptor.environmentId !== input.environmentId) return yield* transient(undefined);
      const client = yield* makeEnvironmentHttpApiGroupClient(input.origin, "auth").pipe(
        Effect.provideService(HttpClient.HttpClient, httpClient),
      );
      yield* client
        .enrollRenewal({
          payload: { next: renewalCommitment(slot.current) },
          headers: input.bearerToken ? { authorization: `Bearer ${input.bearerToken}` } : {},
        })
        .pipe(
          Effect.timeoutOption("10 seconds"),
          Effect.asVoid,
          Effect.catch(() => Effect.void),
        );
    });
    return SessionRenewal.of({ stage, complete, ensure, enrollExisting, read: store.read });
  });

export const layer = Layer.effect(SessionRenewal, make(true));
export const layerMobile = Layer.effect(SessionRenewal, make(false));
