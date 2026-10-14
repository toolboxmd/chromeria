import { ORCHESTRATION_PROTOCOL_VERSION, type AuthRenewalLocalRecord } from "@t3tools/contracts";
import { renewalCommitment } from "@t3tools/shared/sessionRenewal";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Renewal from "./renewal.ts";
import * as Records from "../connection/renewalRecordStore.ts";
import { layerRemoteHttpClient } from "../rpc/http.ts";

const origin = "http://remote.test";
const descriptor = {
  environmentId: "paired",
  label: "Paired",
  platform: { os: "linux", arch: "x64" },
  serverVersion: "0.0.0-test",
  orchestrationProtocolVersion: ORCHESTRATION_PROTOCOL_VERSION,
  capabilities: { repositoryIdentity: true },
};
const decodeRequest = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Struct({ secret: Schema.String, next: Schema.String })),
);

function fixture(reply: (body: { secret: string; next: string }) => Response) {
  let record: AuthRenewalLocalRecord | null = null;
  let counter = 0;
  const requests: Array<{ secret: string; next: string }> = [];
  const storage = Layer.succeed(Records.RenewalRecordStore, {
    read: () => Effect.sync(() => record),
    compareAndSet: (_key, revision, value) =>
      Effect.sync(() => {
        if ((record?.revision ?? null) !== revision) return false;
        record = value;
        return true;
      }),
  });
  const http = layerRemoteHttpClient((input, init) => {
    if (String(input).endsWith("/.well-known/t3/environment"))
      return Promise.resolve(Response.json(descriptor));
    const raw = init?.body;
    const body = decodeRequest(
      raw instanceof Uint8Array ? new TextDecoder().decode(raw) : String(raw),
    );
    requests.push(body);
    return Promise.resolve(reply(body));
  });
  const crypto = Layer.succeed(
    Crypto.Crypto,
    Crypto.make({
      randomBytes: (size) => new Uint8Array(size).fill(++counter),
      digest: (_algorithm, data) => Effect.succeed(data),
    }),
  );
  const layer = Renewal.layer.pipe(Layer.provide(Layer.mergeAll(storage, http, crypto)));
  return { layer, requests, read: () => record };
}

it.effect(
  "independent client instances share one durable successor and survive duplicate responses",
  () =>
    Effect.gen(function* () {
      const f = fixture(() =>
        Response.json({
          outcome: "rotated",
          generation: 1,
          expiresAt: 2000,
          renewAfter: 1500,
          accessToken: "renewed",
        }),
      );
      const first = Context.get(yield* Layer.build(Layer.fresh(f.layer)), Renewal.SessionRenewal);
      const second = Context.get(yield* Layer.build(Layer.fresh(f.layer)), Renewal.SessionRenewal);
      const slot = yield* first.stage({
        key: "paired",
        environmentId: "paired",
        origin,
        kind: "bearer",
      });
      yield* first.complete("paired", slot.epoch, {
        accessToken: "old",
        expiresAt: 1000,
        renewAfter: 500,
      });
      const results = yield* Effect.all([first.ensure("paired"), second.ensure("paired")], {
        concurrency: "unbounded",
      });
      expect(results.map((result) => result._tag)).toEqual(["Renewed", "Renewed"]);
      expect(f.requests).toHaveLength(2);
      expect(f.requests[0]).toEqual(f.requests[1]);
      expect(f.read()?.active).toMatchObject({
        generation: 1,
        pending: null,
        accessToken: "renewed",
      });
      expect(renewalCommitment(f.read()!.active!.current)).toBe(f.requests[0]!.next);
    }).pipe(Effect.scoped),
);

it.effect(
  "retains the provisional secret and successor through pending approval and not-due recovery",
  () =>
    Effect.gen(function* () {
      let pending = true;
      const f = fixture(() =>
        pending
          ? Response.json(
              {
                _tag: "EnvironmentOperationForbiddenError",
                code: "operation_forbidden",
                reason: "renewal_pending_approval",
                traceId: "test",
              },
              { status: 403 },
            )
          : Response.json({
              outcome: "current",
              generation: 0,
              expiresAt: 1000,
              renewAfter: 500,
              accessToken: "recovered",
            }),
      );
      const first = Context.get(yield* Layer.build(Layer.fresh(f.layer)), Renewal.SessionRenewal);
      const slot = yield* first.stage({
        key: "paired",
        environmentId: "paired",
        origin,
        kind: "bearer",
      });
      expect((yield* first.ensure("paired"))._tag).toBe("Pending");
      const saved = f.read()?.provisional[0];
      expect(saved?.current).toBe(slot.current);
      expect(saved?.pending).not.toBeNull();
      pending = false;
      const restarted = Context.get(
        yield* Layer.build(Layer.fresh(f.layer)),
        Renewal.SessionRenewal,
      );
      expect((yield* restarted.ensure("paired"))._tag).toBe("Renewed");
      expect(f.requests[1]).toEqual(f.requests[0]);
      expect(f.read()?.active).toMatchObject({
        current: slot.current,
        generation: 0,
        pending: saved?.pending,
        accessToken: "recovered",
      });
    }).pipe(Effect.scoped),
);
