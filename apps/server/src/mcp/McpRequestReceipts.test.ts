import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Config from "../config.ts";
import * as Receipts from "./McpRequestReceipts.ts";
import type { McpInvocationScope } from "./McpInvocationContext.ts";

const scope: McpInvocationScope = {
  environmentId: EnvironmentId.make("receipts-a"),
  requestNamespace: "rotating-credential",
  issuedAt: 0,
  thread: {
    threadId: ThreadId.make("caller"),
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerSessionId: "first",
  },
  client: undefined,
  capabilities: new Set(["orchestration"]),
};
const database = Sqlite.layerMemory.pipe(
  Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "t3-request-receipts-" })),
  Layer.provideMerge(NodeServices.layer),
);
const reconstruct = Layer.build(Receipts.layer).pipe(
  Effect.map((context) => Context.get(context, Receipts.McpRequestReceipts)),
);

it.effect(
  "binds canonical intent across renewal/reconstruction and denies changed input or grant",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* reconstruct;
        const identity = yield* original.identify(
          scope,
          "send",
          { text: "once", target: "b" },
          "key",
        );
        const rebuilt = yield* reconstruct;
        const renewed = {
          ...scope,
          requestNamespace: "new-credential",
          thread: { ...scope.thread!, providerSessionId: "renewed" },
        };
        expect(
          yield* rebuilt.identify(renewed, "send", { target: "b", text: "once" }, "key"),
        ).toEqual(identity);
        const conflict = yield* Effect.result(
          rebuilt.identify(renewed, "send", { text: "changed", target: "b" }, "key"),
        );
        expect(conflict._tag).toBe("Failure");
        if (conflict._tag === "Failure") expect(conflict.failure.code).toBe("invalid_request");
        const replaced = yield* Effect.result(
          rebuilt.identify(
            {
              ...renewed,
              client: {
                sessionId: "replacement",
                label: "Peer",
                access: "full-access",
                linked: true,
              },
            },
            "send",
            { text: "once", target: "b" },
            "key",
          ),
        );
        expect(replaced._tag).toBe("Failure");
        if (replaced._tag === "Failure") expect(replaced.failure.code).toBe("capability_denied");
      }),
    ).pipe(Effect.provide(database)),
);

it.effect("persists started before external dispatch and never repeats an ambiguous effect", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const receipts = yield* reconstruct;
      const identity = yield* receipts.identify(
        scope,
        "preview_click",
        { locator: "button" },
        "click-once",
      );
      let actions = 0;
      const first = yield* Effect.result(
        receipts.external(
          identity,
          Effect.gen(function* () {
            expect(
              (yield* sql<{
                state: string;
              }>`SELECT state FROM mcp_request_receipts WHERE id=${identity.id}`)[0]?.state,
            ).toBe("started");
            actions++;
            return yield* Effect.fail("accepted reply lost");
          }),
        ),
      );
      expect(first._tag).toBe("Failure");
      const rebuilt = yield* reconstruct;
      const replay = yield* Effect.result(
        rebuilt.external(
          identity,
          Effect.sync(() => ++actions),
        ),
      );
      expect(replay._tag).toBe("Failure");
      if (replay._tag === "Failure")
        expect(replay.failure).toMatchObject({
          code: "outcome_unknown",
          requestId: "click-once",
          lastConfirmedStage: "dispatch_started",
        });
      expect(actions).toBe(1);
    }),
  ).pipe(Effect.provide(database)),
);

it.effect("arbitrates concurrent external calls and replays only a confirmed bounded receipt", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const receipts = yield* reconstruct;
      const identity = yield* receipts.identify(
        scope,
        "device_open",
        { deviceId: "simulator" },
        "open-once",
      );
      const entered = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<void>();
      let actions = 0;
      const first = yield* receipts
        .external(
          identity,
          Effect.gen(function* () {
            actions++;
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(finish);
            return { deviceId: "simulator", opened: true };
          }),
        )
        .pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      const concurrent = yield* Effect.result(
        receipts.external(
          identity,
          Effect.sync(() => ++actions),
        ),
      );
      expect(concurrent._tag).toBe("Failure");
      yield* Deferred.succeed(finish, undefined);
      expect(yield* Fiber.join(first)).toEqual({ deviceId: "simulator", opened: true });
      const rebuilt = yield* reconstruct;
      expect(
        yield* rebuilt.external(
          identity,
          Effect.sync(() => {
            actions++;
            return { deviceId: "wrong" };
          }),
        ),
      ).toEqual({ deviceId: "simulator", opened: true });
      expect(actions).toBe(1);
    }),
  ).pipe(Effect.provide(database)),
);

it.effect("fails closed for a registered external operation without its receipt service", () =>
  Effect.gen(function* () {
    let actions = 0;
    const result = yield* Effect.result(
      Receipts.externalDispatch(Effect.sync(() => ++actions)).pipe(
        Effect.provideService(Receipts.CurrentOperation, { tool: "preview_click", input: {} }),
      ),
    );
    expect(result._tag).toBe("Failure");
    expect(actions).toBe(0);
  }),
);

it.effect(
  "refuses imported legacy keys without guessing ownership or rewriting accepted receipts",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const legacyId = "mcp:launch:client%3Aold-grant:imported-key";
        yield* sql`INSERT INTO orchestration_command_receipts (command_id,aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status,error)
      VALUES (${legacyId},'thread','original-thread','thread.create','2026-01-01T00:00:00.000Z',1,'accepted',NULL)`;
        const receipts = yield* reconstruct;
        const result = yield* Effect.result(
          receipts.identify(
            scope,
            "t3_thread_launch",
            { message: "same key cannot prove same intent" },
            "imported-key",
          ),
        );
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") expect(result.failure.message).toContain("legacy operation");
        expect(yield* sql`SELECT id FROM mcp_request_receipts`).toHaveLength(0);
        expect(
          (yield* sql<{
            aggregate_id: string;
          }>`SELECT aggregate_id FROM orchestration_command_receipts WHERE command_id=${legacyId}`)[0]
            ?.aggregate_id,
        ).toBe("original-thread");
        const fresh = yield* receipts.identify(
          scope,
          "t3_thread_launch",
          { message: "independent new intent" },
          "fresh-key",
        );
        expect(fresh.clientRequestId).toBe("fresh-key");
      }),
    ).pipe(Effect.provide(database)),
);

it("canonical request keys use locale-independent UTF-16 order", () => {
  expect(Receipts.canonicalRequest({ ą: 3, a: 2, Z: 1 })).toBe('{"Z":1,"a":2,"ą":3}');
});
