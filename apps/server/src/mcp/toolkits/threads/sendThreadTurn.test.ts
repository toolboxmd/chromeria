// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  TurnId,
  type ProviderSendTurnInput,
  type ProviderSession,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import { ProviderCommandReactor } from "../../../orchestration/Services/ProviderCommandReactor.ts";
import {
  callTool,
  createParent,
  NOW,
  temporaryDirectory,
  withServer,
} from "./handlers.testFixtures.ts";

// The engine, projections and provider command reactor are real; only the
// provider boundary is faked. Codex reads turn effort only from the turn's
// modelSelection, so a server-origin turn without it runs at Codex's default.
describe("server-origin turns", () => {
  it.live("a spawned Codex child's turn reaches the provider with its stored effort", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("t3-turn-effort-");
      const sent: ProviderSendTurnInput[] = [];
      const sessions: ProviderSession[] = [];
      const sentReceipt = yield* Deferred.make<void>();
      yield* withServer(
        NodePath.join(directory, "state.sqlite"),
        Effect.gen(function* () {
          yield* createParent(directory);
          const reactor = yield* ProviderCommandReactor;
          yield* reactor.start();
          yield* callTool("spawn_thread", {
            task: "Do the work.",
            model: "gpt-5",
            effort: "xhigh",
            reportBack: false,
          });
          yield* Deferred.await(sentReceipt);
          yield* reactor.drain;
          expect(sent).toHaveLength(1);
          expect(sent[0]?.modelSelection).toEqual({
            instanceId: "codex",
            model: "gpt-5",
            options: [{ id: "reasoningEffort", value: "xhigh" }],
          });
        }),
        undefined,
        {
          provider: {
            listSessions: () => Effect.succeed(sessions),
            getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
            startSession: (threadId, input) =>
              Effect.sync(() => {
                const value: ProviderSession = {
                  threadId,
                  provider: ProviderDriverKind.make("codex"),
                  providerInstanceId: ProviderInstanceId.make("codex"),
                  runtimeMode: input.runtimeMode,
                  status: "ready",
                  model: input.modelSelection?.model,
                  createdAt: NOW,
                  updatedAt: NOW,
                };
                sessions.push(value);
                return value;
              }),
            sendTurn: (input) =>
              Effect.sync(() => sent.push(input)).pipe(
                Effect.andThen(Deferred.succeed(sentReceipt, undefined)),
                Effect.as({ threadId: input.threadId, turnId: TurnId.make("effort-turn") }),
              ),
          },
        },
      );
    }).pipe(Effect.scoped),
  );
});
