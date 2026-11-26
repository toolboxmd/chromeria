import { describe, expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProviderDriverKind,
  ProviderInstanceId,
  UsageLimitSourceId,
  type ProviderClearRedeemedResetResult,
  type ProviderConsumeResetCreditOutcome,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { Atom, AtomRegistry } from "effect/reactivity";
import * as TestClock from "effect/testing/TestClock";
import { EnvironmentRpcUnavailableError } from "../rpc/client.ts";
import { redeemResetCreditCommand, type RedeemedResetEnvironment } from "./redeemedReset.ts";

const bestia = EnvironmentId.make("bestia");
const rocky = EnvironmentId.make("rocky");
const cavallo = EnvironmentId.make("cavallo");

const provider = (
  instanceId: string,
  auth: { readonly email?: string; readonly workspaceId?: string },
  extra: Partial<ServerProvider> = {},
) =>
  ({
    instanceId: ProviderInstanceId.make(instanceId),
    driver: ProviderDriverKind.make("claudeAgent"),
    enabled: true,
    auth: { status: "authenticated", ...auth },
    ...extra,
  }) as ServerProvider;

const login = { email: "owner@example.com", workspaceId: "org-1" };
const environment = (
  label: string,
  providers: ReadonlyArray<ServerProvider>,
): RedeemedResetEnvironment => ({ entry: { target: { label } }, serverConfig: { providers } });

const environments = new Map([
  [
    bestia,
    environment("bestia", [
      provider("claude", login),
      provider("claude-work", login, { displayName: "Claude work" }),
      provider("claude-other", { ...login, workspaceId: "org-2" }),
    ]),
  ],
  [
    rocky,
    environment("rocky", [
      provider("claude", { ...login, email: "Owner@Example.com" }),
      provider("claude-no-workspace", { email: login.email }),
      provider("claude-no-email", { workspaceId: login.workspaceId }),
      provider("claude-disabled", login, { enabled: false }),
      provider("codex", login, { driver: ProviderDriverKind.make("codex") }),
      provider("claude-elsewhere", { ...login, email: "other@example.com" }),
    ]),
  ],
  [cavallo, environment("cavallo", [provider("claude", login)])],
]);

type Answer = ProviderClearRedeemedResetResult | "unreachable" | "hangs";

const setup = (
  outcome: ProviderConsumeResetCreditOutcome,
  answers: Partial<Record<string, Answer>> = {},
) => {
  const calls: Array<string> = [];
  const command = redeemResetCreditCommand({
    consume: { label: "consume", run: () => Effect.succeed({ outcome }) },
    clear: {
      label: "clear",
      run: (_registry, { environmentId, input }) => {
        calls.push(`${environmentId}/${input.instanceId}`);
        const answer = answers[environmentId] ?? { voided: 1, usage: "headroom" };
        return answer === "unreachable"
          ? Effect.fail(
              new EnvironmentRpcUnavailableError({
                environmentId,
                message: `${environmentId} is not connected.`,
              }),
            )
          : answer === "hangs"
            ? Effect.never
            : Effect.succeed(answer);
      },
    },
    environmentsAtom: Atom.make(environments),
  });
  const registry = AtomRegistry.make();
  return { calls, run: (input: Parameters<typeof command.run>[1]) => command.run(registry, input) };
};

const redeemClaude = {
  environmentId: bestia,
  input: { instanceId: ProviderInstanceId.make("claude") },
};

describe("redeemResetCreditCommand", () => {
  it.effect(
    "tells only the other enabled instances with the same driver, email and workspace",
    () =>
      Effect.gen(function* () {
        const { calls, run } = setup("reset", {
          rocky: { voided: 3, usage: "exhausted" },
          cavallo: { voided: 0, usage: "unread" },
        });
        const result = yield* run(redeemClaude);
        expect(calls.toSorted()).toEqual(["bestia/claude-work", "cavallo/claude", "rocky/claude"]);
        expect(result).toEqual({
          outcome: "reset",
          machines: [
            {
              environmentId: bestia,
              instanceId: "claude-work",
              label: "bestia · Claude work",
              reach: "cleared",
            },
            { environmentId: rocky, instanceId: "claude", label: "rocky", reach: "exhausted" },
            { environmentId: cavallo, instanceId: "claude", label: "cavallo", reach: "unread" },
          ],
        });
      }),
  );

  it.effect("tells nobody when the redeemed instance has no email or workspace", () =>
    Effect.gen(function* () {
      const { calls, run } = setup("reset");
      const result = yield* run({
        environmentId: rocky,
        input: { instanceId: ProviderInstanceId.make("claude-no-workspace") },
      });
      expect(calls).toEqual([]);
      expect(result.machines).toEqual([]);
    }),
  );

  it.effect.each(["nothingToReset", "noCredit", "alreadyRedeemed"] as const)(
    "tells nobody after outcome %s",
    (outcome) =>
      Effect.gen(function* () {
        const { calls, run } = setup(outcome);
        expect(yield* run(redeemClaude)).toEqual({ outcome, machines: [] });
        expect(calls).toEqual([]);
      }),
  );

  it.effect("tells nobody after a hub redemption", () =>
    Effect.gen(function* () {
      const { calls, run } = setup("reset");
      const result = yield* run({
        environmentId: bestia,
        input: { sourceId: UsageLimitSourceId.make("hub"), accountId: "claude", creditId: "c1" },
      });
      expect(result).toEqual({ outcome: "reset", machines: [] });
      expect(calls).toEqual([]);
    }),
  );

  it.effect(
    "reports a machine that fails or does not answer in time without failing the reset",
    () =>
      Effect.gen(function* () {
        const { run } = setup("reset", { rocky: "unreachable", cavallo: "hangs" });
        const fiber = yield* run(redeemClaude).pipe(Effect.forkChild);
        yield* TestClock.adjust("10 seconds");
        const result = yield* Fiber.join(fiber);
        expect(result.outcome).toBe("reset");
        expect(result.machines.map(({ label, reach }) => [label, reach])).toEqual([
          ["bestia · Claude work", "cleared"],
          ["rocky", "notReached"],
          ["cavallo", "notReached"],
        ]);
      }),
  );
});
