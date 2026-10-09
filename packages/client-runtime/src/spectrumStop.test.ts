import {
  CommandId,
  EnvironmentId,
  ORCHESTRATION_V2_WS_METHODS,
  RunId,
  SPECTRUM_WS_METHODS,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";

import {
  AVAILABLE_CONNECTION_STATE,
  PrimaryConnectionTarget,
  type PreparedConnection,
} from "./connection/model.ts";
import * as EnvironmentSupervisor from "./connection/supervisor.ts";
import { interruptThreadTurn } from "./operations/commands.ts";
import type { WsRpcProtocolClient } from "./rpc/protocol.ts";
import type * as RpcSession from "./rpc/session.ts";
import { presentThreadShell } from "./state/models.ts";
import {
  v2Projection,
  v2ShellSnapshot,
  v2ThreadId,
  v2ThreadShell,
} from "./state/orchestrationV2TestFixtures.ts";
import { applyShellStreamEvent } from "./state/shellReducer.ts";
import { isForkSpectrumRunning, threadStopInput } from "./spectrumStop.ts";

const environmentId = EnvironmentId.make("environment-1");
const shell = (forkSpectrumRunning?: boolean) =>
  presentThreadShell(environmentId, {
    ...v2ThreadShell,
    ...(forkSpectrumRunning === undefined ? {} : { forkSpectrumRunning }),
  });
const runId = RunId.make("run-1");

describe("Spectrum Stop reachability", () => {
  it("reads the server's flag through the presented shell", () => {
    expect(isForkSpectrumRunning(shell(true))).toBe(true);
    expect(isForkSpectrumRunning(shell(false))).toBe(false);
    expect(isForkSpectrumRunning(shell())).toBe(false);
    expect(isForkSpectrumRunning(null)).toBe(false);
  });

  it("follows live shell updates that change only the flag", () => {
    const update = (sequence: number, forkSpectrumRunning: boolean) => ({
      kind: "thread.updated" as const,
      sequence,
      location: "active" as const,
      thread: { ...v2ThreadShell, forkSpectrumRunning },
    });
    const flagOf = (snapshot: typeof v2ShellSnapshot) =>
      snapshot.threads.some((thread) =>
        isForkSpectrumRunning(presentThreadShell(environmentId, thread)),
      );

    const started = applyShellStreamEvent(v2ShellSnapshot, update(1, true));
    expect(flagOf(started)).toBe(true);
    const stopped = applyShellStreamEvent(started, update(2, false));
    expect(flagOf(stopped)).toBe(false);
  });

  it("stops a Spectrum thread with no interruptible run", () => {
    expect(threadStopInput(shell(true), null)).toEqual({
      threadId: v2ThreadId,
      forkSpectrumRunning: true,
    });
  });

  it("stops the whole Spectrum thread even while a run is interruptible", () => {
    expect(threadStopInput(shell(true), runId)).toEqual({
      threadId: v2ThreadId,
      forkSpectrumRunning: true,
    });
  });

  it("leaves other threads on their interruptible run, and offers nothing without one", () => {
    expect(threadStopInput(shell(), runId)).toEqual({ threadId: v2ThreadId, runId });
    expect(threadStopInput(shell(false), null)).toBeNull();
  });
});

const layerTestCrypto = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => new Uint8Array(size),
    digest: (_algorithm, data) => Effect.succeed(data),
  }),
);

/** A connected environment that records every request it serves, in order. */
const makeSupervisor = Effect.fn("TestSpectrumStop.makeSupervisor")(function* (
  calls: Array<readonly [method: string, input: unknown]>,
) {
  const serve =
    <A>(method: string, reply: A) =>
    (input: unknown) =>
      Effect.sync(() => {
        calls.push([method, input]);
        return reply;
      });
  const client = {
    [ORCHESTRATION_V2_WS_METHODS.dispatchCommand]: serve(
      ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
      { sequence: 1 },
    ),
    [ORCHESTRATION_V2_WS_METHODS.getThreadProjection]: serve(
      ORCHESTRATION_V2_WS_METHODS.getThreadProjection,
      v2Projection,
    ),
    [SPECTRUM_WS_METHODS.stop]: serve(SPECTRUM_WS_METHODS.stop, { sequence: 7 }),
  } as unknown as WsRpcProtocolClient;
  const session: RpcSession.RpcSession = {
    client,
    initialConfig: Effect.succeed({ environment: { capabilities: {} } } as never),
    subscribeServerConfig: (subscription) => client.subscribeServerConfig(subscription),
    ready: Effect.void,
    probe: Effect.void,
    closed: Effect.never,
  };
  return EnvironmentSupervisor.EnvironmentSupervisor.of({
    target: new PrimaryConnectionTarget({
      environmentId,
      label: "Test environment",
      httpBaseUrl: "https://environment.example.test",
      wsBaseUrl: "wss://environment.example.test",
    }),
    state: yield* SubscriptionRef.make(AVAILABLE_CONNECTION_STATE),
    session: yield* SubscriptionRef.make(Option.some(session)),
    prepared: yield* SubscriptionRef.make(Option.none<PreparedConnection>()),
    connect: Effect.void,
    disconnect: Effect.void,
    retryNow: Effect.void,
  } satisfies EnvironmentSupervisor.EnvironmentSupervisor["Service"]);
});

describe("interruptThreadTurn with a running Spectrum", () => {
  it.effect("asks the server to stop the thread, with no run to resolve or interrupt", () =>
    Effect.gen(function* () {
      const calls: Array<readonly [string, unknown]> = [];
      const supervisor = yield* makeSupervisor(calls);

      const result = yield* interruptThreadTurn({
        threadId: v2ThreadId,
        forkSpectrumRunning: true,
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(result).toEqual({ sequence: 7 });
      expect(calls).toEqual([
        [
          SPECTRUM_WS_METHODS.stop,
          { threadId: v2ThreadId, commandId: "00000000-0000-4000-8000-000000000000" },
        ],
      ]);
    }).pipe(Effect.provide(layerTestCrypto)),
  );

  it.effect("stops the whole thread under the caller's command id, even with a run id", () =>
    Effect.gen(function* () {
      const calls: Array<readonly [string, unknown]> = [];
      const supervisor = yield* makeSupervisor(calls);
      const commandId = CommandId.make("stop-command-1");

      yield* interruptThreadTurn({
        threadId: v2ThreadId,
        commandId,
        runId,
        forkSpectrumRunning: true,
      }).pipe(Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor));

      expect(calls).toEqual([[SPECTRUM_WS_METHODS.stop, { threadId: v2ThreadId, commandId }]]);
    }).pipe(Effect.provide(layerTestCrypto)),
  );

  it.effect("keeps run.interrupt for a thread without a running Spectrum", () =>
    Effect.gen(function* () {
      const calls: Array<readonly [string, unknown]> = [];
      const supervisor = yield* makeSupervisor(calls);

      yield* interruptThreadTurn({ threadId: v2ThreadId, runId, forkSpectrumRunning: false }).pipe(
        Effect.provideService(EnvironmentSupervisor.EnvironmentSupervisor, supervisor),
      );

      expect(calls).toEqual([
        [
          ORCHESTRATION_V2_WS_METHODS.dispatchCommand,
          {
            type: "run.interrupt",
            commandId: "00000000-0000-4000-8000-000000000000",
            threadId: v2ThreadId,
            runId,
            holdQueue: true,
          },
        ],
      ]);
    }).pipe(Effect.provide(layerTestCrypto)),
  );
});
