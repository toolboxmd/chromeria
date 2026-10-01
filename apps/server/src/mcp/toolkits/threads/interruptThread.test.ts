// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";

import { ThreadId, type OrchestrationEvent } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { INTERRUPT_SETTLE_TIMEOUT, INTERRUPT_TURN_START_TIMEOUT } from "./handlers.ts";
import {
  callTool,
  createParent,
  dispatchAll,
  PARENT_ID,
  session,
  temporaryDirectory,
  withServer,
} from "./handlers.testFixtures.ts";

const interruptRequested = (threadId: string) => (event: OrchestrationEvent) =>
  event.type === "thread.turn-interrupt-requested" && event.aggregateId === threadId;

/** A parent with one child whose session is in `status`, then `body` with the child id. */
const withChild = <A, E, R>(
  prefix: string,
  status: "starting" | "running" | "ready",
  body: (child: ThreadId) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const directory = yield* temporaryDirectory(prefix);
    return yield* withServer(
      NodePath.join(directory, "state.sqlite"),
      Effect.gen(function* () {
        yield* createParent(directory);
        const spawned = yield* callTool("spawn_thread", { task: "Work.", reportBack: false });
        const child = ThreadId.make(spawned.threadId);
        yield* dispatchAll([session(child, status, status === "running" ? "turn-1" : null)]);
        return yield* body(child);
      }),
    );
  }).pipe(Effect.scoped);

/**
 * Starts interrupt_thread on `child` and returns once the interrupt command
 * landed, with the call still waiting for the turn to settle.
 */
const startInterrupt = (child: ThreadId) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    const events = yield* engine.subscribeDomainEvents;
    const call = yield* callTool("interrupt_thread", { threadId: child, scope: "children" }).pipe(
      Effect.forkScoped,
    );
    const requested = yield* events.pipe(
      Stream.filter(interruptRequested(child)),
      Stream.runHead,
      Effect.map(Option.getOrThrow),
    );
    return { call, requested };
  });

describe("interrupt_thread", () => {
  it.effect("stops a running child's turn and returns once T3 reports it settled", () =>
    withChild("t3-interrupt-running-", "running", (child) =>
      Effect.gen(function* () {
        const { call, requested } = yield* startInterrupt(child);
        expect(
          requested.type === "thread.turn-interrupt-requested" && requested.payload.turnId,
        ).toBe("turn-1");
        // Still waiting: T3 has not reported the turn settled.
        expect(call.pollUnsafe()).toBeUndefined();
        yield* dispatchAll([session(child, "interrupted", null)]);
        expect(yield* Fiber.join(call)).toEqual({
          threadId: child,
          turnId: "turn-1",
          status: "interrupted",
          statusAfter: "stopped",
        });
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("settles when the interrupted turn ends and a newer one has started", () =>
    withChild("t3-interrupt-newer-", "running", (child) =>
      Effect.gen(function* () {
        const { call } = yield* startInterrupt(child);
        // Turn 1 ended and turn 2 started before T3 handled the interrupt; the
        // reactor drops it, so turn 2 keeps running.
        yield* dispatchAll([session(child, "running", "turn-2")]);
        expect(yield* Fiber.join(call)).toEqual({
          threadId: child,
          turnId: "turn-1",
          status: "interrupted",
          statusAfter: "running",
        });
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("waits for a starting child's turn id and interrupts only that turn", () =>
    withChild("t3-interrupt-starting-", "starting", (child) =>
      Effect.gen(function* () {
        const engine = yield* OrchestrationEngineService;
        const events = yield* engine.subscribeDomainEvents;
        const call = yield* callTool("interrupt_thread", {
          threadId: child,
          scope: "children",
        }).pipe(Effect.forkScoped);
        // The turn becomes active only now; the interrupt must name it.
        yield* dispatchAll([session(child, "running", "turn-1")]);
        const requested = yield* events.pipe(
          Stream.filter(interruptRequested(child)),
          Stream.runHead,
          Effect.map(Option.getOrThrow),
        );
        expect(
          requested.type === "thread.turn-interrupt-requested" && requested.payload.turnId,
        ).toBe("turn-1");
        yield* dispatchAll([session(child, "interrupted", null)]);
        expect(yield* Fiber.join(call)).toEqual({
          threadId: child,
          turnId: "turn-1",
          status: "interrupted",
          statusAfter: "stopped",
        });
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("sends nothing when a starting child reports no turn within the wait", () =>
    withChild("t3-interrupt-no-turn-", "starting", (child) =>
      Effect.gen(function* () {
        const engine = yield* OrchestrationEngineService;
        const events = yield* engine.subscribeDomainEvents;
        const call = yield* callTool("interrupt_thread", {
          threadId: child,
          scope: "children",
        }).pipe(Effect.forkScoped);
        yield* TestClock.adjust(INTERRUPT_TURN_START_TIMEOUT);
        expect(yield* Fiber.join(call)).toEqual({
          threadId: child,
          turnId: null,
          status: "no_active_run",
          statusAfter: "starting",
        });
        // The marker lands after anything the call dispatched, in order.
        yield* dispatchAll([session(child, "running", "marker")]);
        const seen = yield* events.pipe(
          Stream.takeUntil(
            (event) =>
              event.type === "thread.session-set" &&
              event.payload.session.activeTurnId === "marker",
          ),
          Stream.runCollect,
        );
        expect(seen.some(interruptRequested(child))).toBe(false);
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("returns no_active_run when a starting child goes idle without a turn", () =>
    withChild("t3-interrupt-start-idle-", "starting", (child) =>
      Effect.gen(function* () {
        const call = yield* callTool("interrupt_thread", {
          threadId: child,
          scope: "children",
        }).pipe(Effect.forkScoped);
        yield* dispatchAll([session(child, "ready", null)]);
        expect(yield* Fiber.join(call)).toEqual({
          threadId: child,
          turnId: null,
          status: "no_active_run",
          statusAfter: "idle",
        });
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("returns an idle child unchanged without sending an interrupt", () =>
    withChild("t3-interrupt-idle-", "ready", (child) =>
      Effect.gen(function* () {
        const engine = yield* OrchestrationEngineService;
        const events = yield* engine.subscribeDomainEvents;
        const result = yield* callTool("interrupt_thread", { threadId: child, scope: "children" });
        expect(result).toEqual({
          threadId: child,
          turnId: null,
          status: "no_active_run",
          statusAfter: "idle",
        });
        // The marker lands after anything the call dispatched, in order.
        yield* dispatchAll([session(child, "running", "marker")]);
        const seen = yield* events.pipe(
          Stream.takeUntil(
            (event) =>
              event.type === "thread.session-set" &&
              event.payload.session.activeTurnId === "marker",
          ),
          Stream.runCollect,
        );
        expect(seen.some(interruptRequested(child))).toBe(false);
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("reports interrupt_requested when the turn does not settle in time", () =>
    withChild("t3-interrupt-stuck-", "running", (child) =>
      Effect.gen(function* () {
        const { call } = yield* startInterrupt(child);
        yield* TestClock.adjust(INTERRUPT_SETTLE_TIMEOUT);
        expect(yield* Fiber.join(call)).toEqual({
          threadId: child,
          turnId: "turn-1",
          status: "interrupt_requested",
          statusAfter: "running",
        });
      }).pipe(Effect.scoped),
    ),
  );

  it.effect("refuses a thread outside the requested scope", () =>
    withChild("t3-interrupt-scope-", "running", (child) =>
      Effect.gen(function* () {
        // The child calling on its parent: not one of its children.
        const error = yield* callTool(
          "interrupt_thread",
          { threadId: PARENT_ID, scope: "children" },
          child,
        ).pipe(Effect.flip);
        expect(error.message).toContain("is not in scope: children");
      }),
    ),
  );
});
