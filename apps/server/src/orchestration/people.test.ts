import { EventId, ProjectId, ProviderInstanceId, threadOwner } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import { createEmptyReadModel, projectEvent } from "./projector.ts";
import {
  commandId,
  createParent,
  NOW,
  PARENT_ID,
  temporaryDirectory,
  withServer,
} from "../mcp/toolkits/threads/handlers.testFixtures.ts";

it.effect("projects legacy creation with a null owner and empty co-owners", () =>
  Effect.gen(function* () {
    const model = yield* projectEvent(createEmptyReadModel(NOW), {
      sequence: 1,
      eventId: EventId.make("legacy-created"),
      type: "thread.created",
      aggregateKind: "thread",
      aggregateId: PARENT_ID,
      occurredAt: NOW,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      payload: {
        threadId: PARENT_ID,
        projectId: ProjectId.make("project"),
        title: "Legacy",
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdAt: NOW,
        updatedAt: NOW,
      },
    });
    expect(model.threads[0]?.owner).toBeNull();
    expect(model.threads[0]?.coOwners).toEqual([]);
    expect(threadOwner(model.threads[0]!)).toBe("Luke");
  }),
);

it.effect("persists ownership and sharing through events, thread reads and shell reads", () =>
  Effect.gen(function* () {
    const directory = yield* temporaryDirectory("t3-people-");
    yield* withServer(
      `${directory}/state.sqlite`,
      Effect.gen(function* () {
        yield* createParent(directory, "full-access", "Luke");
        const engine = yield* OrchestrationEngineService;
        const query = yield* ProjectionSnapshotQuery;
        const detail = () =>
          query.getThreadDetailById(PARENT_ID).pipe(Effect.map(Option.getOrThrow));
        expect((yield* detail()).owner).toBe("Luke");
        const command = { threadId: PARENT_ID, createdAt: NOW };
        yield* engine.dispatch({
          ...command,
          type: "thread.share",
          commandId: commandId(),
          actor: "Luke",
          coOwner: "Pauli",
        });
        let thread = yield* detail();
        expect(thread.coOwners).toEqual(["Pauli"]);
        expect(thread.activities.at(-1)).toMatchObject({
          kind: "thread.sharing",
          tone: "info",
          summary: "Luke shared this thread with Pauli",
        });
        expect(Option.getOrThrow(yield* query.getThreadShellById(PARENT_ID))).toMatchObject({
          owner: "Luke",
          coOwners: ["Pauli"],
        });
        expect((yield* query.getShellSnapshot()).threads[0]).toMatchObject({
          owner: "Luke",
          coOwners: ["Pauli"],
        });
        const activities = thread.activities.length;
        yield* engine.dispatch({
          ...command,
          type: "thread.share",
          commandId: commandId(),
          actor: "Luke",
          coOwner: "Pauli",
        });
        expect((yield* detail()).coOwners).toEqual(["Pauli"]);
        expect((yield* detail()).activities).toHaveLength(activities);
        const rejected = yield* engine
          .dispatch({ ...command, type: "thread.unshare", commandId: commandId(), actor: "Pauli" })
          .pipe(Effect.flip);
        expect(rejected.message).toContain("Only the owner");
        yield* engine.dispatch({
          ...command,
          type: "thread.share",
          commandId: commandId(),
          actor: "Luke",
          coOwner: "Future person",
        });
        yield* engine.dispatch({
          ...command,
          type: "thread.leave",
          commandId: commandId(),
          actor: "Pauli",
        });
        thread = yield* detail();
        expect(thread.coOwners).toEqual(["Future person"]);
        expect(thread.activities.at(-1)?.summary).toBe("Pauli left this shared thread");
        expect(
          (yield* engine
            .dispatch({ ...command, type: "thread.leave", commandId: commandId(), actor: "Pauli" })
            .pipe(Effect.flip)).message,
        ).toContain("Only a current co-owner");
        yield* engine.dispatch({
          ...command,
          type: "thread.unshare",
          commandId: commandId(),
          actor: "Luke",
        });
        thread = yield* detail();
        expect(thread.coOwners).toEqual([]);
        expect(thread.activities.at(-1)?.summary).toBe("Luke stopped sharing this thread");
        expect(
          (yield* engine
            .dispatch({
              ...command,
              type: "thread.share",
              commandId: commandId(),
              actor: "Luke",
              coOwner: "Luke",
            })
            .pipe(Effect.flip)).message,
        ).toContain("cannot be a co-owner");
      }),
    );
  }).pipe(Effect.scoped),
);
