import { expect, it } from "@effect/vitest";
import { AuthSessionId, EnvironmentId, ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Authority from "./DestinationAutomationAuthority.ts";

const source = EnvironmentId.make("source-384");
const destination = EnvironmentId.make("destination-384");
const certificate = (id = "accepted-384"): Authority.AutomationCertificate => ({
  version: 1,
  id,
  audience: destination,
  binding: {
    kind: "schedule",
    resourceId: "task-384",
    projectId: ProjectId.make("project-384"),
    threadId: null,
    definitionGeneration: id,
    definitionDigest: `digest:${id}`,
    incarnation: "incarnation-384",
  },
  recordIds: [],
  graph: { records: [], legacy: [], dependencies: [] },
  ceiling: {
    runtimeMode: "full-access",
    interactionMode: "default",
    resourceScope: "environment-resources",
    onward: true,
    capabilities: ["orchestration"],
  },
  namespace: "thread:destination-384:parent-384",
  generation: "grant-384",
  acceptedAt: 0,
  expiresAt: null,
});
const dependency = (value = certificate()): Authority.AutomationDependency => ({
  sourceEnvironmentId: source,
  destinationEnvironmentId: destination,
  certificateId: value.id,
  recordId: "source-record-384",
  grantGeneration: value.generation,
  resourceId: value.binding.resourceId,
  definitionGeneration: value.binding.definitionGeneration,
  definitionDigest: value.binding.definitionDigest,
});
const fixture = Layer.provideMerge(Authority.layer, Sqlite.layerMemory);
const accept = (value: Authority.AutomationCertificate) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const authority = yield* Authority.DestinationAutomationAuthority;
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`CREATE TABLE IF NOT EXISTS scheduled_task_request_incarnations (task_id TEXT PRIMARY KEY,incarnation TEXT NOT NULL,active_request_id TEXT)`;
        yield* sql`INSERT INTO scheduled_task_request_incarnations(task_id,incarnation) VALUES (${value.binding.resourceId},${value.binding.incarnation}) ON CONFLICT(task_id) DO UPDATE SET incarnation=excluded.incarnation`;
        yield* sql`INSERT INTO scheduled_tasks ${sql.insert({
          task_id: value.binding.resourceId,
          title: "Fixture",
          prompt: "Isolated fixture",
          enabled: 1,
          schedule_json: "{}",
          project_id: value.binding.projectId,
          thread_id: value.binding.threadId,
          workspace_strategy_json: '{"type":"root"}',
          model_selection_json: '{"instanceId":"codex","model":"fixture"}',
          runtime_mode: "full-access",
          interaction_mode: "default",
          created_by: "user",
          creation_source: "web",
          created_at: "2026-10-19T00:00:00Z",
          updated_at: "2026-10-19T00:00:00Z",
          last_run_status: "never",
          run_count: 0,
        })} ON CONFLICT(task_id) DO NOTHING`;
        yield* authority.accept(
          Authority.ValidatedAcceptance.afterLiveCapture(value, []),
          value.binding,
        );
        yield* sql`UPDATE scheduled_tasks SET schedule_json=${`{"type":"destination-automation-v1","certificateId":"${value.id}","schedule":{"type":"interval","everyMs":60000}}`} WHERE task_id=${value.binding.resourceId}`;
      }),
    );
  });

it.effect(
  "a durable disable before acceptance fences a lost capture reply and remains idempotent",
  () =>
    Effect.gen(function* () {
      const authority = yield* Authority.DestinationAutomationAuthority;
      const value = certificate();
      const input = { version: 1, dependency: dependency(value) } as const;
      yield* authority.registerRecovery(
        input.dependency,
        "session-384",
        "synthetic-fingerprint-384",
      );
      expect(
        (yield* Effect.result(authority.receiveDisable(input, "wrong-fingerprint")))._tag,
      ).toBe("Failure");
      const acknowledgement = yield* authority.receiveDisable(input, "synthetic-fingerprint-384");
      expect(acknowledgement).toEqual({ ...input, disabled: true });
      expect(yield* authority.receiveDisable(input, "synthetic-fingerprint-384")).toEqual(
        acknowledgement,
      );
      const late = yield* Effect.result(accept(value));
      expect(late._tag).toBe("Failure");
      expect((yield* Effect.result(authority.claim(value.id)))._tag).toBe("Failure");
      const changed = {
        ...input,
        dependency: { ...input.dependency, definitionDigest: "another-definition" },
      };
      expect(
        (yield* Effect.result(authority.receiveDisable(changed, "synthetic-fingerprint-384")))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(fixture)),
);

it.effect(
  "acceptance rolls back with its native transaction, and one admission cannot be reused",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const authority = yield* Authority.DestinationAutomationAuthority;
      const value = certificate();
      const admission = Authority.ValidatedAcceptance.afterLiveCapture(value, []);
      const failed = yield* Effect.result(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* authority.accept(admission, value.binding);
            return yield* Effect.fail("native definition failed");
          }),
        ),
      );
      expect(failed._tag).toBe("Failure");
      expect((yield* Effect.result(authority.claim(value.id)))._tag).toBe("Failure");
      expect((yield* Effect.result(authority.accept(admission, value.binding)))._tag).toBe(
        "Failure",
      );
      yield* accept(value);
      expect((yield* authority.claim(value.id)).id).toBe(value.id);
    }).pipe(Effect.provide(fixture)),
);

it.effect(
  "disable and known expiry are monotone, while a fresh accepted generation supersedes only the old one",
  () =>
    Effect.gen(function* () {
      const authority = yield* Authority.DestinationAutomationAuthority;
      const old = certificate();
      yield* accept(old);
      yield* authority.disableLocal("schedule", old.binding.resourceId);
      expect((yield* Effect.result(authority.claim(old.id)))._tag).toBe("Failure");
      expect((yield* Effect.result(accept(old)))._tag).toBe("Failure");
      const next = { ...certificate("next-384"), expiresAt: 1000 };
      yield* accept(next);
      expect((yield* authority.claim(next.id)).id).toBe(next.id);
      yield* TestClock.adjust("1 second");
      expect((yield* Effect.result(authority.claim(next.id)))._tag).toBe("Failure");
      expect((yield* Effect.result(authority.claim(old.id)))._tag).toBe("Failure");
    }).pipe(Effect.provide(fixture)),
);

it.effect(
  "source invalidation remains unconfirmed until an exact acknowledgement from its original generation",
  () =>
    Effect.gen(function* () {
      const authority = yield* Authority.DestinationAutomationAuthority;
      const sql = yield* SqlClient.SqlClient;
      const dep = dependency();
      yield* authority.registerDependency(
        dep,
        { environmentId: destination, generation: dep.grantGeneration },
        [AuthSessionId.make("session-384")],
        true,
      );
      expect(yield* authority.pending).toEqual([]);
      yield* sql.withTransaction(Authority.invalidateAutomationSessions(["session-384"]));
      expect(yield* authority.pending).toEqual([
        { dependency: dep, environmentId: destination, generation: dep.grantGeneration },
      ]);
      const acknowledgement = { version: 1, dependency: dep, disabled: true } as const;
      expect(
        (yield* Effect.result(authority.acknowledge(acknowledgement, destination, "successor-384")))
          ._tag,
      ).toBe("Failure");
      expect(
        (yield* Effect.result(
          authority.acknowledge(
            { ...acknowledgement, dependency: { ...dep, certificateId: "wrong" } },
            destination,
            dep.grantGeneration,
          ),
        ))._tag,
      ).toBe("Failure");
      expect((yield* authority.pending).length).toBe(1);
      yield* authority.acknowledge(acknowledgement, destination, dep.grantGeneration);
      expect(yield* authority.pending).toEqual([]);
    }).pipe(Effect.provide(fixture)),
);

it.effect(
  "retiring a source automation generation persists its downstream disable before replacement and rollback preserves both",
  () =>
    Effect.gen(function* () {
      const authority = yield* Authority.DestinationAutomationAuthority;
      const sql = yield* SqlClient.SqlClient;
      const parent = certificate();
      yield* accept(parent);
      const dep = {
        ...dependency(certificate("downstream-384")),
        destinationEnvironmentId: EnvironmentId.make("third-384"),
      };
      const route = {
        environmentId: dep.destinationEnvironmentId,
        generation: "original-downstream-link",
      };
      yield* authority.registerDependency(dep, route, [], true, null, [parent.id]);
      const aborted = yield* Effect.result(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* authority.disableLocal("schedule", parent.binding.resourceId);
            expect(
              yield* sql`SELECT 1 FROM fork_automation_obligations WHERE pending=1 AND ack_json IS NULL`,
            ).toHaveLength(1);
            return yield* Effect.fail("native save rejected");
          }),
        ),
      );
      expect(aborted._tag).toBe("Failure");
      expect(yield* authority.pending).toEqual([]);
      expect((yield* authority.claim(parent.id)).id).toBe(parent.id);
      yield* accept(certificate("replacement-384"));
      expect((yield* Effect.result(authority.claim(parent.id)))._tag).toBe("Failure");
      expect(yield* authority.pending).toEqual([{ dependency: dep, ...route }]);
      expect((yield* authority.claim("replacement-384")).id).toBe("replacement-384");
      yield* authority.acknowledge(
        { version: 1, dependency: dep, disabled: true },
        route.environmentId,
        route.generation,
      );
      expect(yield* authority.pending).toEqual([]);
      expect((yield* authority.claim("replacement-384")).id).toBe("replacement-384");
    }).pipe(Effect.provide(fixture)),
);

it.effect(
  "bounded disable batches reach every pending obligation across owner reconstruction",
  () =>
    Effect.gen(function* () {
      const authority = yield* Authority.DestinationAutomationAuthority;
      for (let index = 0; index < 33; index++) {
        const dep = dependency(certificate(`fair-${index}`));
        yield* authority.registerDependency(
          dep,
          { environmentId: destination, generation: dep.grantGeneration },
          ["fair-session"],
          true,
        );
      }
      yield* Authority.invalidateAutomationSessions(["fair-session"]);
      const first = yield* authority.pending;
      expect(first).toHaveLength(32);
      const reopened = yield* Authority.layer.pipe(Layer.fresh, Layer.build);
      const second = yield* Context.get(reopened, Authority.DestinationAutomationAuthority).pending;
      expect(second.length).toBeLessThanOrEqual(32);
      expect(
        new Set([...first, ...second].map((entry) => entry.dependency.certificateId)).size,
      ).toBe(33);
      const sql = yield* SqlClient.SqlClient;
      expect(
        yield* sql`SELECT 1 FROM fork_automation_obligations WHERE pending=1 AND ack_json IS NULL`,
      ).toHaveLength(33);
    }).pipe(Effect.provide(fixture)),
);

it.effect("new obligation arrivals do not starve an older pending retry", () =>
  Effect.gen(function* () {
    const authority = yield* Authority.DestinationAutomationAuthority;
    const original = dependency(certificate("older-retry"));
    yield* authority.registerDependency(
      original,
      { environmentId: destination, generation: original.grantGeneration },
      ["older-session"],
      true,
    );
    yield* Authority.invalidateAutomationSessions(["older-session"]);
    expect((yield* authority.pending).map((row) => row.dependency)).toEqual([original]);
    let retried = false;
    for (let wave = 0; wave < 2; wave++) {
      for (let index = 0; index < 33; index++) {
        const dep = dependency(certificate(`new-arrival-${wave}-${index}`));
        yield* authority.registerDependency(
          dep,
          { environmentId: destination, generation: dep.grantGeneration },
          [`arrival-session-${wave}`],
          true,
        );
      }
      yield* Authority.invalidateAutomationSessions([`arrival-session-${wave}`]);
      const batch = yield* authority.pending;
      expect(batch.length).toBeLessThanOrEqual(32);
      retried ||= batch.some((row) => row.dependency.certificateId === original.certificateId);
    }
    expect(retried).toBe(true);
  }).pipe(Effect.provide(fixture)),
);
