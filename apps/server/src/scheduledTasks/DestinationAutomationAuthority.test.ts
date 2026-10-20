import { expect, it } from "@effect/vitest";
import { AuthSessionId, EnvironmentId, ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
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
      yield* authority.registerDependency(dep, route, [], true, null, [
        { kind: "automation-execution", certificateId: parent.id },
      ]);
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

it.effect(
  "lifecycle retirement preserves original birth facts while source invalidation remains permanent",
  () =>
    Effect.gen(function* () {
      const authority = yield* Authority.DestinationAutomationAuthority;
      const value = certificate("birth-retired-384");
      yield* accept(value);
      yield* authority.registerRecovery(
        dependency(value),
        "birth-source-session",
        "fixture-original-identity",
      );
      yield* authority.disableLocal("schedule", value.binding.resourceId);
      expect((yield* Effect.result(authority.claim(value.id)))._tag).toBe("Failure");
      expect(yield* authority.birth(value.id)).toEqual(value);
      yield* accept(certificate("new-definition-384"));
      expect(yield* authority.birth(value.id)).toEqual(value);
      const reopened = Context.get(
        yield* Authority.layer.pipe(Layer.fresh, Layer.build),
        Authority.DestinationAutomationAuthority,
      );
      expect(yield* reopened.birth(value.id)).toEqual(value);
      yield* Authority.invalidateAutomationSessions(["birth-source-session"]);
      expect((yield* Effect.result(authority.birth(value.id)))._tag).toBe("Failure");
      yield* accept(certificate("later-definition-384"));
      expect((yield* Effect.result(authority.birth(value.id)))._tag).toBe("Failure");
    }).pipe(Effect.provide(fixture)),
);

it.effect(
  "a durable source disable still denies a retired birth and cannot be undone by local replacement",
  () =>
    Effect.gen(function* () {
      const authority = yield* Authority.DestinationAutomationAuthority;
      const value = certificate("birth-tombstone-384");
      yield* accept(value);
      yield* authority.registerRecovery(
        dependency(value),
        "birth-source-session",
        "fixture-original-identity",
      );
      yield* authority.disableLocal("schedule", value.binding.resourceId);
      expect(yield* authority.birth(value.id)).toEqual(value);
      const acknowledgement = yield* authority.receiveDisable(
        { version: 1, dependency: dependency(value) },
        "fixture-original-identity",
      );
      expect(acknowledgement?.disabled).toBe(true);
      expect((yield* Effect.result(authority.birth(value.id)))._tag).toBe("Failure");
      yield* accept(certificate("replacement-after-source-disable-384"));
      expect((yield* Effect.result(authority.birth(value.id)))._tag).toBe("Failure");
    }).pipe(Effect.provide(fixture)),
);

it.effect.each([false, true])(
  "birth dependency registration before/after retirement (%s) survives retirement but not security invalidation",
  (retireBeforeCapture) =>
    Effect.gen(function* () {
      const authority = yield* Authority.DestinationAutomationAuthority;
      const value = certificate("r3-original");
      yield* accept(value);
      yield* authority.registerRecovery(
        dependency(value),
        "r3-source-session",
        "r3-original-identity",
      );
      if (retireBeforeCapture) yield* authority.disableLocal("schedule", value.binding.resourceId);
      const dep = {
        ...dependency(certificate("r3-downstream")),
        destinationEnvironmentId: EnvironmentId.make("third-384"),
      };
      const route = {
        environmentId: dep.destinationEnvironmentId,
        generation: "r3-original-route",
      };
      yield* authority.registerDependency(dep, route, [], true, null, [
        { kind: "automation-birth", certificateId: value.id },
      ]);
      if (!retireBeforeCapture) yield* authority.disableLocal("schedule", value.binding.resourceId);
      expect((yield* Effect.result(authority.claim(value.id)))._tag).toBe("Failure");
      expect((yield* authority.birth(value.id)).id).toBe(value.id);
      expect(yield* authority.pending).toEqual([]);
      const reopened = Context.get(
        yield* Authority.layer.pipe(Layer.fresh, Layer.build),
        Authority.DestinationAutomationAuthority,
      );
      expect(yield* reopened.pending).toEqual([]);
      yield* Authority.invalidateAutomationSessions(["r3-source-session"]);
      expect(yield* reopened.pending).toEqual([{ dependency: dep, ...route }]);
      expect((yield* Effect.result(reopened.birth(value.id)))._tag).toBe("Failure");
      yield* reopened.acknowledge(
        { version: 1, dependency: dep, disabled: true },
        route.environmentId,
        route.generation,
      );
      expect(yield* reopened.pending).toEqual([]);
    }).pipe(Effect.provide(fixture)),
);

it.effect.each([false, true])(
  "execution dependency dominates a shared birth regardless of registration order (%s)",
  (executionFirst) =>
    Effect.gen(function* () {
      const authority = yield* Authority.DestinationAutomationAuthority;
      const value = certificate();
      yield* accept(value);
      const kinds = executionFirst
        ? (["automation-execution", "automation-birth"] as const)
        : (["automation-birth", "automation-execution"] as const);
      const dep = dependency(certificate("r3-joint-downstream"));
      const route = { environmentId: destination, generation: "r3-joint-route" };
      for (const kind of kinds)
        yield* authority.registerDependency(dep, route, [], true, null, [
          { kind, certificateId: value.id },
        ]);
      yield* authority.disableLocal("schedule", value.binding.resourceId);
      expect(yield* authority.pending).toEqual([{ dependency: dep, ...route }]);
      expect(
        (yield* Effect.result(
          authority.registerDependency(
            { ...dep, recordId: "new-execution" },
            route,
            [],
            true,
            null,
            [{ kind: "automation-execution", certificateId: value.id }],
          ),
        ))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(fixture)),
);

it.effect("birth dependency expiry still disables downstream after ordinary retirement", () =>
  Effect.gen(function* () {
    const authority = yield* Authority.DestinationAutomationAuthority;
    const value = { ...certificate(), expiresAt: 1000 };
    yield* accept(value);
    const dep = dependency(certificate("r3-expiring-downstream"));
    const route = { environmentId: destination, generation: "r3-expiring-route" };
    yield* authority.registerDependency(dep, route, [], true, null, [
      { kind: "automation-birth", certificateId: value.id },
    ]);
    yield* authority.disableLocal("schedule", value.binding.resourceId);
    expect(yield* authority.pending).toEqual([]);
    yield* TestClock.adjust("1 second");
    expect(yield* authority.pending).toEqual([{ dependency: dep, ...route }]);
    expect((yield* Effect.result(authority.birth(value.id)))._tag).toBe("Failure");
  }).pipe(Effect.provide(fixture)),
);

it.effect(
  "historical dependency rows keep strict execution retirement after additive purpose migration",
  () =>
    Effect.gen(function* () {
      const authority = yield* Authority.DestinationAutomationAuthority;
      const sql = yield* SqlClient.SqlClient;
      const value = certificate();
      yield* accept(value);
      const dep = dependency(certificate("r3-historical-downstream"));
      const route = { environmentId: destination, generation: "r3-historical-route" };
      yield* authority.registerDependency(dep, route, [], true, null, [
        { kind: "automation-execution", certificateId: value.id },
      ]);
      yield* sql`ALTER TABLE fork_automation_dependency_certificates DROP COLUMN certificate_kind`;
      yield* Authority.ensureAutomationAuthoritySchema;
      expect(
        yield* sql`SELECT certificate_kind FROM fork_automation_dependency_certificates`,
      ).toEqual([{ certificate_kind: "automation-execution" }]);
      yield* authority.disableLocal("schedule", value.binding.resourceId);
      expect(yield* authority.pending).toEqual([{ dependency: dep, ...route }]);
    }).pipe(Effect.provide(fixture)),
);

it.effect.each(["birth-only", "execution-first", "execution-last", "unrelated-session"] as const)(
  "session dependency retirement preserves only exact birth facts: %s",
  (scenario) =>
    Effect.gen(function* () {
      const authority = yield* Authority.DestinationAutomationAuthority;
      const sql = yield* SqlClient.SqlClient;
      const dep = dependency(certificate("r4-session-downstream"));
      const route = { environmentId: destination, generation: "r4-original-route" };
      const birth = { kind: "automation-birth", sessionId: "r4-birth-session" } as const;
      const strict = scenario === "unrelated-session" ? "r4-unrelated-session" : birth.sessionId;
      if (scenario === "execution-first")
        yield* authority.registerDependency(dep, route, [strict], true);
      yield* authority.registerDependency(dep, route, [birth], true);
      if (scenario === "execution-last" || scenario === "unrelated-session")
        yield* authority.registerDependency(dep, route, [strict], true);
      yield* Authority.invalidateAutomationSessions([birth.sessionId, strict], "retired");
      const expected = [{ dependency: dep, ...route }];
      expect(yield* authority.pending).toEqual(scenario === "birth-only" ? [] : expected);
      yield* Authority.invalidateAutomationSessions([birth.sessionId]);
      expect(yield* authority.pending).toEqual(expected);
      expect(
        yield* sql`SELECT grant_kind FROM fork_automation_dependency_grants WHERE session_id=${birth.sessionId}`,
      ).toEqual([
        {
          grant_kind:
            scenario === "execution-first" || scenario === "execution-last"
              ? "automation-execution"
              : "automation-birth",
        },
      ]);
    }).pipe(Effect.provide(fixture)),
);

it.effect("historical session indexes migrate to strict execution retirement", () =>
  Effect.gen(function* () {
    const authority = yield* Authority.DestinationAutomationAuthority;
    const sql = yield* SqlClient.SqlClient;
    const dep = dependency(certificate("r4-historical-session"));
    const route = { environmentId: destination, generation: "r4-historical-route" };
    yield* authority.registerDependency(dep, route, ["r4-historical-session"], true);
    yield* sql`ALTER TABLE fork_automation_dependency_grants DROP COLUMN grant_kind`;
    yield* Authority.ensureAutomationAuthoritySchema;
    expect(yield* sql`SELECT grant_kind FROM fork_automation_dependency_grants`).toEqual([
      { grant_kind: "automation-execution" },
    ]);
    yield* Authority.invalidateAutomationSessions(["r4-historical-session"], "retired");
    expect(yield* authority.pending).toEqual([{ dependency: dep, ...route }]);
  }).pipe(Effect.provide(fixture)),
);

it.effect.each([
  "origin",
  "certificate-birth",
  "certificate-execution",
  "grant-birth",
  "grant-execution",
] as const)("recovery deadline and durable expiry promotion agree for %s", (kind) =>
  Effect.gen(function* () {
    const authority = yield* Authority.DestinationAutomationAuthority;
    const sql = yield* SqlClient.SqlClient;
    const expiry = 1001;
    const value = { ...certificate("r6-deadline"), expiresAt: expiry };
    const dep = dependency(certificate("r6-downstream"));
    const route = { environmentId: destination, generation: "r6-route" };
    if (kind.startsWith("certificate")) yield* accept(value);
    if (kind.startsWith("grant"))
      yield* sql`INSERT INTO auth_sessions(session_id,subject,scopes,method,issued_at,expires_at) VALUES ('r6-session','fixture','[]','pairing-code',${DateTime.formatIso(DateTime.makeUnsafe(0))},${DateTime.formatIso(DateTime.makeUnsafe(expiry))})`;
    yield* authority.registerDependency(
      dep,
      route,
      kind === "grant-birth"
        ? [{ kind: "automation-birth", sessionId: "r6-session" }]
        : kind === "grant-execution"
          ? ["r6-session"]
          : [],
      true,
      kind === "origin" ? expiry : null,
      kind === "certificate-birth"
        ? [{ kind: "automation-birth", certificateId: value.id }]
        : kind === "certificate-execution"
          ? [{ kind: "automation-execution", certificateId: value.id }]
          : [],
    );
    expect(yield* authority.nextDisableDeadline).toBe(expiry);
    expect(yield* authority.pending).toEqual([]);
    const reopened = Context.get(
      yield* Authority.layer.pipe(Layer.fresh, Layer.build),
      Authority.DestinationAutomationAuthority,
    );
    expect(yield* reopened.nextDisableDeadline).toBe(expiry);
    yield* TestClock.setTime(expiry - 1);
    expect(yield* reopened.pending).toEqual([]);
    expect(yield* reopened.nextDisableDeadline).toBe(expiry);
    yield* TestClock.adjust("1 millis");
    expect(yield* reopened.pending).toEqual([{ dependency: dep, ...route }]);
    expect(yield* reopened.nextDisableDeadline).toBeNull();
    yield* reopened.acknowledge(
      { version: 1, dependency: dep, disabled: true },
      route.environmentId,
      route.generation,
    );
    expect(yield* reopened.pending).toEqual([]);
    expect(yield* reopened.nextDisableDeadline).toBeNull();
  }).pipe(Effect.provide(fixture)),
);

it.effect(
  "deadline selection rearms to earlier registration and cancels ACKed or removed obligations",
  () =>
    Effect.gen(function* () {
      const authority = yield* Authority.DestinationAutomationAuthority;
      const sql = yield* SqlClient.SqlClient;
      const later = dependency(certificate("r6-later"));
      const earlier = dependency(certificate("r6-earlier"));
      const route = { environmentId: destination, generation: "r6-cancel-route" };
      expect(yield* authority.nextDisableDeadline).toBeNull();
      yield* authority.registerDependency(later, route, [], true, 2000);
      yield* authority.registerDependency(earlier, route, [], true, 1000);
      expect(yield* authority.nextDisableDeadline).toBe(1000);
      yield* authority.acknowledge(
        { version: 1, dependency: earlier, disabled: true },
        destination,
        route.generation,
      );
      expect(yield* authority.nextDisableDeadline).toBe(2000);
      yield* sql`DELETE FROM fork_automation_obligations WHERE ack_json IS NULL`;
      expect(yield* authority.nextDisableDeadline).toBeNull();
      yield* TestClock.adjust("1 hour");
      expect(yield* authority.pending).toEqual([]);
    }).pipe(Effect.provide(fixture)),
);
