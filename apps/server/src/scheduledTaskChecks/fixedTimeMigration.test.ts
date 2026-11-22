import { assert, it } from "@effect/vitest";
import { EnvironmentId, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as Sqlite from "../persistence/Sqlite.ts";
import * as Authority from "../scheduledTasks/DestinationAutomationAuthority.ts";
import { migrateLegacyCalendarSchedules } from "./fixedTimeMigration.ts";

const projectId = ProjectId.make("project:719");
const weekly = {
  type: "weekly",
  weekdays: [1, 3],
  times: ["09:00", "17:30"],
  timeZone: "Europe/Warsaw",
  chosen: [{ requested: "09:00", offsetMinutes: -4 }],
};
const certificate: Authority.AutomationCertificate = {
  version: 1,
  id: "certificate:719",
  audience: EnvironmentId.make("destination:719"),
  binding: {
    kind: "schedule",
    resourceId: "task:wrapped",
    projectId,
    threadId: null,
    definitionGeneration: "certificate:719",
    definitionDigest: "digest:719",
    incarnation: "incarnation:719",
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
  namespace: "thread:destination:719",
  generation: "grant:719",
  acceptedAt: 0,
  expiresAt: null,
};

const insert = (taskId: string, schedule: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO scheduled_tasks ${sql.insert({
      task_id: taskId,
      title: "Fixture",
      prompt: "Isolated fixture",
      enabled: 1,
      schedule_json: schedule,
      project_id: projectId,
      thread_id: null,
      workspace_strategy_json: '{"type":"root"}',
      model_selection_json: '{"instanceId":"codex","model":"fixture"}',
      runtime_mode: "full-access",
      interaction_mode: "default",
      created_by: "agent",
      creation_source: "mcp",
      created_at: "2026-10-19T00:00:00.000Z",
      updated_at: "2026-10-19T00:00:00.000Z",
      next_run_at: "2026-10-21T07:00:00.000Z",
      last_run_at: "2026-10-19T15:30:00.000Z",
      last_run_status: "succeeded",
      last_run_error: null,
      run_count: 4,
    })}`;
  });

const rows = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<Record<string, unknown>>`SELECT * FROM scheduled_tasks ORDER BY task_id`;
});

it.effect("rewrites only the trigger, once, and leaves rows it cannot read untouched", () =>
  Effect.gen(function* () {
    yield* insert("task:weekly", JSON.stringify(weekly));
    yield* insert(
      "task:monthly",
      JSON.stringify({ type: "monthly", day: "last", times: ["09:00"], timeZone: "UTC" }),
    );
    const broken = JSON.stringify({ ...weekly, times: [] });
    yield* insert("task:broken", broken);
    yield* insert("task:interval", JSON.stringify({ type: "interval", everyMs: 60_000 }));
    // A prompt that mentions weekly is not a weekly trigger.
    yield* insert("task:weekly-word", JSON.stringify({ type: "fixed_time", timeOfDay: "weekly" }));
    const before = yield* rows;
    yield* migrateLegacyCalendarSchedules();
    const after = yield* rows;
    const byId = new Map(after.map((row) => [row.task_id, row]));
    assert.deepEqual(JSON.parse(String(byId.get("task:weekly")!.schedule_json)), {
      type: "fixed_time",
      timeOfDay: "09:00",
      extraTimes: ["17:30"],
      weekdays: [1, 3],
      timeZone: "Europe/Warsaw",
      windowMinutes: 30,
      chosen: [{ requested: "09:00", offsetMinutes: -4 }],
    });
    assert.deepEqual(JSON.parse(String(byId.get("task:monthly")!.schedule_json)), {
      type: "fixed_time",
      timeOfDay: "09:00",
      timeZone: "UTC",
      monthDays: [31],
    });
    // Every other column, and every other row, is byte for byte what it was.
    for (const [index, row] of before.entries()) {
      const migrated = row.task_id === "task:weekly" || row.task_id === "task:monthly";
      assert.deepEqual(
        { ...after[index], schedule_json: migrated ? null : after[index]!.schedule_json },
        { ...row, schedule_json: migrated ? null : row.schedule_json },
      );
    }
    assert.equal(byId.get("task:broken")!.schedule_json, broken);
    yield* migrateLegacyCalendarSchedules();
    assert.deepEqual(yield* rows, after);
  }).pipe(Effect.provide(Sqlite.layerMemory)),
);

it.effect("a failed write changes no row and fails the migration", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* insert("task:a", JSON.stringify(weekly));
    yield* insert("task:b", JSON.stringify({ ...weekly, weekdays: [5] }));
    yield* sql`CREATE TRIGGER refuse_b BEFORE UPDATE ON scheduled_tasks WHEN old.task_id = 'task:b' BEGIN SELECT RAISE(ABORT, 'refused'); END`;
    const before = yield* rows;
    const result = yield* Effect.exit(migrateLegacyCalendarSchedules());
    assert.equal(result._tag, "Failure");
    assert.deepEqual(yield* rows, before);
  }).pipe(Effect.provide(Sqlite.layerMemory)),
);

it.effect("an accepted automation keeps its wrapper and is still admitted after migration", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const authority = yield* Authority.DestinationAutomationAuthority;
    yield* sql`CREATE TABLE IF NOT EXISTS scheduled_task_request_incarnations (task_id TEXT PRIMARY KEY,incarnation TEXT NOT NULL,active_request_id TEXT)`;
    yield* insert("task:wrapped", "{}");
    yield* sql`INSERT INTO scheduled_task_request_incarnations(task_id,incarnation) VALUES ('task:wrapped','incarnation:719')`;
    yield* sql.withTransaction(
      authority.accept(
        Authority.ValidatedAcceptance.afterLiveCapture(certificate, []),
        certificate.binding,
      ),
    );
    const wrapped = { type: "destination-automation-v1", certificateId: certificate.id };
    yield* sql`UPDATE scheduled_tasks SET schedule_json = ${JSON.stringify({ ...wrapped, schedule: weekly })}`;
    yield* migrateLegacyCalendarSchedules();
    const [row] = yield* rows;
    assert.deepEqual(JSON.parse(String(row!.schedule_json)), {
      ...wrapped,
      schedule: {
        type: "fixed_time",
        timeOfDay: "09:00",
        extraTimes: ["17:30"],
        weekdays: [1, 3],
        timeZone: "Europe/Warsaw",
        windowMinutes: 30,
        chosen: [{ requested: "09:00", offsetMinutes: -4 }],
      },
    });
    assert.equal((yield* authority.claim(certificate.id)).id, certificate.id);
  }).pipe(Effect.provide(Layer.provideMerge(Authority.layer, Sqlite.layerMemory))),
);
