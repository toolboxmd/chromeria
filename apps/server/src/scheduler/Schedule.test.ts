import { describe, expect, it } from "vite-plus/test";
import { ProjectId, type TaskDefinition, type ScheduledTask } from "@t3tools/contracts";
import { chooseMinutes, taskSlots } from "./Schedule.ts";
const now = Date.parse("2026-10-01T00:00:00Z"); // Thursday
const weekly = (
  weekdays = [0, 1, 2, 3, 4, 5, 6],
  timeZone = "UTC",
  time = "08:00",
): TaskDefinition => ({
  title: "Daily",
  prompt: "work",
  target: { kind: "new-thread", projectId: ProjectId.make("p") },
  role: "worker",
  schedule: { kind: "weekly", weekdays, times: [time], timeZone },
});
const task = (
  definition: TaskDefinition,
  tasks: ReadonlyArray<ScheduledTask> = [],
): ScheduledTask => ({
  checkCwd: "/",
  id: String(tasks.length),
  revision: 1,
  definition,
  createdAt: "2026-10-01T00:00:00Z",
  updatedAt: "2026-10-01T00:00:00Z",
  paused: false,
  deleted: false,
  checks: [],
  choices: chooseMinutes(definition, tasks, now),
  consumedSlot: null,
  runs: [],
  failureStreak: 0,
  lastError: null,
});
describe("scheduler time math", () => {
  it("spreads ten daily 08:00 tasks symmetrically and returns actual neighbours", () => {
    const tasks: ScheduledTask[] = [];
    for (let index = 0; index < 10; index++) tasks.push(task(weekly(), tasks));
    expect(new Set(tasks.map((task) => task.choices[0]!.chosen)).size).toBe(10);
    expect(tasks.every((task) => Math.abs(task.choices[0]!.offsetMinutes) <= 30)).toBe(true);
    expect(tasks.at(-1)?.choices[0]?.neighbours.length).toBeGreaterThan(0);
    expect(tasks.map((task) => task.choices[0]!.offsetMinutes).slice(0, 3)).toEqual([0, -1, 1]);
  });
  it("keeps two requested weekly times distinct when a neighbour is occupied", () => {
    const existing = task({
      ...weekly([], "UTC", "08:01"),
      schedule: {
        kind: "weekly",
        weekdays: [0, 1, 2, 3, 4, 5, 6],
        times: ["08:01"],
        timeZone: "UTC",
        windowMinutes: 0,
      },
    });
    const selected = task(
      {
        ...weekly(),
        schedule: {
          kind: "weekly",
          weekdays: [0, 1, 2, 3, 4, 5, 6],
          times: ["08:00", "08:01"],
          timeZone: "UTC",
        },
      },
      [existing],
    );
    expect(selected.choices.map((choice) => choice.chosen)).toEqual(["08:00", "08:02"]);
    expect(taskSlots(selected, Date.parse("2026-10-01T08:00Z")).next).toBe(
      Date.parse("2026-10-01T08:02Z"),
    );
  });
  it("counts cross-kind actual instants and normalizes one-shot seconds", () => {
    const daily = task(weekly());
    const once = task({ ...weekly(), schedule: { kind: "once", at: "2026-10-01T08:00:44Z" } }, [
      daily,
    ]);
    expect(once.choices[0]!.chosen).toBe("2026-10-01T07:59:00.000Z");
    const exact = task({
      ...weekly(),
      schedule: { kind: "once", at: "2026-10-01T08:00:44Z", windowMinutes: 0 },
    });
    expect(exact.choices[0]!.chosen).toBe("2026-10-01T08:00:00.000Z");
    const interval = task({ ...weekly(), schedule: { kind: "interval", minutes: 60 } });
    expect(task(weekly(), [interval]).choices[0]!.offsetMinutes).toBe(-1);
  });
  it("does not penalize disjoint weekdays or unrelated timezone instants", () => {
    const thursday = task(weekly([4]));
    expect(task(weekly([5]), [thursday]).choices[0]!.offsetMinutes).toBe(0);
    expect(task(weekly([4], "Europe/Warsaw", "08:00"), [thursday]).choices[0]!.offsetMinutes).toBe(
      0,
    );
    expect(task(weekly([4], "Europe/Warsaw", "10:00"), [thursday]).choices[0]!.offsetMinutes).toBe(
      -1,
    );
  });
  it("pins exact zero windows and emits latest missed/next weekly and interval slots", () => {
    const exact = task({
      ...weekly(),
      schedule: {
        kind: "weekly",
        weekdays: [4],
        times: ["08:00"],
        timeZone: "UTC",
        windowMinutes: 0,
      },
    });
    expect(taskSlots(exact, Date.parse("2026-10-15T10:00Z"))).toEqual({
      latest: Date.parse("2026-10-15T08:00Z"),
      next: Date.parse("2026-10-22T08:00Z"),
    });
    const interval = task({ ...weekly(), schedule: { kind: "interval", minutes: 30 } });
    expect(taskSlots(interval, now + 3 * 3600_000 + 600_000)).toEqual({
      latest: now + 3 * 3600_000,
      next: now + 3 * 3600_000 + 1800_000,
    });
  });
  it("honours local DST named times and offset crossing midnight", () => {
    const zoned = task({
      ...weekly([0], "Europe/Warsaw"),
      schedule: {
        kind: "weekly",
        weekdays: [0],
        times: ["08:00"],
        timeZone: "Europe/Warsaw",
        windowMinutes: 0,
      },
    });
    expect(taskSlots(zoned, Date.parse("2026-10-25T00:00Z")).next).toBe(
      Date.parse("2026-10-25T07:00Z"),
    );
    const crossing = {
      ...task(weekly([4], "UTC", "00:05")),
      choices: [{ requested: "00:05", chosen: "23:55", offsetMinutes: -10, neighbours: [] }],
    };
    expect(taskSlots(crossing, now).next).toBe(Date.parse("2026-10-07T23:55Z"));
  });
});
