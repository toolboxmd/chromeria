import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { applyServerSettingsPatch } from "@t3tools/shared/serverSettings";
import { describe, expect, it } from "vite-plus/test";
import {
  movePrismPreference,
  planPrismRolePatch,
  planPrismRolesReset,
  prismWriteObserved,
  syncPrismDraft,
} from "./PrismSettings.logic";
import { resolveSettingsScope } from "./settingsScope";
import { persistScopedSettingsPatch } from "./scopedSettings";

const first = { instanceId: ProviderInstanceId.make("codex"), model: "first", effort: "high" };
const second = { instanceId: ProviderInstanceId.make("opencode"), model: "second" };
const rolesLanes = {
  ...DEFAULT_SERVER_SETTINGS.prismRoles.worker.lanes,
  easy: [second],
  hard: [{ ...first, effort: "low" }],
};
const roles = {
  ...DEFAULT_SERVER_SETTINGS.prismRoles,
  worker: {
    ...DEFAULT_SERVER_SETTINGS.prismRoles.worker,
    instructions: "Keep this kit",
    skills: ["test"],
    lanes: { ...rolesLanes, medium: [first] },
  },
};
const environment = (id: string, connected = true) => ({
  environmentId: EnvironmentId.make(id),
  label: id,
  connection: { phase: connected ? ("connected" as const) : ("offline" as const) },
  serverConfig: {
    settings: { ...DEFAULT_SERVER_SETTINGS, prismRoles: roles },
    environment: { capabilities: { projectSettingsOverrides: true } },
  },
});

describe("Prism role preferences", () => {
  it("saves, clears and resets the computer-use Color list", () => {
    const envs = [environment("one")];
    const scope = resolveSettingsScope({}, [], envs);
    for (const models of [[second], []]) {
      const plan = planPrismRolePatch(scope, envs, {
        kind: "models",
        role: "computer-use",
        models,
      });
      const next = applyServerSettingsPatch(
        envs[0]!.serverConfig.settings,
        plan.serverWrites[0]!.patch,
      );
      expect(next.prismRoles["computer-use"].models).toEqual(models);
      const changed = [
        {
          ...envs[0]!,
          serverConfig: { settings: next, environment: envs[0]!.serverConfig.environment },
        },
      ];
      const reset = planPrismRolesReset(scope, changed);
      expect(
        applyServerSettingsPatch(next, reset.serverWrites[0]!.patch).prismRoles["computer-use"]
          .models,
      ).toEqual([{ instanceId: "claudeAgent", model: "claude-haiku-5-5" }]);
    }
  });

  it("moves preferences with their effort and does not mutate or wrap the list", () => {
    const original = [first, second];
    expect(movePrismPreference(original, 0, 1)).toEqual([second, first]);
    expect(original).toEqual([first, second]);
    expect(movePrismPreference(original, 0, -1)).toBe(original);
    expect(movePrismPreference(original, 1, 1)).toBe(original);
    expect(movePrismPreference([], 0, 1)).toEqual([]);
  });

  it("replaces model arrays, including clearing them, without changing role kits or offline environments", () => {
    const envs = [environment("one"), environment("two", false)];
    const scope = resolveSettingsScope({}, [], envs);
    const plan = planPrismRolePatch(scope, envs, {
      kind: "lane",
      role: "worker",
      lane: "medium",
      models: [second],
    });
    expect(plan.serverWrites.map((write) => write.environmentId)).toEqual(["one"]);
    const next = applyServerSettingsPatch(
      envs[0]!.serverConfig.settings,
      plan.serverWrites[0]!.patch,
    );
    expect(next.prismRoles.worker).toEqual({
      ...roles.worker,
      lanes: { ...rolesLanes, medium: [second] },
    });
    expect(next.prismRoles.planner).toEqual(roles.planner);
    const clear = planPrismRolePatch(scope, envs, {
      kind: "lane",
      role: "worker",
      lane: "medium",
      models: [],
    });
    expect(
      applyServerSettingsPatch(next, clear.serverWrites[0]!.patch).prismRoles.worker.lanes.medium,
    ).toEqual([]);
  });

  it("writes only the edited role to a project override and resets without touching Promachos", () => {
    const env = environment("one");
    const projectId = ProjectId.make("project");
    const member = {
      id: projectId,
      environmentId: env.environmentId,
      title: "Project",
      workspaceRoot: "/repo",
      physicalProjectKey: "one:/repo",
      environmentLabel: "one",
      defaultModelSelection: null,
      scripts: [],
      createdAt: "2026-09-25T00:00:00.000Z",
      updatedAt: "2026-09-25T00:00:00.000Z",
    };
    const scopedEnv = {
      ...env,
      serverConfig: {
        ...env.serverConfig,
        settings: {
          ...env.serverConfig.settings,
          projectSettingsOverrides: {
            [projectId]: {
              defaultAutoPull: false,
              prismRoles: {
                promachos: { models: [second] },
                worker: { instructions: "Project instructions" },
              },
            },
          },
        },
      },
    };
    const group = {
      ...member,
      projectKey: "group",
      displayName: "Project",
      memberProjects: [member],
      memberProjectRefs: [{ environmentId: env.environmentId, projectId }],
      groupedProjectCount: 1,
      environmentPresence: "remote-only" as const,
      allRemoteMembersAreDesktopLocal: false,
      allRemoteMembersAreWsl: false,
      remoteEnvironmentLabels: ["one"],
    };
    const scope = resolveSettingsScope({ project: "group" }, [group], [scopedEnv]);
    const plan = planPrismRolePatch(scope, [scopedEnv], {
      kind: "lane",
      role: "worker",
      lane: "medium",
      models: [second, first],
    });
    const next = applyServerSettingsPatch(
      scopedEnv.serverConfig.settings,
      plan.serverWrites[0]!.patch,
    );
    expect(next.prismRoles).toEqual(roles);
    expect(next.projectSettingsOverrides[projectId]).toEqual({
      defaultAutoPull: false,
      prismRoles: {
        promachos: { models: [second] },
        worker: { instructions: "Project instructions", lanes: { medium: [second, first] } },
      },
    });
    expect(resolveProjectSettings(next, projectId).settings.prismRoles.worker).toEqual({
      ...roles.worker,
      instructions: "Project instructions",
      lanes: { ...rolesLanes, medium: [second, first] },
    });

    const savedEnv = { ...scopedEnv, serverConfig: { ...scopedEnv.serverConfig, settings: next } };
    // A Run edit stores only that role's Run; the environment keeps its own.
    const runPlan = planPrismRolePatch(scope, [savedEnv], { kind: "run", role: "roast", run: 2 });
    const withRun = applyServerSettingsPatch(next, runPlan.serverWrites[0]!.patch);
    expect(withRun.projectSettingsOverrides[projectId]?.prismRoles?.roast).toEqual({ run: 2 });
    expect(resolveProjectSettings(withRun, projectId).settings.prismRoles.roast.run).toBe(2);
    expect(withRun.prismRoles.roast.run).toBe(1);
    const reset = planPrismRolesReset(scope, [savedEnv]);
    const afterReset = applyServerSettingsPatch(next, reset.serverWrites[0]!.patch);
    expect(afterReset.projectSettingsOverrides[projectId]).toEqual({
      defaultAutoPull: false,
      prismRoles: { promachos: { models: [second] } },
    });
    const resetEnv = {
      ...savedEnv,
      serverConfig: { ...savedEnv.serverConfig, settings: afterReset },
    };
    expect(prismWriteObserved(reset, [resetEnv], { kind: "inherit" })).toBe(true);
    expect(prismWriteObserved(reset, [savedEnv], { kind: "inherit" })).toBe(false);
  });

  it("reports disconnected scopes and partial save failures", async () => {
    const offline = environment("offline", false);
    const scope = resolveSettingsScope({ machine: offline.environmentId }, [], [offline]);
    expect(
      planPrismRolePatch(scope, [offline], {
        kind: "lane",
        role: "worker",
        lane: "medium",
        models: [],
      }).unavailableReason,
    ).toBeTruthy();
    const envs = [environment("one"), environment("two")];
    const plan = planPrismRolePatch(resolveSettingsScope({}, [], envs), envs, {
      kind: "lane",
      role: "worker",
      lane: "medium",
      models: [first],
    });
    const result = await persistScopedSettingsPatch(
      plan,
      async ({ environmentId }) => ({ _tag: environmentId === "one" ? "Success" : "Failure" }),
      () => {},
    );
    expect(result.savedEnvironmentCount).toBe(1);
    expect(result.failedEnvironments.map((env) => env.label)).toEqual(["two"]);
  });
});

it("keeps the same model at distinct efforts as separate ordered entries", () => {
  const envs = [environment("one")];
  const models = [first, { ...first, effort: "low" }];
  const plan = planPrismRolePatch(resolveSettingsScope({}, [], envs), envs, {
    kind: "models",
    role: "reviewer",
    models,
  });
  const next = applyServerSettingsPatch(
    envs[0]!.serverConfig.settings,
    plan.serverWrites[0]!.patch,
  );
  expect(next.prismRoles.reviewer.models).toEqual(models);
  expect(next.prismRoles.worker).toEqual(roles.worker);
});

it("saves a Planner list without touching the other read-only roles", () => {
  const envs = [environment("one")];
  const scope = resolveSettingsScope({}, [], envs);
  const planner = planPrismRolePatch(scope, envs, {
    kind: "models",
    role: "planner",
    models: [first],
  });
  const next = applyServerSettingsPatch(
    envs[0]!.serverConfig.settings,
    planner.serverWrites[0]!.patch,
  );
  expect(next.prismRoles.planner.models).toEqual([first]);
  const judge = planPrismRolePatch(scope, envs, {
    kind: "models",
    role: "judge",
    models: [second],
  });
  const both = applyServerSettingsPatch(next, judge.serverWrites[0]!.patch);
  expect(both.prismRoles.planner.models).toEqual([first]);
  expect(both.prismRoles.judge.models).toEqual([second]);
  expect(both.prismRoles.roast).toEqual(roles.roast);
  expect(both.prismRoles.researcher).toEqual(roles.researcher);
});

it("keeps the save barrier until the updated settings snapshot arrives", () => {
  const envs = [environment("one")];
  const plan = planPrismRolePatch(resolveSettingsScope({}, [], envs), envs, {
    kind: "lane",
    role: "worker",
    lane: "easy",
    models: [first],
  });
  const expectation = {
    kind: "lane" as const,
    role: "worker" as const,
    lane: "easy" as const,
    models: [first],
  };
  expect(prismWriteObserved(plan, envs, expectation)).toBe(false);
  const updated = {
    ...envs[0]!,
    serverConfig: {
      ...envs[0]!.serverConfig,
      settings: applyServerSettingsPatch(
        envs[0]!.serverConfig.settings,
        plan.serverWrites[0]!.patch,
      ),
    },
  };
  expect(prismWriteObserved(plan, [updated], expectation)).toBe(true);
  expect(prismWriteObserved(plan, [], expectation)).toBe(false);
  // Failed writes do not wait forever for a snapshot that will never arrive.
  expect(prismWriteObserved({ ...plan, serverWrites: [] }, envs, expectation)).toBe(true);
});

describe("syncPrismDraft", () => {
  const third = { instanceId: ProviderInstanceId.make("claude"), model: "third" };
  const drafted = [
    { ...second, entryId: 1 },
    { ...first, entryId: 0 },
  ];

  it("keeps the draft itself when the reordered save lands", () => {
    expect(syncPrismDraft(drafted, [second, first])).toBe(drafted);
  });

  it("restores the last saved order with the same row ids after a failed save", () => {
    expect(syncPrismDraft(drafted, [first, second])).toEqual([
      { ...first, entryId: 0 },
      { ...second, entryId: 1 },
    ]);
  });

  it("adopts a different saved list, reusing ids only for matching preferences", () => {
    expect(syncPrismDraft(drafted, [third, { ...first, effort: "low" }, second])).toEqual([
      { ...third, entryId: 2 },
      { ...first, effort: "low", entryId: 3 },
      { ...second, entryId: 1 },
    ]);
    expect(syncPrismDraft(drafted, [])).toEqual([]);
  });

  it("keeps each effort of one model on its own row id", () => {
    const efforts = [
      { ...first, effort: "low", entryId: 0 },
      { ...first, effort: "high", entryId: 1 },
    ];
    expect(
      syncPrismDraft(efforts, [
        { ...first, effort: "high" },
        { ...first, effort: "low" },
      ]),
    ).toEqual([
      { ...first, effort: "high", entryId: 1 },
      { ...first, effort: "low", entryId: 0 },
    ]);
  });
});

it("resets environment Prism roles without changing the Promachos launch kit", () => {
  const env = environment("one");
  env.serverConfig.settings.prismRoles = {
    ...roles,
    promachos: { ...roles.promachos, models: [second], instructions: "Persona" },
  };
  const plan = planPrismRolesReset(resolveSettingsScope({}, [], [env]), [env]);
  expect(plan.unavailableReason).toBeNull();
  expect(plan.serverWrites).toHaveLength(1);
  const next = applyServerSettingsPatch(env.serverConfig.settings, plan.serverWrites[0]!.patch);
  expect(next.prismRoles.promachos).toEqual(env.serverConfig.settings.prismRoles.promachos);
  expect(next.prismRoles.worker).toEqual(DEFAULT_SERVER_SETTINGS.prismRoles.worker);
  expect(prismWriteObserved(plan, [env], { kind: "inherit" })).toBe(false);
  expect(
    prismWriteObserved(plan, [{ ...env, serverConfig: { ...env.serverConfig, settings: next } }], {
      kind: "inherit",
    }),
  ).toBe(true);
});

it("adopts a tier-only change and keeps distinct Color row identities across reordering", () => {
  const standard = {
    instanceId: ProviderInstanceId.make("codex"),
    model: "sol",
    options: [{ id: "serviceTier", value: "default" }],
  };
  const fast = { ...standard, options: [{ id: "serviceTier", value: "priority" }] };
  const draft = [
    { ...standard, entryId: 1 },
    { ...fast, entryId: 2 },
  ];
  expect(syncPrismDraft(draft, [fast, standard])).toEqual([
    { ...fast, entryId: 2 },
    { ...standard, entryId: 1 },
  ]);
  expect(syncPrismDraft([draft[0]!], [fast])[0]?.options).toEqual(fast.options);
});
