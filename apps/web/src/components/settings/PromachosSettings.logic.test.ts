import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { applyServerSettingsPatch } from "@t3tools/shared/serverSettings";
import { describe, expect, it } from "vite-plus/test";
import { planPrismRolePatch, planPrismRolesReset, prismWriteObserved } from "./PrismSettings.logic";
import { planPromachosReset } from "./PromachosSettings.logic";
import type { ResolvedSettingsScope } from "./settingsScope";
import { resolveSettingsScope } from "./settingsScope";

const environmentId = EnvironmentId.make("one");
const projectId = ProjectId.make("home");
const first = { instanceId: ProviderInstanceId.make("codex"), model: "first", effort: "high" };
const second = { ...first, model: "second", effort: "low" };
const settings = {
  ...DEFAULT_SERVER_SETTINGS,
  prismRoleOverridesPartial: true,
  prismRoles: {
    ...DEFAULT_SERVER_SETTINGS.prismRoles,
    reviewer: { ...DEFAULT_SERVER_SETTINGS.prismRoles.reviewer, models: [second] },
    promachos: {
      ...DEFAULT_SERVER_SETTINGS.prismRoles.promachos,
      models: [first],
      instructions: "Persona",
    },
  },
  projectSettingsOverrides: {
    [projectId]: {
      defaultAutoPull: false,
      prismRoles: {
        worker: { instructions: "Project work", lanes: { hard: [first] } },
        promachos: { models: [second], instructions: "Project persona" },
      },
    },
  },
};
const environment = {
  environmentId,
  label: "one",
  connection: { phase: "connected" as const },
  serverConfig: { settings, environment: { capabilities: { projectSettingsOverrides: true } } },
};
const environmentScope = resolveSettingsScope({}, [], [environment]);
const member = {
  id: projectId,
  environmentId,
  title: "Home",
  workspaceRoot: "/home",
  physicalProjectKey: "one:/home",
  environmentLabel: "one",
  defaultModelSelection: null,
  scripts: [],
  createdAt: "2026-10-19T00:00:00Z",
  updatedAt: "2026-10-19T00:00:00Z",
};
const group = {
  ...member,
  projectKey: "group",
  displayName: "Home",
  memberProjects: [member],
  memberProjectRefs: [{ environmentId, projectId }],
  groupedProjectCount: 1,
  environmentPresence: "remote-only" as const,
  allRemoteMembersAreDesktopLocal: false,
  allRemoteMembersAreWsl: false,
  remoteEnvironmentLabels: ["one"],
};
const projectScope = resolveSettingsScope({ project: "group" }, [group], [environment]);
const apply = (plan: ReturnType<typeof planPromachosReset>) => {
  expect(plan.unavailableReason).toBeNull();
  expect(plan.serverWrites).toHaveLength(1);
  // updateSettings declares partial overrides at the client-runtime boundary.
  return applyServerSettingsPatch(settings, {
    ...plan.serverWrites[0]!.patch,
    prismRoleOverridesPartial: true,
  });
};

describe.each<[string, ResolvedSettingsScope]>([
  ["environment", environmentScope],
  ["project", projectScope],
])("Promachos %s settings", (_name, scope) => {
  it("saves ordered Colors, including efforts and an empty list, without touching Prism roles", () => {
    for (const models of [[second, first], []]) {
      const next = apply(
        planPrismRolePatch(scope, [environment], { kind: "models", role: "promachos", models }),
      );
      const effective =
        scope.kind === "project" ? resolveProjectSettings(next, projectId).settings : next;
      expect(effective.prismRoles.promachos.models).toEqual(models);
      expect(next.prismRoles.reviewer).toEqual(settings.prismRoles.reviewer);
      expect(next.projectSettingsOverrides[projectId]?.prismRoles?.worker).toEqual(
        settings.projectSettingsOverrides[projectId]!.prismRoles.worker,
      );
      expect(effective.prismRoles.promachos.instructions).toBe(
        scope.kind === "project" ? "Project persona" : "Persona",
      );
    }
  });

  it("resets only Promachos and waits for the streamed reset snapshot", () => {
    const plan = planPromachosReset(scope, [environment]);
    const next = apply(plan);
    expect(next.prismRoles.reviewer).toEqual(settings.prismRoles.reviewer);
    expect(next.projectSettingsOverrides[projectId]?.prismRoles?.worker).toEqual(
      settings.projectSettingsOverrides[projectId]!.prismRoles.worker,
    );
    expect(next.projectSettingsOverrides[projectId]?.defaultAutoPull).toBe(false);
    if (scope.kind === "project") {
      expect(next.projectSettingsOverrides[projectId]?.prismRoles?.promachos).toBeUndefined();
      expect(resolveProjectSettings(next, projectId).settings.prismRoles.promachos).toEqual(
        settings.prismRoles.promachos,
      );
    } else {
      expect(next.prismRoles.promachos).toEqual(DEFAULT_SERVER_SETTINGS.prismRoles.promachos);
      expect(next.projectSettingsOverrides).toEqual(settings.projectSettingsOverrides);
    }
    const expectation = { kind: "inherit" as const, roles: ["promachos" as const] };
    expect(prismWriteObserved(plan, [environment], expectation)).toBe(false);
    expect(
      prismWriteObserved(
        plan,
        [{ ...environment, serverConfig: { ...environment.serverConfig, settings: next } }],
        expectation,
      ),
    ).toBe(true);
  });

  it("Prism reset leaves the saved Promachos kit unchanged", () => {
    const next = apply(planPrismRolesReset(scope, [environment]));
    expect(next.prismRoles.promachos).toEqual(settings.prismRoles.promachos);
    expect(next.projectSettingsOverrides[projectId]?.prismRoles?.promachos).toEqual(
      settings.projectSettingsOverrides[projectId]!.prismRoles.promachos,
    );
  });
});

it("removes an otherwise empty project override on Promachos reset", () => {
  const onlyPromachos = {
    ...environment,
    serverConfig: {
      ...environment.serverConfig,
      settings: {
        ...settings,
        projectSettingsOverrides: {
          [projectId]: { prismRoles: { promachos: { models: [first] } } },
        },
      },
    },
  };
  const plan = planPromachosReset(projectScope, [onlyPromachos]);
  const next = applyServerSettingsPatch(
    onlyPromachos.serverConfig.settings,
    plan.serverWrites[0]!.patch,
  );
  expect(next.projectSettingsOverrides[projectId]).toBeUndefined();
});
