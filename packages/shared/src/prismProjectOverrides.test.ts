import { DEFAULT_SERVER_SETTINGS, ProjectId, ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { resolveProjectSettings } from "./projectSettings.ts";
import { applyServerSettingsPatch } from "./serverSettings.ts";

describe("Prism project override resolution", () => {
  const projectId = ProjectId.make("project-a");

  it("overlays a one-role Prism override and inherits every other role, also after an environment change", () => {
    const sol = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1-sol" };
    const grok = { instanceId: ProviderInstanceId.make("grok"), model: "grok-4.7" };
    const luna = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6-luna" };
    const environment = applyServerSettingsPatch(DEFAULT_SERVER_SETTINGS, {
      prismRoles: {
        reviewer: { models: [sol], instructions: "Review." },
        worker: { lanes: { medium: [grok], hard: [sol] } },
      },
      projectSettingsOverrides: {
        [projectId]: {
          defaultAutoPull: true,
          prismRoles: { worker: { lanes: { hard: [luna] } }, planner: { models: [] } },
        },
      },
    });
    const kits = resolveProjectSettings(environment, projectId).settings.prismRoles;
    expect(kits.reviewer).toMatchObject({ instructions: "Review.", skills: [], models: [sol] });
    expect(kits.worker.lanes).toMatchObject({ easy: [], medium: [grok], hard: [luna] });
    // A present empty list is the project's own value: the caller's model.
    expect(kits.planner.models).toEqual([]);

    const changed = applyServerSettingsPatch(environment, {
      prismRoles: { reviewer: { models: [grok] }, worker: { lanes: { medium: [sol] } } },
    });
    const after = resolveProjectSettings(changed, projectId).settings.prismRoles;
    expect(after.reviewer.models).toEqual([grok]);
    expect(after.worker.lanes).toMatchObject({ easy: [], medium: [sol], hard: [luna] });
    expect(resolveProjectSettings(changed, projectId).settings.defaultAutoPull).toBe(true);
  });
});

describe("Prism project override writes", () => {
  const projectId = ProjectId.make("project-prism");
  const sol = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1-sol" };
  const grok = { instanceId: ProviderInstanceId.make("grok"), model: "grok-4.7" };
  const migrated = applyServerSettingsPatch(
    { ...DEFAULT_SERVER_SETTINGS, prismRoleOverridesPartial: true },
    { prismRoles: { reviewer: { models: [sol] }, worker: { lanes: { hard: [sol] } } } },
  );
  // What a client built before partial overrides sends back after editing
  // another project setting: the decoded whole kit, unset roles as `[]`.
  const legacyEntry = {
    defaultAutoPull: true,
    prismRoles: {
      ...Object.fromEntries(
        Object.entries(DEFAULT_SERVER_SETTINGS.prismRoles).filter(
          ([role]) => role !== "computer-use",
        ),
      ),
      worker: {
        ...DEFAULT_SERVER_SETTINGS.prismRoles.worker,
        lanes: { easy: [], medium: [grok], hard: [] },
      },
    },
  };

  it("drops the empty lists of an undeclared whole-kit write, so unset roles still inherit", () => {
    const next = applyServerSettingsPatch(migrated, {
      projectSettingsOverrides: { [projectId]: legacyEntry },
    });
    expect(next.projectSettingsOverrides[projectId]).toEqual({
      defaultAutoPull: true,
      prismRoles: { worker: { lanes: { medium: [grok] } } },
    });
    const kits = resolveProjectSettings(next, projectId).settings.prismRoles;
    expect(kits.reviewer.models).toEqual([sol]);
    expect(kits.worker.lanes).toMatchObject({ easy: [], medium: [grok], hard: [sol] });
    expect(next.prismRoleOverridesPartial).toBe(true);
    const changed = applyServerSettingsPatch(next, {
      prismRoles: { "computer-use": { models: [sol] } },
    });
    expect(
      resolveProjectSettings(changed, projectId).settings.prismRoles["computer-use"].models,
    ).toEqual([sol]);
  });

  it("keeps an explicitly supplied Haiku list even in an undeclared write", () => {
    const models = [
      { instanceId: ProviderInstanceId.make("claudeAgent"), model: "claude-haiku-5-5" },
    ];
    const next = applyServerSettingsPatch(migrated, {
      projectSettingsOverrides: { [projectId]: { prismRoles: { "computer-use": { models } } } },
    });
    expect(next.projectSettingsOverrides[projectId]?.prismRoles).toEqual({
      "computer-use": { models },
    });
    const changed = applyServerSettingsPatch(next, {
      prismRoles: { "computer-use": { models: [sol] } },
    });
    expect(
      resolveProjectSettings(changed, projectId).settings.prismRoles["computer-use"].models,
    ).toEqual(models);
  });

  it("keeps a Run the project set and inherits the environment's otherwise", () => {
    const environment = applyServerSettingsPatch(migrated, { prismRoles: { roast: { run: 2 } } });
    const inherited = applyServerSettingsPatch(environment, {
      projectSettingsOverrides: { [projectId]: legacyEntry },
    });
    expect(inherited.projectSettingsOverrides[projectId]?.prismRoles).not.toHaveProperty("roast");
    expect(resolveProjectSettings(inherited, projectId).settings.prismRoles.roast.run).toBe(2);
    const own = applyServerSettingsPatch(environment, {
      projectSettingsOverrides: {
        [projectId]: { prismRoles: { ...legacyEntry.prismRoles, roast: { run: 3 } } },
      },
    });
    expect(own.projectSettingsOverrides[projectId]?.prismRoles?.roast).toEqual({ run: 3 });
    expect(resolveProjectSettings(own, projectId).settings.prismRoles.roast.run).toBe(3);
  });

  it("keeps a declared empty list and marks the overrides partial; a false flag clears nothing", () => {
    const fresh = applyServerSettingsPatch(DEFAULT_SERVER_SETTINGS, {
      projectSettingsOverrides: { [projectId]: { prismRoles: { reviewer: { models: [] } } } },
      prismRoleOverridesPartial: true,
    });
    expect(fresh.projectSettingsOverrides[projectId]?.prismRoles).toEqual({
      reviewer: { models: [] },
    });
    expect(fresh.prismRoleOverridesPartial).toBe(true);
    const undeclared = applyServerSettingsPatch(DEFAULT_SERVER_SETTINGS, {
      projectSettingsOverrides: { [projectId]: { prismRoles: { reviewer: { models: [] } } } },
    });
    expect(undeclared.projectSettingsOverrides[projectId]).toBeUndefined();
    expect(undeclared.prismRoleOverridesPartial).toBe(false);
    expect(
      applyServerSettingsPatch(fresh, { prismRoleOverridesPartial: false })
        .prismRoleOverridesPartial,
    ).toBe(true);
  });
});
