import { DEFAULT_SERVER_SETTINGS, ProjectId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import { migratePrismRoleOverrides } from "./PrismRoleOverrides.ts";

describe("migratePrismRoleOverrides", () => {
  it("keeps a deliberate empty Prism list once project overrides are partial", () => {
    const projectId = ProjectId.make("project-empty");
    const settings = {
      ...DEFAULT_SERVER_SETTINGS,
      prismRoleOverridesPartial: true,
      projectSettingsOverrides: { [projectId]: { prismRoles: { planner: { models: [] } } } },
    };
    expect(migratePrismRoleOverrides(settings)).toBe(settings);
  });
});
