import { type ServerSettings, withoutUnsetPrismRoles } from "@t3tools/contracts";

/**
 * One-time rewrite of project `prismRoles` overrides saved as whole kits.
 * Those builds filled every role the project never set with empty lists and
 * blank fields, so the roles ran on the caller's model instead of the
 * environment kit (#395). Empty lists, blank instructions and the default
 * `enabled` are dropped so those roles inherit again; set values are kept.
 */
export function migratePrismRoleOverrides(settings: ServerSettings): ServerSettings {
  if (settings.prismRoleOverridesPartial) return settings;
  const entries = Object.entries(settings.projectSettingsOverrides);
  if (!entries.some(([, entry]) => entry.prismRoles !== undefined)) return settings;
  const projectSettingsOverrides = Object.fromEntries(
    entries.flatMap(([projectId, entry]) => {
      const next = withoutUnsetPrismRoles(entry);
      return Object.keys(next).length === 0 ? [] : [[projectId, next]];
    }),
  );
  return { ...settings, projectSettingsOverrides, prismRoleOverridesPartial: true };
}
