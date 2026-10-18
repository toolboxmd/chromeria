import {
  overlayPrismRoleKits,
  ProjectId,
  type Color,
  type PrismRole,
  type PrismRoleKits,
  type PrismRoleKitsPatch,
  type PrismLane,
  type PrismSwitchableRole,
  type ProjectSettingsOverrides,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import { colorKey } from "./ColorPicker.logic";
import { planScopedSettingsClear, planScopedSettingsPatch } from "./scopedSettings";

export function movePrismPreference<T extends Color>(
  models: readonly T[],
  index: number,
  direction: -1 | 1,
) {
  const destination = index + direction;
  if (index < 0 || index >= models.length || destination < 0 || destination >= models.length)
    return models;
  const next = [...models];
  [next[index], next[destination]] = [next[destination]!, next[index]!];
  return next;
}

/** A list row while editing; `entryId` keeps drag identity stable across saves. */
export type PrismDraftPreference = Color & { entryId: number };

const samePreference = (a: Color, b: Color) => colorKey(a) === colorKey(b) && a.effort === b.effort;

/** Row ids only need to be unique within their list. */
export const nextPrismEntryId = (draft: readonly PrismDraftPreference[]) =>
  Math.max(-1, ...draft.map((entry) => entry.entryId)) + 1;

/**
 * Adopts the saved list into the draft. Returns the same draft when nothing differs, and
 * otherwise reuses the ids of matching rows so a landed save never remounts them.
 */
export function syncPrismDraft(
  draft: readonly PrismDraftPreference[],
  saved: readonly Color[],
): readonly PrismDraftPreference[] {
  if (
    draft.length === saved.length &&
    draft.every((entry, index) => samePreference(entry, saved[index]!))
  )
    return draft;
  const unused = [...draft];
  let nextEntryId = nextPrismEntryId(draft);
  return saved.map((model) => {
    const match = unused.findIndex((entry) => samePreference(entry, model));
    const entryId = match >= 0 ? unused.splice(match, 1)[0]!.entryId : nextEntryId++;
    return { ...model, entryId };
  });
}

/** One autosaved edit on the Prism page. */
export type PrismRoleUpdate =
  | { kind: "lane"; role: "worker"; lane: PrismLane; models: readonly Color[] }
  | {
      kind: "models";
      role: Exclude<PrismRole, "worker">;
      models: readonly Color[];
    }
  | { kind: "enabled"; role: PrismSwitchableRole; enabled: boolean };

function prismRolePatch(update: PrismRoleUpdate): PrismRoleKitsPatch {
  switch (update.kind) {
    case "lane":
      return { worker: { lanes: { [update.lane]: update.models } } };
    case "models":
      return { [update.role]: { models: update.models } };
    case "enabled":
      return { [update.role]: { enabled: update.enabled } };
  }
}

function applyPrismRoleUpdate(kits: PrismRoleKits, update: PrismRoleUpdate): PrismRoleKits {
  return overlayPrismRoleKits(kits, prismRolePatch(update));
}

/** Rewrite each project entry of a scoped write; environment writes pass through. */
function withProjectEntries(
  plan: ReturnType<typeof planScopedSettingsPatch>,
  environments: Parameters<typeof planScopedSettingsPatch>[1],
  entry: (
    written: ProjectSettingsOverrides | null,
    stored: ProjectSettingsOverrides | undefined,
  ) => ProjectSettingsOverrides | null,
): ReturnType<typeof planScopedSettingsPatch> {
  return {
    ...plan,
    serverWrites: plan.serverWrites.map((write) => {
      const overrides = write.patch.projectSettingsOverrides;
      const settings = environments.find((env) => env.environmentId === write.environmentId)
        ?.serverConfig?.settings;
      if (!overrides || !settings) return write;
      return {
        ...write,
        patch: {
          ...write.patch,
          projectSettingsOverrides: Object.fromEntries(
            Object.entries(overrides).map(([id, written]) => [
              id,
              entry(written, settings.projectSettingsOverrides[ProjectId.make(id)]),
            ]),
          ),
        },
      };
    }),
  };
}

/** One role edit; a project override stores only the edited role field or lane. */
export function planPrismRolePatch(
  scope: Parameters<typeof planScopedSettingsPatch>[0],
  environments: Parameters<typeof planScopedSettingsPatch>[1],
  update: PrismRoleUpdate,
): ReturnType<typeof planScopedSettingsPatch> {
  const patch = prismRolePatch(update);
  // The generic planner completes object values from the effective kits;
  // a project's Prism override keeps only what it set.
  return withProjectEntries(
    planScopedSettingsPatch(scope, environments, { prismRoles: patch }),
    environments,
    (written, stored) =>
      written && {
        ...written,
        prismRoles: overlayPrismRoleKits(stored?.prismRoles ?? {}, patch),
      },
  );
}

/** "Use environment role settings": drop the project's role overrides, keeping its Promachos kit. */
export function planPrismRolesReset(
  scope: Parameters<typeof planScopedSettingsPatch>[0],
  environments: Parameters<typeof planScopedSettingsPatch>[1],
): ReturnType<typeof planScopedSettingsPatch> {
  return withProjectEntries(
    planScopedSettingsClear(scope, environments, ["prismRoles"]),
    environments,
    (written, stored) => {
      const promachos = stored?.prismRoles?.promachos;
      return promachos ? { ...written, prismRoles: { promachos } } : written;
    },
  );
}

export type PrismWriteExpectation = PrismRoleUpdate | { kind: "inherit" };

/** A successful RPC is not enough: wait for the streamed settings before building another override. */
export function prismWriteObserved(
  plan: ReturnType<typeof planScopedSettingsPatch>,
  environments: Parameters<typeof planScopedSettingsPatch>[1],
  expectation: PrismWriteExpectation,
) {
  return plan.serverWrites.every((write) => {
    const settings = environments.find((env) => env.environmentId === write.environmentId)
      ?.serverConfig?.settings;
    if (!settings) return false;
    const projectIds = Object.keys(write.patch.projectSettingsOverrides ?? {});
    if (expectation.kind === "inherit") {
      return projectIds.every((id) =>
        Object.keys(settings.projectSettingsOverrides[ProjectId.make(id)]?.prismRoles ?? {}).every(
          (role) => role === "promachos",
        ),
      );
    }
    const effective = projectIds.length
      ? projectIds.map((id) => resolveProjectSettings(settings, ProjectId.make(id)).settings)
      : [settings];
    return effective.every(
      (target) =>
        JSON.stringify(target.prismRoles[expectation.role]) ===
        JSON.stringify(applyPrismRoleUpdate(target.prismRoles, expectation)[expectation.role]),
    );
  });
}
