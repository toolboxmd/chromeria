import { PRISM_ROLE_DESCRIPTIONS, PRISM_ROLE_LABELS, PrismLane } from "@t3tools/contracts";
import { Link } from "@tanstack/react-router";
import { type ReactNode, useState } from "react";
import { useEnvironmentProviders } from "../../state/environments";
import {
  FlameIcon,
  HammerIcon,
  ListChecksIcon,
  PencilRulerIcon,
  ScaleIcon,
  SearchCheckIcon,
  SearchIcon,
} from "lucide-react";
import { getCustomModelOptionsByInstance } from "../../modelSelection";
import {
  applyProviderInstanceSettings,
  deriveProviderInstanceEntries,
} from "../../providerInstances";
import { cn } from "../../lib/utils";
import { useThreadShells } from "../../state/entities";
import { EMPTY_SERVER_PROVIDERS } from "../../state/server";
import { Button } from "../ui/button";
import { Toggle, ToggleGroup } from "../ui/toggle-group";
import { SettingsGroup } from "./SettingsGroup";
import { useSettingsScope } from "./SettingsScopeContext";
import { SettingsPageContainer, SettingsSection } from "./settingsLayout";
import { useScopedSettings } from "./useScopedSettings";
import { useScopedModelDisabledReason } from "./useScopedModelAvailability";
import { planPrismRolesReset } from "./PrismSettings.logic";
import { usePrismSettingsSave } from "./usePrismSettingsSave";
import { ColorList } from "./ColorList";
import { colorProviderChoices } from "./ColorPicker.logic";

const PAGE_ROLES = [
  "planner",
  "researcher",
  "designer",
  "worker",
  "roast",
  "reviewer",
  "judge",
] as const;
type PageRole = (typeof PAGE_ROLES)[number];

const ROLE_ICONS: Record<PageRole, ReactNode> = {
  planner: <ListChecksIcon />,
  researcher: <SearchIcon />,
  designer: <PencilRulerIcon />,
  worker: <HammerIcon />,
  roast: <FlameIcon />,
  reviewer: <SearchCheckIcon />,
  judge: <ScaleIcon />,
};

const LANE_LABELS: Record<PrismLane, string> = { easy: "Easy", medium: "Medium", hard: "Hard" };

function RoleRow({
  role,
  selected,
  onSelect,
}: {
  role: PageRole;
  selected: boolean;
  onSelect: () => void;
}) {
  const name = PRISM_ROLE_LABELS[role];
  return (
    <div
      data-slot="settings-row"
      className={cn(
        "group flex min-h-18 items-center gap-3 px-3 py-3 transition-colors sm:px-4",
        selected ? "bg-muted/45" : "hover:bg-muted/25",
      )}
    >
      <div className="pointer-events-none relative flex min-w-0 flex-1 items-start gap-3 rounded-md text-left">
        <button
          type="button"
          className="pointer-events-auto absolute inset-0 cursor-pointer rounded-md outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
          onClick={onSelect}
          aria-label={`Select ${name}`}
          aria-pressed={selected}
        />
        <span className="mt-0.5 flex size-4 shrink-0 text-muted-foreground [&_svg]:size-4">
          {ROLE_ICONS[role]}
        </span>
        <span className="min-w-0 flex-1">
          <span className="block truncate text-sm font-medium text-foreground">{name}</span>
          <span className="mt-0.5 line-clamp-2 text-xs leading-normal text-muted-foreground/80">
            {PRISM_ROLE_DESCRIPTIONS[role]}
          </span>
        </span>
      </div>
    </div>
  );
}

export function PrismSettings() {
  const { scope, target, targets, environments, connectedEnvironments } = useSettingsScope();
  const settings = useScopedSettings();
  const { pendingWrite, saveError, setSaveError, savePlan, saveRole } = usePrismSettingsSave();
  const [selectedRole, setSelectedRole] = useState<PageRole>("planner");
  const [workerLane, setWorkerLane] = useState<PrismLane>("medium");

  const providersByEnvironment = useEnvironmentProviders();
  const providers =
    (target && providersByEnvironment.get(target.environmentId)) ?? EMPTY_SERVER_PROVIDERS;
  const entries = applyProviderInstanceSettings(
    deriveProviderInstanceEntries(providers),
    settings,
    target?.projectId ?? null,
  );
  const options = getCustomModelOptionsByInstance(settings, providers);
  const disabledReason = useScopedModelDisabledReason(settings, entries);
  const providerChoices = colorProviderChoices(entries, options, disabledReason);
  const threads = useThreadShells();
  const thread = threads
    .filter((candidate) =>
      targets.some(
        (selected) =>
          selected.environmentId === candidate.environmentId &&
          (selected.projectId === null || selected.projectId === candidate.projectId),
      ),
    )
    .toSorted((a, b) => b.updatedAt.localeCompare(a.updatedAt))[0];
  return (
    <SettingsPageContainer width="wide" className="@container/prism gap-8">
      <SettingsSection
        id="prism-roles"
        title="Prism (Model Router)"
        variant="plain"
        headerAction={
          <Button
            size="xs"
            variant="ghost-muted"
            disabled={!target || pendingWrite !== null}
            onClick={() => {
              void savePlan(planPrismRolesReset(scope, environments), {
                kind: "inherit",
              }).catch((cause: unknown) =>
                setSaveError(
                  cause instanceof Error ? cause.message : "Could not reset role settings.",
                ),
              );
            }}
          >
            {scope.kind === "project" || scope.kind === "checkout"
              ? "Use environment role settings"
              : "Reset role settings"}
          </Button>
        }
      >
        <p className="px-3 text-sm text-muted-foreground sm:px-4">
          The first Color in a list is primary; the rest are fallbacks. Only providers and models
          enabled in Providers for the selected scope are offered. Changes save automatically.
        </p>
        {!target && (
          <p role="status" className="px-3 text-sm text-muted-foreground sm:px-4">
            Connect an environment to edit Prism preferences.
          </p>
        )}
        {saveError && (
          <p role="alert" className="px-3 text-sm text-destructive sm:px-4">
            {saveError}
          </p>
        )}
        <SettingsGroup
          divided={false}
          className="overflow-hidden @min-[48rem]/prism:grid @min-[48rem]/prism:min-h-[28rem] @min-[48rem]/prism:grid-cols-[17rem_minmax(0,1fr)]"
        >
          <div className="divide-y divide-border/50 border-b border-border/60 bg-muted/10 @min-[48rem]/prism:border-r @min-[48rem]/prism:border-b-0">
            {PAGE_ROLES.map((role) => (
              <RoleRow
                key={role}
                role={role}
                selected={selectedRole === role}
                onSelect={() => setSelectedRole(role)}
              />
            ))}
          </div>
          <div id={`prism-${selectedRole}`} className="min-w-0 space-y-4 p-4">
            <div className="flex items-start gap-3">
              <span className="mt-0.5 flex size-4 shrink-0 text-muted-foreground [&_svg]:size-4">
                {ROLE_ICONS[selectedRole]}
              </span>
              <div className="min-w-0 flex-1">
                <h3 className="text-sm font-medium">{PRISM_ROLE_LABELS[selectedRole]}</h3>
                <p className="text-xs text-muted-foreground">
                  {PRISM_ROLE_DESCRIPTIONS[selectedRole]}
                </p>
              </div>
              {pendingWrite && (
                <span role="status" className="shrink-0 text-xs text-muted-foreground">
                  Saving…
                </span>
              )}
            </div>
            {selectedRole === "worker" ? (
              <>
                <ToggleGroup
                  aria-label="Worker lanes"
                  variant="segmented"
                  value={[workerLane]}
                  onValueChange={(next) => {
                    const lane = PrismLane.literals.find((candidate) => candidate === next[0]);
                    if (lane) setWorkerLane(lane);
                  }}
                >
                  {PrismLane.literals.map((lane) => (
                    <Toggle key={lane} value={lane}>
                      {LANE_LABELS[lane]}
                    </Toggle>
                  ))}
                </ToggleGroup>
                <ColorList
                  key={`worker:${workerLane}`}
                  title={`Worker ${workerLane}`}
                  saved={settings.prismRoles.worker.lanes[workerLane]}
                  providers={providerChoices}
                  disabled={!target}
                  saving={pendingWrite !== null}
                  mixed={targets.some(
                    (candidate) =>
                      JSON.stringify(candidate.settings.prismRoles.worker.lanes[workerLane]) !==
                      JSON.stringify(settings.prismRoles.worker.lanes[workerLane]),
                  )}
                  save={(models) =>
                    saveRole({ kind: "lane", role: "worker", lane: workerLane, models })
                  }
                />
              </>
            ) : (
              <ColorList
                key={selectedRole}
                title={PRISM_ROLE_LABELS[selectedRole]}
                saved={settings.prismRoles[selectedRole].models}
                providers={providerChoices}
                disabled={!target}
                saving={pendingWrite !== null}
                mixed={targets.some(
                  (candidate) =>
                    JSON.stringify(candidate.settings.prismRoles[selectedRole].models) !==
                    JSON.stringify(settings.prismRoles[selectedRole].models),
                )}
                save={(models) => saveRole({ kind: "models", role: selectedRole, models })}
              />
            )}
          </div>
        </SettingsGroup>
      </SettingsSection>
      <SettingsSection id="prism-capacity" title="Live capacity">
        <div className="space-y-4 px-3 sm:px-4">
          {connectedEnvironments.map((env) => (
            <div key={env.environmentId} className="space-y-3">
              <h3 className="text-sm font-medium">{env.label}</h3>
              {(providersByEnvironment.get(env.environmentId) ?? EMPTY_SERVER_PROVIDERS).map(
                (provider) => (
                  <div key={provider.instanceId} className="space-y-2 rounded-lg border p-3">
                    <p className="text-sm">
                      {provider.displayName ?? provider.instanceId}
                      {!provider.enabled ? " (disabled)" : ""}
                    </p>
                    {provider.usageLimits?.checkedAt && (
                      <p className="text-xs text-muted-foreground">
                        Checked {new Date(provider.usageLimits.checkedAt).toLocaleString()}
                      </p>
                    )}
                    {provider.usageLimits?.unavailable && (
                      <p className="text-xs text-muted-foreground">
                        {provider.usageLimits.unavailable.message ??
                          (provider.usageLimits.unavailable.reason === "probeFailed"
                            ? "Usage refresh failed; showing the last reading if available."
                            : "Usage reporting is unsupported.")}
                      </p>
                    )}
                    {!provider.usageLimits?.windows.length && (
                      <p className="text-xs text-muted-foreground">No usage windows reported.</p>
                    )}
                    {provider.usageLimits?.windows.map((window) => (
                      <div key={window.id} className="space-y-1">
                        <div className="flex flex-wrap justify-between gap-2 text-xs">
                          <span>
                            {window.label}: {window.usedPercent}% used
                          </span>
                          <span className="text-muted-foreground">
                            {window.resetsAt
                              ? `Resets ${new Date(window.resetsAt).toLocaleString()}`
                              : "Reset time not reported"}
                          </span>
                        </div>
                        <meter
                          className="h-2 w-full"
                          aria-label={`${provider.displayName ?? provider.instanceId} ${window.label} used`}
                          min={0}
                          max={100}
                          value={window.usedPercent}
                        />
                      </div>
                    ))}
                  </div>
                ),
              )}
            </div>
          ))}
          {connectedEnvironments.length === 0 && (
            <p className="text-sm text-muted-foreground">
              Connect an environment to see its capacity.
            </p>
          )}
        </div>
      </SettingsSection>
      <SettingsSection id="prism-jobs" title="Recent Prism jobs">
        <div className="space-y-2 px-3 text-sm text-muted-foreground sm:px-4">
          <p>
            Recent jobs are not included in this snapshot. Open a thread to see its delegated tasks
            in Lineage.
          </p>
          {thread ? (
            <Link
              className="text-primary underline"
              to="/$environmentId/$threadId"
              params={{ environmentId: thread.environmentId, threadId: thread.id }}
            >
              Open thread
            </Link>
          ) : (
            <Link className="text-primary underline" to="/">
              Open a thread to view Lineage
            </Link>
          )}
        </div>
      </SettingsSection>
    </SettingsPageContainer>
  );
}
