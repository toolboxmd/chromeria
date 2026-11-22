import {
  isPrismRunGroupRole,
  PRISM_ROLE_DESCRIPTIONS,
  PRISM_ROLE_LABELS,
  PrismLane,
} from "@t3tools/contracts";
import { msg, T, useGT, useMessages } from "gt-react";
import { type ReactNode, useState } from "react";
import { useEnvironmentProviders } from "../../state/environments";
import {
  FlameIcon,
  MonitorIcon,
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
import { EMPTY_SERVER_PROVIDERS } from "../../state/server";
import { Button } from "../ui/button";
import { Toggle, ToggleGroup } from "../ui/toggle-group";
import { ChromeriaFeatureMark } from "../ChromeriaFeatureMark";
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
  "computer-use",
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
  "computer-use": <MonitorIcon />,
  roast: <FlameIcon />,
  reviewer: <SearchCheckIcon />,
  judge: <ScaleIcon />,
};

const LANE_LABELS: Record<PrismLane, string> = {
  easy: msg("Easy", { $context: "worker difficulty lane" }),
  medium: msg("Medium", { $context: "worker difficulty lane" }),
  hard: msg("Hard", { $context: "worker difficulty lane" }),
  ui: msg("UI", { $context: "worker lane for user interface work" }),
};

function RoleRow({
  role,
  selected,
  onSelect,
}: {
  role: PageRole;
  selected: boolean;
  onSelect: () => void;
}) {
  const gt = useGT();
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
          aria-label={gt("Select {name}", { name })}
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

/** Run N starts the role's first N Colors that can start as one Run group. */
function RunControl({
  run,
  colors,
  disabled,
  save,
}: {
  run: number;
  colors: number;
  disabled: boolean;
  save: (run: number) => void;
}) {
  const gt = useGT();
  const choices = Array.from({ length: Math.max(colors, run, 1) }, (_, index) => index + 1);
  return (
    <div className="flex flex-wrap items-center gap-3">
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium">
          <T context="noun, how many Colors one delegation starts together">Run</T>
        </p>
        <p className="text-xs text-muted-foreground">
          <T>
            Colors one delegation starts together. Above 1 they share the caller's checkout and wake
            it once when all finish.
          </T>
        </p>
      </div>
      <ToggleGroup
        aria-label={gt("Run", { $context: "noun, how many Colors one delegation starts together" })}
        variant="segmented"
        value={[String(run)]}
        disabled={disabled}
        onValueChange={(next) => {
          const chosen = Number(next[0]);
          if (Number.isInteger(chosen) && chosen >= 1 && chosen !== run) save(chosen);
        }}
      >
        {choices.map((choice) => (
          <Toggle key={choice} value={String(choice)}>
            {choice}
          </Toggle>
        ))}
      </ToggleGroup>
    </div>
  );
}

export function PrismSettings() {
  const gt = useGT();
  const resolve = useMessages();
  const { scope, target, targets, environments } = useSettingsScope();
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
  const providerChoices = colorProviderChoices(entries, options, disabledReason, resolve);
  return (
    <SettingsPageContainer width="wide" className="@container/prism gap-8">
      <SettingsSection
        id="prism-roles"
        title={gt("Prism (Model Router)")}
        titleAction={<ChromeriaFeatureMark />}
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
                  cause instanceof Error ? cause.message : gt("Could not reset role settings."),
                ),
              );
            }}
          >
            {scope.kind === "project" || scope.kind === "checkout"
              ? gt("Use environment role settings")
              : gt("Reset role settings")}
          </Button>
        }
      >
        <p className="px-3 text-sm text-muted-foreground sm:px-4">
          <T>
            The first Color in a list is primary; the rest are fallbacks. Only providers and models
            enabled in Providers for the selected scope are offered. Changes save automatically.
          </T>
        </p>
        {!target && (
          <p role="status" className="px-3 text-sm text-muted-foreground sm:px-4">
            <T>Connect an environment to edit Prism preferences.</T>
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
                  <T>Saving…</T>
                </span>
              )}
            </div>
            {selectedRole === "worker" ? (
              <>
                <ToggleGroup
                  aria-label={gt("Worker lanes")}
                  variant="segmented"
                  value={[workerLane]}
                  onValueChange={(next) => {
                    const lane = PrismLane.literals.find((candidate) => candidate === next[0]);
                    if (lane) setWorkerLane(lane);
                  }}
                >
                  {PrismLane.literals.map((lane) => (
                    <Toggle key={lane} value={lane}>
                      {resolve(LANE_LABELS[lane])}
                    </Toggle>
                  ))}
                </ToggleGroup>
                <ColorList
                  key={`worker:${workerLane}`}
                  title={gt("Worker {lane}", { lane: workerLane })}
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
            {isPrismRunGroupRole(selectedRole) && (
              <RunControl
                key={`run:${selectedRole}`}
                run={settings.prismRoles[selectedRole].run}
                colors={settings.prismRoles[selectedRole].models.length}
                disabled={!target || pendingWrite !== null}
                save={(run) => saveRole({ kind: "run", role: selectedRole, run })}
              />
            )}
          </div>
        </SettingsGroup>
      </SettingsSection>
    </SettingsPageContainer>
  );
}
