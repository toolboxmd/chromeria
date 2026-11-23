import { useAtomValue } from "@effect/atom-react";
import { type EnvironmentId, ProjectId, type ServerSettings } from "@t3tools/contracts";
import { T, useGT } from "gt-react";

import { useEnvironments } from "../../state/environments";
import { useProjects, useServerConfigs } from "../../state/entities";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { PromachosNewHome } from "../promachos/PromachosNewHome";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { SettingsRow, SettingsSection } from "./settingsLayout";

const NO_HOME = "none";

/**
 * Settings → Promachos → Home: each connected machine names which of its projects is the Promachos
 * home. The choice is that machine's server setting, so every client connected to it follows.
 */
export function PromachosHomeSettings() {
  const gt = useGT();
  const { environments } = useEnvironments();
  const configs = useServerConfigs();
  const connected = environments.filter((environment) => configs.has(environment.environmentId));
  return (
    <SettingsSection id="promachos-home" title={gt("Home")}>
      {connected.length === 0 ? (
        <p role="status" className="px-4 py-3 text-sm text-muted-foreground">
          <T>Connect an environment to choose the Promachos home.</T>
        </p>
      ) : (
        connected.map((environment) => (
          <PromachosHomeRow
            key={environment.environmentId}
            environmentId={environment.environmentId}
            title={connected.length > 1 ? environment.label : gt("Promachos home")}
            settings={configs.get(environment.environmentId)!.settings}
            newHomeLabel={gt("New home")}
          />
        ))
      )}
    </SettingsSection>
  );
}

function PromachosHomeRow({
  environmentId,
  title,
  settings,
  newHomeLabel,
}: {
  environmentId: EnvironmentId;
  title: string;
  settings: Pick<ServerSettings, "promachosHome">;
  newHomeLabel: string;
}) {
  const gt = useGT();
  const projects = useProjects().filter((project) => project.environmentId === environmentId);
  const canWrite = useAtomValue(serverEnvironment.updateSettings.permissionAtom(environmentId));
  const update = useAtomCommand(serverEnvironment.updateSettings, "Promachos home update");
  const save = (promachosHome: ProjectId | null) =>
    void update({ environmentId, input: { patch: { promachosHome } } });
  const home = settings.promachosHome;
  const missing = home !== null && !projects.some((project) => project.id === home);
  const titleOf = (value: string | null) =>
    value === null || value === NO_HOME
      ? gt("Not set")
      : (projects.find((project) => project.id === value)?.title ?? gt("Unavailable project"));
  return (
    <SettingsRow
      title={title}
      description={
        missing
          ? gt("The Promachos home is not available here. Choose it again.")
          : gt("His conversations live in this project, on this machine.")
      }
      control={
        <Select
          value={home ?? NO_HOME}
          disabled={!canWrite}
          onValueChange={(value) => {
            if (typeof value === "string") save(value === NO_HOME ? null : ProjectId.make(value));
          }}
        >
          <SelectTrigger
            size="sm"
            aria-label={gt("Promachos home on {machine}", { machine: title })}
          >
            <SelectValue>{titleOf}</SelectValue>
          </SelectTrigger>
          <SelectPopup align="end" alignItemWithTrigger={false}>
            <SelectItem value={NO_HOME}>{gt("Not set")}</SelectItem>
            {projects.map((project) => (
              <SelectItem key={project.id} value={project.id}>
                {project.title}
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      }
    >
      <div className="flex pt-2">
        <PromachosNewHome
          environmentId={environmentId}
          label={newHomeLabel}
          onCreated={(created) => save(created.projectId)}
        />
      </div>
    </SettingsRow>
  );
}
