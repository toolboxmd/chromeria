import {
  createEnvironmentRpcCommand,
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import {
  DEFAULT_PROMACHOS_HOME_PATH,
  type EnvironmentId,
  PROMACHOS_HOME_WS_METHODS,
  type ScopedProjectRef,
} from "@t3tools/contracts";
import { PlusIcon } from "lucide-react";
import { useState } from "react";

import { connectionAtomRuntime } from "../../connection/runtime";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { SidebarMenuButton, SidebarMenuItem } from "../ui/sidebar";

const createHome = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "promachos:create-home",
  tag: PROMACHOS_HOME_WS_METHODS.create,
});

/**
 * The picker's **New home** entry for one environment: creates a starter home folder on that
 * environment's machine (toolboxmd/chromeria#139) and selects it.
 */
export function PromachosNewHome({
  environmentId,
  label,
  onCreated,
}: {
  environmentId: EnvironmentId;
  label: string;
  onCreated: (home: ScopedProjectRef) => void;
}) {
  const create = useAtomCommand(createHome, { reportFailure: false });
  // Null while the form is closed.
  const [path, setPath] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (path === null) {
    return (
      <SidebarMenuItem>
        <SidebarMenuButton onClick={() => setPath(DEFAULT_PROMACHOS_HOME_PATH)}>
          <PlusIcon />
          <span>{label}</span>
        </SidebarMenuButton>
      </SidebarMenuItem>
    );
  }

  const submit = async () => {
    if (path.trim().length === 0 || pending) return;
    setPending(true);
    setError(null);
    const result = await create({ environmentId, input: { path: path.trim() } });
    setPending(false);
    if (result._tag === "Success") {
      onCreated({ environmentId, projectId: result.value.projectId });
      setPath(null);
      return;
    }
    if (isAtomCommandInterrupted(result)) return;
    const failure = squashAtomCommandFailure(result);
    setError(failure instanceof Error ? failure.message : "The home could not be created.");
  };

  return (
    <SidebarMenuItem>
      <form
        className="flex flex-col gap-1.5 px-2 py-1"
        onSubmit={(event) => {
          event.preventDefault();
          void submit();
        }}
      >
        <span className="text-muted-foreground text-xs">{label}: folder for the new home</span>
        <Input
          aria-label="Folder for the new home"
          autoFocus
          font="mono"
          size="sm"
          value={path}
          disabled={pending}
          onChange={(event) => setPath(event.target.value)}
        />
        {error === null ? null : <p className="text-destructive text-xs">{error}</p>}
        <div className="flex gap-1">
          <Button size="sm" type="submit" disabled={pending || path.trim().length === 0}>
            {pending ? "Creating…" : "Create"}
          </Button>
          <Button
            size="sm"
            variant="ghost"
            type="button"
            disabled={pending}
            onClick={() => {
              setPath(null);
              setError(null);
            }}
          >
            Cancel
          </Button>
        </div>
      </form>
    </SidebarMenuItem>
  );
}
