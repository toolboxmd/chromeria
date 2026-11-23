import { useAtomValue } from "@effect/atom-react";
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
import { T, useGT, Var } from "gt-react";
import { useState } from "react";

import { connectionAtomRuntime } from "../../connection/runtime";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Input } from "../ui/input";

const createHome = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "promachos:create-home",
  tag: PROMACHOS_HOME_WS_METHODS.create,
});

/**
 * **New home** for one environment in Settings → Promachos: creates a starter home folder on that
 * environment's machine (toolboxmd/chromeria#139), then `onCreated` makes it that machine's home.
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
  const gt = useGT();
  const create = useAtomCommand(createHome, { reportFailure: false });
  const canCreate = useAtomValue(createHome.permissionAtom(environmentId));
  // Null while the form is closed.
  const [path, setPath] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  if (path === null) {
    return (
      <Button
        size="xs"
        variant="outline"
        disabled={!canCreate}
        onClick={() => setPath(DEFAULT_PROMACHOS_HOME_PATH)}
      >
        <PlusIcon />
        {label}
      </Button>
    );
  }

  const submit = async () => {
    if (!canCreate || path.trim().length === 0 || pending) return;
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
    setError(failure instanceof Error ? failure.message : gt("The home could not be created."));
  };

  return (
    <form
      className="flex w-full flex-col gap-1.5"
      onSubmit={(event) => {
        event.preventDefault();
        void submit();
      }}
    >
      <span className="text-muted-foreground text-xs">
        <T>
          <Var>{label}</Var>: folder for the new home
        </T>
      </span>
      <Input
        aria-label={gt("Folder for the new home")}
        autoFocus
        font="mono"
        size="sm"
        value={path}
        disabled={pending}
        onChange={(event) => setPath(event.target.value)}
      />
      {error === null ? null : <p className="text-destructive text-xs">{error}</p>}
      <div className="flex gap-1">
        <Button
          size="sm"
          type="submit"
          disabled={!canCreate || pending || path.trim().length === 0}
        >
          {pending ? gt("Creating…") : gt("Create", { $context: "button, creates a new home" })}
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
          <T>Cancel</T>
        </Button>
      </div>
    </form>
  );
}
