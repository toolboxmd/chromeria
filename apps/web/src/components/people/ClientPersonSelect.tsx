import {
  AuthAccessWriteScope,
  DEFAULT_PERSON,
  PEOPLE,
  type AuthSessionId,
  type EnvironmentId,
} from "@t3tools/contracts";
import { useState } from "react";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { AsyncResult } from "effect/reactivity";

import { appAtomRegistry } from "../../rpc/atomRegistry";
import { authEnvironment } from "../../state/auth";
import { useAtomCommand } from "../../state/use-atom-command";
import { environmentSession, useEnvironmentScope } from "../../state/session";
import { ChromeriaFeatureMark } from "../ChromeriaFeatureMark";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { toastManager } from "../ui/toast";

/**
 * The person a paired client acts as, in its Connections row. Unlabelled
 * clients, such as this host's own desktop app, are the default person.
 */
export function ClientPersonSelect({
  sessionId,
  person,
  current,
  environmentId,
}: {
  readonly environmentId: EnvironmentId;
  readonly sessionId: AuthSessionId;
  readonly person: string | undefined;
  /** The row is this device: its own session state carries the label too. */
  readonly current: boolean;
}) {
  const canEditPerson = useEnvironmentScope(environmentId, AuthAccessWriteScope);
  const setClientPerson = useAtomCommand(authEnvironment.setClientPerson, { reportFailure: false });
  const [saving, setSaving] = useState(false);
  const value = person ?? DEFAULT_PERSON;

  const save = async (next: string) => {
    if (!canEditPerson || saving) return;
    setSaving(true);
    await setClientPerson({ environmentId, input: { sessionId, person: next } })
      .then((result) => {
        if (AsyncResult.isFailure(result)) {
          if (!isAtomCommandInterrupted(result)) {
            const error = squashAtomCommandFailure(result);
            toastManager.add({
              type: "error",
              title: "Could not change the person",
              description: error instanceof Error ? error.message : String(error),
            });
          }
          return;
        }
        if (current) {
          appAtomRegistry.refresh(environmentSession.sessionStateAtom(environmentId));
        }
      })
      .catch((error: unknown) => {
        toastManager.add({
          type: "error",
          title: "Could not change the person",
          description: error instanceof Error ? error.message : "An error occurred.",
        });
      })
      .finally(() => {
        setSaving(false);
      });
  };

  return (
    <span className="inline-flex items-center gap-1.5">
      <ChromeriaFeatureMark />
      <Select
        value={value}
        onValueChange={(next) => {
          if (typeof next === "string" && next !== value) void save(next);
        }}
      >
        <SelectTrigger size="xs" aria-label="Person" disabled={!canEditPerson || saving}>
          <SelectValue>{value}</SelectValue>
        </SelectTrigger>
        <SelectPopup align="end" alignItemWithTrigger={false}>
          {PEOPLE.map((option) => (
            <SelectItem hideIndicator key={option} value={option}>
              {option}
            </SelectItem>
          ))}
        </SelectPopup>
      </Select>
    </span>
  );
}
