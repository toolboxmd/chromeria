import { DEFAULT_PERSON, PEOPLE, type AuthSessionId } from "@t3tools/contracts";
import { useState } from "react";

import { setServerClientSessionPerson } from "../../environments/primary";
import { appAtomRegistry } from "../../rpc/atomRegistry";
import { usePrimaryEnvironmentId } from "../../state/environments";
import { environmentSession } from "../../state/session";
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
}: {
  readonly sessionId: AuthSessionId;
  readonly person: string | undefined;
  /** The row is this device: its own session state carries the label too. */
  readonly current: boolean;
}) {
  const primaryEnvironmentId = usePrimaryEnvironmentId();
  const [saving, setSaving] = useState(false);
  const value = person ?? DEFAULT_PERSON;

  const save = async (next: string) => {
    setSaving(true);
    try {
      await setServerClientSessionPerson(sessionId, next);
      if (current && primaryEnvironmentId !== null) {
        appAtomRegistry.refresh(environmentSession.sessionStateAtom(primaryEnvironmentId));
      }
    } catch (error) {
      toastManager.add({
        type: "error",
        title: "Could not change the person",
        description: error instanceof Error ? error.message : "An error occurred.",
      });
    } finally {
      setSaving(false);
    }
  };

  return (
    <Select
      value={value}
      onValueChange={(next) => {
        if (typeof next === "string" && next !== value) void save(next);
      }}
    >
      <SelectTrigger size="xs" aria-label="Person" disabled={saving}>
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
  );
}
