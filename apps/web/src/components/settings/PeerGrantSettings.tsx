import { useAtomValue } from "@effect/atom-react";
import { type EnvironmentId, type PeerGrant, type PeerGrantSummary } from "@t3tools/contracts";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { toastManager } from "../ui/toast";
import { useState } from "react";
import * as AsyncResult from "effect/reactivity/AsyncResult";
import * as Option from "effect/Option";
import { peerGrants, updatePeerGrant } from "../../state/peerGrants";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Checkbox } from "../ui/checkbox";

function GrantEditor({
  environmentId,
  grant,
  label,
}: {
  environmentId: EnvironmentId;
  grant: PeerGrant;
  label: string;
}) {
  const [draft, setDraft] = useState(grant);
  const [saving, setSaving] = useState(false);
  const canSave = useAtomValue(updatePeerGrant.permissionAtom(environmentId));
  const save = useAtomCommand(updatePeerGrant);
  return (
    <fieldset disabled={!canSave || saving} className="space-y-3 border-t border-border px-4 py-3">
      <legend className="text-sm font-medium">{label}</legend>
      <p className="text-xs text-muted-foreground">
        New work uses these permissions. Existing delegated work keeps its original ceiling.
        Removing permission also limits existing work.
      </p>
      {(
        [
          [
            "resourceScope",
            "Access existing environment resources",
            "Includes other threads, within this link’s runtime permissions.",
          ],
          [
            "onward",
            "Delegate onward",
            "Allow children to use explicitly selected links to other environments.",
          ],
          [
            "preview",
            "Browser access",
            "Allow permitted browser tools and private browser output.",
          ],
          ["device", "Device access", "Allow permitted simulator and emulator tools and output."],
        ] as const
      ).map(([key, title, description]) => (
        <label key={key} className="flex items-start gap-3 text-xs">
          <Checkbox
            checked={
              key === "resourceScope" ? draft.resourceScope === "environment-resources" : draft[key]
            }
            onCheckedChange={(checked) =>
              setDraft((previous) => ({
                ...previous,
                [key]:
                  key === "resourceScope"
                    ? checked
                      ? "environment-resources"
                      : "created-work"
                    : checked === true,
              }))
            }
          />
          <span>
            <span className="block font-medium">{title}</span>
            <span className="text-muted-foreground">{description}</span>
          </span>
        </label>
      ))}
      <Button
        size="xs"
        variant="outline"
        onClick={async () => {
          setSaving(true);
          try {
            const result = await save({ environmentId, input: draft });
            if (AsyncResult.isFailure(result) && !isAtomCommandInterrupted(result)) {
              const error = squashAtomCommandFailure(result);
              toastManager.add({
                type: "error",
                title: "Could not save link permissions",
                description: error instanceof Error ? error.message : String(error),
              });
            }
          } finally {
            setSaving(false);
          }
        }}
      >
        Save link permissions
      </Button>
    </fieldset>
  );
}

export function PeerGrantSettings({ environmentId }: { environmentId: EnvironmentId }) {
  const result = useAtomValue(peerGrants({ environmentId, input: {} }));
  const grants: ReadonlyArray<PeerGrantSummary> = Option.getOrElse(
    AsyncResult.value(result),
    () => [],
  );
  return grants.map((grant) => (
    <GrantEditor
      key={`${grant.sessionId}:${grant.resourceScope}:${grant.onward}:${grant.preview}:${grant.device}`}
      environmentId={environmentId}
      grant={grant}
      label={grant.label}
    />
  ));
}
