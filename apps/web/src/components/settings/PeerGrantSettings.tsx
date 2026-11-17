import { useAtomValue } from "@effect/atom-react";
import {
  type EnvironmentId,
  isFullPeerGrant,
  type PeerGrantSummary,
  uniformPeerGrant,
} from "@t3tools/contracts";
import { ChevronRightIcon } from "lucide-react";
import {
  autosavePeerGrant,
  isPeerGrantPermissionEnabled,
  setPeerGrantPermission,
} from "@t3tools/client-runtime/state/peer-grants";
import { toastManager } from "../ui/toast";
import { useState } from "react";
import * as AsyncResult from "effect/reactivity/AsyncResult";
import * as Option from "effect/Option";
import { peerGrants, updatePeerGrant } from "../../state/peerGrants";
import { useAtomCommand } from "../../state/use-atom-command";
import { ChromeriaTitle } from "../ChromeriaFeatureMark";
import { Checkbox } from "../ui/checkbox";
import { Switch } from "../ui/switch";
import { SettingsRow, SettingsSection } from "./settingsLayout";

const PERMISSIONS = [
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
  ["preview", "Browser access", "Allow permitted browser tools and private browser output."],
  ["device", "Device access", "Allow permitted simulator and emulator tools and output."],
  [
    "browserExport",
    "Share browser logins",
    "Let its agents copy this environment's browser sessions for the sites they name.",
  ],
] as const;

function GrantEditor({
  environmentId,
  grant,
}: {
  environmentId: EnvironmentId;
  grant: PeerGrantSummary;
}) {
  const [draft, setDraft] = useState(grant);
  const [saving, setSaving] = useState(false);
  const canSave = useAtomValue(updatePeerGrant.permissionAtom(environmentId));
  const save = useAtomCommand(updatePeerGrant);
  const disabled = !canSave || saving;
  const commit = (next: PeerGrantSummary) => {
    setSaving(true);
    void autosavePeerGrant({
      previous: draft,
      next,
      show: setDraft,
      save: (input) => save({ environmentId, input }),
      onError: (error) =>
        toastManager.add({
          type: "error",
          title: "Could not save link permissions",
          description: error instanceof Error ? error.message : String(error),
        }),
    }).finally(() => setSaving(false));
  };
  const full = isFullPeerGrant(draft);
  const none = PERMISSIONS.every(([key]) => !isPeerGrantPermissionEnabled(draft, key));
  return (
    <div>
      <SettingsRow
        title={<ChromeriaTitle>{grant.label}</ChromeriaTitle>}
        description="Full access lets this machine's agents use every permission below. Removing permission also limits existing work."
        control={
          <Switch
            aria-label={`Full access for ${grant.label}`}
            checked={full}
            mixed={!full && !none}
            disabled={disabled}
            onCheckedChange={(checked) => commit({ ...draft, ...uniformPeerGrant(checked) })}
          />
        }
      />
      <details className="group px-3 pb-2 sm:px-4">
        <summary className="flex min-h-9 cursor-pointer list-none items-center gap-2 rounded-md text-xs font-medium text-muted-foreground outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring [&::-webkit-details-marker]:hidden">
          Advanced
          <ChevronRightIcon aria-hidden className="size-3 group-open:rotate-90" />
        </summary>
        <fieldset disabled={disabled} className="space-y-3 pb-2">
          <p className="text-xs text-muted-foreground">
            New work uses these permissions. Existing delegated work keeps its original ceiling.
          </p>
          {PERMISSIONS.map(([key, title, description]) => (
            <label key={key} className="flex items-start gap-3 text-xs">
              <Checkbox
                checked={isPeerGrantPermissionEnabled(draft, key)}
                onCheckedChange={(checked) =>
                  commit(setPeerGrantPermission(draft, key, checked === true))
                }
              />
              <span>
                <span className="block font-medium">{title}</span>
                <span className="text-muted-foreground">{description}</span>
              </span>
            </label>
          ))}
        </fieldset>
      </details>
    </div>
  );
}

export function PeerGrantSettings({ environmentId }: { environmentId: EnvironmentId }) {
  const result = useAtomValue(peerGrants({ environmentId, input: {} }));
  const grants: ReadonlyArray<PeerGrantSummary> = Option.getOrElse(
    AsyncResult.value(result),
    () => [],
  );
  if (grants.length === 0) return null;
  return (
    <SettingsSection id="linked-environments" title="Linked environments">
      {grants.map((grant) => (
        <GrantEditor
          key={`${grant.sessionId}:${grant.resourceScope}:${grant.onward}:${grant.preview}:${grant.device}:${grant.browserExport}`}
          environmentId={environmentId}
          grant={grant}
        />
      ))}
    </SettingsSection>
  );
}
