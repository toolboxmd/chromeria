import { useAtomValue } from "@effect/atom-react";
import {
  type EnvironmentId,
  isFullPeerGrant,
  type PeerGrantSummary,
  uniformPeerGrant,
} from "@t3tools/contracts";
import {
  autosavePeerGrant,
  isPeerGrantPermissionEnabled,
  peerGrantRevision,
  type PeerGrantPermission,
  setPeerGrantPermission,
} from "@t3tools/client-runtime/state/peer-grants";
import { useState } from "react";
import { Alert } from "react-native";
import * as AsyncResult from "effect/reactivity/AsyncResult";
import * as Option from "effect/Option";
import { peerGrants } from "../../state/peer-grants";
import { useAtomCommand } from "../../state/use-atom-command";
import { SettingsSection } from "./components/SettingsSection";
import { SettingsSwitchRow } from "./components/SettingsSwitchRow";
import { ListActionRow } from "../../components/ui/ListActionRow";

const PERMISSIONS: ReadonlyArray<readonly [PeerGrantPermission, string, string?]> = [
  [
    "resourceScope",
    "Existing environment resources",
    "Includes other threads, within the link’s runtime permissions.",
  ],
  ["onward", "Delegate onward"],
  ["preview", "Browser access"],
  ["device", "Device access"],
  ["browserExport", "Share browser logins"],
];

function GrantEditor({
  environmentId,
  grant,
}: {
  environmentId: EnvironmentId;
  grant: PeerGrantSummary;
}) {
  const revision = peerGrantRevision(grant);
  const [savedRevision, setSavedRevision] = useState(revision);
  const [draft, setDraft] = useState(grant);
  if (savedRevision !== revision) {
    setSavedRevision(revision);
    setDraft(grant);
  }
  const [saving, setSaving] = useState(false);
  const [advanced, setAdvanced] = useState(false);
  const canSave = useAtomValue(peerGrants.update.permissionAtom(environmentId));
  const save = useAtomCommand(peerGrants.update);
  const disabled = !canSave || saving;
  const commit = (next: PeerGrantSummary) => {
    setSaving(true);
    void autosavePeerGrant({
      previous: draft,
      next,
      show: setDraft,
      save: (input) => save({ environmentId, input }),
      onError: (error) =>
        Alert.alert(
          "Could not save link permissions",
          error instanceof Error ? error.message : String(error),
        ),
    }).finally(() => setSaving(false));
  };
  return (
    <SettingsSection
      title={`Linked environment: ${grant.label}`}
      footer="New work uses these permissions. Existing work keeps its original ceiling. Removing permission also limits existing work."
    >
      <SettingsSwitchRow
        label="Full access"
        subtitle="Every permission below."
        value={isFullPeerGrant(draft)}
        disabled={disabled}
        onValueChange={(value) => commit({ ...draft, ...uniformPeerGrant(value) })}
      />
      <ListActionRow
        icon={advanced ? "chevron.down" : "chevron.right"}
        label="Advanced"
        onPress={() => setAdvanced((open) => !open)}
      />
      {advanced
        ? PERMISSIONS.map(([key, label, subtitle]) => (
            <SettingsSwitchRow
              key={key}
              label={label}
              subtitle={subtitle}
              value={isPeerGrantPermissionEnabled(draft, key)}
              disabled={disabled}
              onValueChange={(value) => commit(setPeerGrantPermission(draft, key, value))}
            />
          ))
        : null}
    </SettingsSection>
  );
}

export function PeerGrantSettings({ environmentId }: { environmentId: EnvironmentId }) {
  const result = useAtomValue(peerGrants.list({ environmentId, input: {} }));
  const grants = Option.getOrElse(AsyncResult.value(result), () => []);
  return grants.map((grant) => (
    <GrantEditor key={grant.sessionId} environmentId={environmentId} grant={grant} />
  ));
}
