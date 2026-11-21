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
import { msg, useGT, useMessages } from "gt-react-native";
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
    msg("Existing environment resources"),
    msg("Includes other threads, within the link’s runtime permissions."),
  ],
  ["onward", msg("Delegate onward")],
  ["preview", msg("Browser access")],
  ["device", msg("Device access")],
  ["browserExport", msg("Share browser logins")],
];

function GrantEditor({
  environmentId,
  grant,
}: {
  environmentId: EnvironmentId;
  grant: PeerGrantSummary;
}) {
  const gt = useGT();
  const messages = useMessages();
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
          gt("Could not save link permissions"),
          error instanceof Error ? error.message : String(error),
        ),
    }).finally(() => setSaving(false));
  };
  return (
    <SettingsSection
      title={gt("Linked environment: {label}", { label: grant.label })}
      footer={gt(
        "New work uses these permissions. Existing work keeps its original ceiling. Removing permission also limits existing work.",
      )}
    >
      <SettingsSwitchRow
        label={gt("Full access")}
        subtitle={gt("Every permission below.")}
        value={isFullPeerGrant(draft)}
        disabled={disabled}
        onValueChange={(value) => commit({ ...draft, ...uniformPeerGrant(value) })}
      />
      <ListActionRow
        icon={advanced ? "chevron.down" : "chevron.right"}
        label={gt("Advanced", { $context: "show advanced settings" })}
        onPress={() => setAdvanced((open) => !open)}
      />
      {advanced
        ? PERMISSIONS.map(([key, label, subtitle]) => (
            <SettingsSwitchRow
              key={key}
              label={messages(label)}
              subtitle={subtitle === undefined ? undefined : messages(subtitle)}
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
