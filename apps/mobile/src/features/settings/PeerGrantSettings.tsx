import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { useAtomValue } from "@effect/atom-react";
import { type EnvironmentId, type PeerGrantSummary } from "@t3tools/contracts";
import { useState } from "react";
import { Alert } from "react-native";
import * as AsyncResult from "effect/reactivity/AsyncResult";
import * as Option from "effect/Option";
import { peerGrants } from "../../state/peer-grants";
import { useAtomCommand } from "../../state/use-atom-command";
import { SettingsSection } from "./components/SettingsSection";
import { SettingsSwitchRow } from "./components/SettingsSwitchRow";
import { ListActionRow as SettingsActionRow } from "../../components/ui/ListActionRow";

function GrantEditor({
  environmentId,
  grant,
}: {
  environmentId: EnvironmentId;
  grant: PeerGrantSummary;
}) {
  const [draft, setDraft] = useState(grant);
  const [saving, setSaving] = useState(false);
  const canSave = useAtomValue(peerGrants.update.permissionAtom(environmentId));
  const save = useAtomCommand(peerGrants.update);
  return (
    <SettingsSection
      title={grant.label}
      footer="New work uses these permissions. Existing work keeps its original ceiling. Removing permission also limits existing work."
    >
      <SettingsSwitchRow
        label="Existing environment resources"
        subtitle="Includes other threads, within the link’s runtime permissions."
        value={draft.resourceScope === "environment-resources"}
        disabled={!canSave || saving}
        onValueChange={(value) =>
          setDraft((current) => ({
            ...current,
            resourceScope: value ? "environment-resources" : "created-work",
          }))
        }
      />
      {(
        [
          ["onward", "Delegate onward"],
          ["preview", "Browser access"],
          ["device", "Device access"],
        ] as const
      ).map(([key, label]) => (
        <SettingsSwitchRow
          key={key}
          label={label}
          value={draft[key]}
          disabled={!canSave || saving}
          onValueChange={(value) => setDraft((current) => ({ ...current, [key]: value }))}
        />
      ))}
      <SettingsActionRow
        icon="checkmark"
        label="Save link permissions"
        disabled={!canSave || saving}
        loading={saving}
        onPress={() => {
          setSaving(true);
          void save({ environmentId, input: draft })
            .then((result) => {
              if (AsyncResult.isFailure(result))
                Alert.alert(
                  "Could not save link permissions",
                  String(squashAtomCommandFailure(result)),
                );
            })
            .finally(() => setSaving(false));
        }}
      />
    </SettingsSection>
  );
}

export function PeerGrantSettings({ environmentId }: { environmentId: EnvironmentId }) {
  const result = useAtomValue(peerGrants.list({ environmentId, input: {} }));
  const grants = Option.getOrElse(AsyncResult.value(result), () => []);
  return grants.map((grant) => (
    <GrantEditor
      key={`${grant.sessionId}:${grant.resourceScope}:${grant.onward}:${grant.preview}:${grant.device}`}
      environmentId={environmentId}
      grant={grant}
    />
  ));
}
