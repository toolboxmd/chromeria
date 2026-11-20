import { resolveBareToggle, type BareToggleState } from "@t3tools/client-runtime/bare-launch";
import type { ServerProvider } from "@t3tools/contracts";
import { useGT } from "gt-react-native";
import { View } from "react-native";

import { AppText as Text } from "../../components/ui/AppText";
import { ThemedSwitch } from "../../components/ui/ThemedSwitch";
import { updateComposerDraftSettings, useComposerDraft } from "../../state/use-composer-drafts";

/** Fork (toolboxmd/chromeria#319): the Bare toggle, kept on the persisted new-task draft. */
export function useNewTaskBareToggle(input: {
  readonly draftKey: string | null;
  readonly provider: Pick<ServerProvider, "bare"> | null | undefined;
}) {
  const { draftKey } = input;
  const requested = useComposerDraft(draftKey).bare === true;
  const state = resolveBareToggle({
    isNewThread: draftKey !== null,
    provider: input.provider,
    requested,
  });
  const toggle = (on: boolean) => {
    if (draftKey !== null) updateComposerDraftSettings(draftKey, { bare: on ? true : undefined });
  };
  return { state, toggle };
}

export function NewTaskBareRow(props: {
  readonly state: BareToggleState;
  readonly onToggle: (on: boolean) => void;
}) {
  const gt = useGT();
  const { state } = props;
  if (state.kind === "hidden") return null;
  const available = state.kind === "available";
  const detail = available
    ? gt("Start this thread without instructions, plugins, MCP servers or skills.")
    : state.reason === null
      ? gt("This server cannot start bare threads.")
      : gt("Bare is not available for this provider: {reason}", { reason: state.reason });
  return (
    <View className="min-h-11 flex-row items-center gap-3 border-b border-border-subtle bg-grouped-card px-4 py-2">
      <View className="flex-1">
        <Text className="text-sm font-t3-medium text-foreground">{gt("Bare")}</Text>
        <Text className="text-xs text-foreground-muted">{detail}</Text>
      </View>
      <ThemedSwitch
        accessibilityLabel={gt("Bare")}
        accessibilityHint={detail}
        disabled={!available}
        onValueChange={props.onToggle}
        value={available && state.on}
      />
    </View>
  );
}
