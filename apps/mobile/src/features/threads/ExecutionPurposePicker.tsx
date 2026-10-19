import { useState } from "react";
import { Pressable, TextInput, View } from "react-native";
import { AppText as Text } from "../../components/ui/AppText";
import {
  type ExecutionPurposeScope,
  type OrchestrationV2Run,
  executionPurposeOf,
  parseTrackingReferenceUrl,
  purposeScopeLabel,
} from "@t3tools/contracts";

export function ExecutionPurposePicker(props: {
  readonly scope: ExecutionPurposeScope;
  readonly disabled?: boolean;
  readonly runs?: ReadonlyArray<Pick<OrchestrationV2Run, "id" | "ordinal" | "purpose">>;
  readonly onChange: (scope: ExecutionPurposeScope) => void;
}) {
  const [open, setOpen] = useState(false);
  const [url, setUrl] = useState("");
  const [error, setError] = useState<string | null>(null);
  return (
    <View className="gap-2 px-3 py-2">
      <Pressable
        accessibilityRole="button"
        accessibilityLabel="Change execution purpose"
        disabled={props.disabled}
        onPress={() => setOpen(!open)}
      >
        <Text className="text-sm text-foreground-muted">
          Purpose: {purposeScopeLabel(props.scope)}
        </Text>
      </Pressable>
      {open ? (
        <View className="gap-2">
          <TextInput
            accessibilityLabel="Issue or PR purpose URL"
            value={url}
            onChangeText={setUrl}
            placeholder="Issue or PR URL"
            autoCapitalize="none"
            autoCorrect={false}
            className="rounded-lg border border-border bg-background px-3 py-2 text-foreground"
          />
          <View className="flex-row gap-4">
            <Pressable
              accessibilityRole="button"
              disabled={props.disabled}
              onPress={() => {
                const reference = parseTrackingReferenceUrl(url);
                if (reference === null) {
                  setError("Enter a valid Issue or PR URL.");
                  return;
                }
                props.onChange({ type: "single", reference });
                setOpen(false);
                setError(null);
                setUrl("");
              }}
            >
              <Text className="text-sm text-foreground">Select purpose</Text>
            </Pressable>
            <Pressable
              accessibilityRole="button"
              disabled={props.disabled}
              onPress={() => {
                props.onChange({ type: "unscoped", reason: "deliberate" });
                setOpen(false);
                setError(null);
              }}
            >
              <Text className="text-sm text-foreground">Clear purpose</Text>
            </Pressable>
          </View>
          {error ? (
            <Text accessibilityRole="alert" className="text-sm text-foreground-muted">
              {error}
            </Text>
          ) : null}
          {props.runs?.map((run) => (
            <Text key={run.id} className="text-xs text-foreground-muted">
              Run {run.ordinal}: {purposeScopeLabel(executionPurposeOf(run).scope)} (
              {executionPurposeOf(run).provenance.type.replaceAll("_", " ")})
            </Text>
          ))}
        </View>
      ) : null}
    </View>
  );
}
