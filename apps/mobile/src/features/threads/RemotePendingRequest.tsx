import { RequestActionButton } from "./RequestActionButton";
import { AppText as Text, AppTextInput as TextInput } from "../../components/ui/AppText";
import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { useState } from "react";
import { View } from "react-native";
import type {
  EnvironmentId,
  OrchestrationV2Subagent,
  PeerTaskSnapshot,
  ProviderApprovalDecision,
  ProviderUserInputAnswers,
} from "@t3tools/contracts";
import { remoteTasks } from "../../state/remote-tasks";
import { useAtomCommand } from "../../state/use-atom-command";
import { randomUUID } from "expo-crypto";

export function RemotePendingRequest(props: {
  environmentId: EnvironmentId;
  task: Pick<OrchestrationV2Subagent, "id" | "threadId">;
  request: PeerTaskSnapshot["requests"][number];
  onResolved: () => void;
  destinationLabel?: string;
}) {
  const base = {
    parentThreadId: props.task.threadId,
    taskId: props.task.id,
    requestId: props.request.requestId,
    requestTarget: { environmentId: props.request.environmentId, threadId: props.request.threadId },
    requestRoute: props.request.route,
  };
  const query = remoteTasks.readRequest({
    environmentId: props.environmentId,
    input: { ...base, operation: { type: "read" } },
  });
  const result = useAtomValue(query);
  const refresh = useAtomRefresh(query);
  const respond = useAtomCommand(remoteTasks.request);
  const mayRespond = useAtomValue(remoteTasks.request.permissionAtom(props.environmentId));
  const [showDetails, setShowDetails] = useState(false);
  const [answers, setAnswers] = useState<ProviderUserInputAnswers>({});
  const [key, setKey] = useState(randomUUID);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const content = AsyncResult.isSuccess(result) && "kind" in result.value ? result.value : null;
  const unavailable = content?.responseType === "not_resumable";
  const complete =
    content?.kind !== "user-input" ||
    content.questions.every((question) => {
      const answer = answers[question.id] ?? question.initialAnswer;
      return (
        question.required === false ||
        question.initialAnswer !== undefined ||
        (typeof answer === "string" &&
          (answer.trim().length > 0 ||
            question.options.some((option) => (option.value ?? option.label) === answer))) ||
        (Array.isArray(answer) && answer.length > 0)
      );
    });
  const act = async (type: "respond" | "dismiss", decision?: ProviderApprovalDecision) => {
    if (!content || content.revision === undefined || busy) return;
    setBusy(true);
    setError(null);
    const outcome = await respond({
      environmentId: props.environmentId,
      input: {
        ...base,
        operation:
          type === "dismiss"
            ? { type, clientRequestId: key, expectedRevision: content.revision }
            : {
                type,
                clientRequestId: key,
                expectedRevision: content.revision,
                ...(content.kind === "approval"
                  ? { decision }
                  : {
                      answers: Object.fromEntries(
                        content.questions.map((question) => [
                          question.id,
                          answers[question.id] ?? question.initialAnswer ?? "",
                        ]),
                      ),
                    }),
              },
      },
    });
    setBusy(false);
    if (outcome._tag === "Success") props.onResolved();
    else
      setError(
        "The response was not confirmed. Refresh to check the request before changing your answer.",
      );
  };
  return (
    <View
      accessibilityLabel="Remote pending request"
      className="gap-4 rounded-2xl border border-border-subtle bg-grouped-card p-4"
    >
      <Text className="font-t3-bold text-sm text-foreground-muted">
        {content?.kind === "approval" ? "Approval requested" : "Question"}
        {props.destinationLabel ? ` · ${props.destinationLabel}` : ""}
      </Text>
      {unavailable ? (
        <Text accessibilityRole="alert" className="text-sm text-danger">
          {content?.unavailableReason ??
            "This request can no longer be answered. Start or continue the task to ask again."}
        </Text>
      ) : null}
      {AsyncResult.isFailure(result) ? (
        <Text accessibilityRole="alert" className="text-sm text-danger">
          This request changed or is unavailable. Refresh to check its current state.
        </Text>
      ) : null}
      {content?.kind === "user-input"
        ? content.questions.map((question) => {
            const answer = answers[question.id] ?? question.initialAnswer;
            return (
              <View key={question.id} className="gap-3">
                <Text className="font-t3-bold text-base">{question.question}</Text>
                {question.options.length > 0 ? (
                  <Text className="text-xs text-foreground-muted">
                    {question.multiSelect ? "Choose any that apply" : "Choose one"}
                    {question.required === false ? " · Optional" : ""}
                  </Text>
                ) : null}
                {question.options.map((option) => {
                  const value = option.value ?? option.label;
                  const selected = Array.isArray(answer)
                    ? answer.includes(value)
                    : answer === value;
                  return (
                    <View key={option.label} className="gap-1">
                      <RequestActionButton
                        label={option.label}
                        tone={selected ? "primary" : "secondary"}
                        accessibilityState={{ selected }}
                        disabled={busy || !mayRespond}
                        onPress={() =>
                          setAnswers((current) => {
                            const previous = current[question.id];
                            const selected = Array.isArray(previous)
                              ? previous.filter(
                                  (entry): entry is string => typeof entry === "string",
                                )
                              : [];
                            return {
                              ...current,
                              [question.id]: question.multiSelect
                                ? selected.includes(value)
                                  ? selected.filter((entry) => entry !== value)
                                  : [...selected, value]
                                : value,
                            };
                          })
                        }
                      />
                      {option.description ? (
                        <Text className="px-1 text-xs text-foreground-muted">
                          {option.description}
                        </Text>
                      ) : null}
                    </View>
                  );
                })}
                {question.allowCustomAnswer !== false ? (
                  <TextInput
                    accessibilityLabel={question.question}
                    placeholder="Your answer"
                    multiline={question.initialAnswer !== undefined}
                    submitBehavior={question.initialAnswer !== undefined ? "newline" : undefined}
                    value={String(answer ?? "")}
                    onChangeText={(value) =>
                      setAnswers((current) => ({ ...current, [question.id]: value }))
                    }
                    editable={!busy && mayRespond}
                  />
                ) : null}
              </View>
            );
          })
        : null}
      {content?.kind === "approval" ? (
        <View className="gap-3">
          <Text className="font-t3-bold text-base">
            {content.appName ? `${content.appName}: ` : ""}
            {content.prompt ?? content.requestKind}
          </Text>
          {(
            content.options ?? [
              { decision: "accept" as const, label: "Approve" },
              { decision: "decline" as const, label: "Deny" },
            ]
          ).map((option) => (
            <View key={option.decision} className="gap-1">
              <RequestActionButton
                label={option.label}
                disabled={busy || !mayRespond || content.revision === undefined || unavailable}
                onPress={() => void act("respond", option.decision)}
              />
              {"warning" in option && option.warning ? (
                <Text className="text-sm text-danger">{option.warning}</Text>
              ) : null}
            </View>
          ))}
        </View>
      ) : null}
      {content?.kind === "user-input" ? (
        <RequestActionButton
          label={busy ? "Sending answer…" : "Send answer"}
          disabled={
            busy || !mayRespond || content.revision === undefined || unavailable || !complete
          }
          onPress={() => void act("respond")}
        />
      ) : null}
      {content?.kind === "user-input" && props.request.responseType === "message" ? (
        <RequestActionButton
          label="Dismiss question"
          tone="secondary"
          disabled={busy || !mayRespond || content.revision === undefined || unavailable}
          onPress={() => void act("dismiss")}
        />
      ) : null}
      {error ? (
        <Text accessibilityRole="alert" className="text-sm text-danger">
          {error}
        </Text>
      ) : null}
      <RequestActionButton
        label="Refresh request"
        tone="secondary"
        disabled={busy}
        onPress={() => {
          setKey(randomUUID());
          refresh();
        }}
      />
      <RequestActionButton
        label="Request details"
        tone="secondary"
        accessibilityState={{ expanded: showDetails }}
        onPress={() => setShowDetails((value) => !value)}
      />
      {showDetails ? (
        <Text selectable className="text-xs text-foreground-muted">
          Environment: {props.request.environmentId}
          {"\n"}Thread: {props.request.threadId}
          {"\n"}Request: {props.request.requestId}
          {"\n"}Revision: {content?.revision ?? props.request.revision}
        </Text>
      ) : null}
    </View>
  );
}
