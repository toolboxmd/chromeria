import { AppText as Text, AppTextInput as TextInput } from "../../components/AppText";
import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { useState } from "react";
import { Pressable, View } from "react-native";
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
    <View accessibilityLabel="Remote pending request">
      {unavailable ? (
        <Text accessibilityRole="alert">
          {content?.unavailableReason ??
            "This request can no longer be answered. Start or continue the task to ask again."}
        </Text>
      ) : null}
      <Text>
        {props.request.environmentId} / {props.request.threadId} / {props.request.requestId}
      </Text>
      {AsyncResult.isFailure(result) ? (
        <Text accessibilityRole="alert">
          This request changed or is unavailable. Refresh to check its current state.
        </Text>
      ) : null}
      {content?.kind === "user-input"
        ? content.questions.map((question) => (
            <View key={question.id}>
              <Text>{question.question}</Text>
              {question.options.map((option) => (
                <Pressable
                  key={option.label}
                  accessibilityRole="button"
                  disabled={busy || !mayRespond}
                  onPress={() =>
                    setAnswers((current) => {
                      const value = option.value ?? option.label;
                      const previous = current[question.id];
                      const selected = Array.isArray(previous)
                        ? previous.filter((entry): entry is string => typeof entry === "string")
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
                >
                  <Text>{option.label}</Text>
                </Pressable>
              ))}
              <TextInput
                accessibilityLabel={question.question}
                value={String(answers[question.id] ?? question.initialAnswer ?? "")}
                onChangeText={(value) =>
                  setAnswers((current) => ({ ...current, [question.id]: value }))
                }
                editable={!busy && mayRespond && question.allowCustomAnswer !== false}
              />
            </View>
          ))
        : null}
      {content?.kind === "approval" ? (
        <>
          <Text>
            {content.appName ? `${content.appName}: ` : ""}
            {content.prompt ?? content.requestKind}
          </Text>
          {(
            content.options ?? [
              { decision: "accept" as const, label: "Approve" },
              { decision: "decline" as const, label: "Deny" },
            ]
          ).map((option) => (
            <View key={option.decision}>
              <Pressable
                accessibilityRole="button"
                disabled={busy || !mayRespond || content.revision === undefined || unavailable}
                onPress={() => void act("respond", option.decision)}
              >
                <Text>{option.label}</Text>
              </Pressable>
              {"warning" in option && option.warning ? <Text>{option.warning}</Text> : null}
            </View>
          ))}
        </>
      ) : null}
      {content?.kind === "user-input" ? (
        <Pressable
          accessibilityRole="button"
          disabled={
            busy || !mayRespond || content.revision === undefined || unavailable || !complete
          }
          onPress={() => void act("respond")}
        >
          <Text>Send answer</Text>
        </Pressable>
      ) : null}
      {content?.kind === "user-input" && props.request.responseType === "message" ? (
        <Pressable
          accessibilityRole="button"
          disabled={busy || !mayRespond || content.revision === undefined || unavailable}
          onPress={() => void act("dismiss")}
        >
          <Text>Dismiss question</Text>
        </Pressable>
      ) : null}
      {error ? <Text accessibilityRole="alert">{error}</Text> : null}
      <Pressable
        accessibilityRole="button"
        disabled={busy}
        onPress={() => {
          setKey(randomUUID());
          refresh();
        }}
      >
        <Text>Refresh request</Text>
      </Pressable>
    </View>
  );
}
