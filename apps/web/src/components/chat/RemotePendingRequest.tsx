import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { useState } from "react";
import type {
  EnvironmentId,
  OrchestrationV2Subagent,
  PeerTaskSnapshot,
  ProviderApprovalDecision,
  ProviderUserInputAnswers,
} from "@t3tools/contracts";
import { remoteTasks } from "../../state/remoteTasks";
import { useAtomCommand } from "../../state/use-atom-command";
import { randomUUID } from "../../lib/utils";
import { Button } from "../ui/button";
import { Input } from "../ui/input";

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
        "The response was not confirmed. Refresh the request before changing your answer; retrying unchanged keeps the same action identity.",
      );
  };
  return (
    <section
      aria-label="Remote pending request"
      className="space-y-4 rounded-xl border border-border bg-card p-4"
    >
      <p className="text-sm font-medium text-muted-foreground">
        {content?.kind === "approval" ? "Approval requested" : "Question"}
        {props.destinationLabel ? ` · ${props.destinationLabel}` : ""}
      </p>
      {unavailable ? (
        <p role="alert">
          {content?.unavailableReason ??
            "This request can no longer be answered. Start or continue the task to ask again."}
        </p>
      ) : null}
      {AsyncResult.isFailure(result) ? (
        <p role="alert">
          This request changed or is unavailable. Refresh to check its current state.
        </p>
      ) : null}
      {content?.kind === "user-input"
        ? content.questions.map((question) => {
            const answer = answers[question.id] ?? question.initialAnswer;
            return (
              <div key={question.id} className="space-y-3">
                <label
                  htmlFor={`${props.request.requestId}:${question.id}`}
                  className="block font-medium"
                >
                  {question.question}
                </label>
                {question.options.length > 0 ? (
                  <p className="text-xs text-muted-foreground">
                    {question.multiSelect ? "Choose any that apply" : "Choose one"}
                    {question.required === false ? " · Optional" : ""}
                  </p>
                ) : null}
                <div className="flex flex-wrap gap-2">
                  {question.options.map((option) => {
                    const value = option.value ?? option.label;
                    const selected = Array.isArray(answer)
                      ? answer.includes(value)
                      : answer === value;
                    return (
                      <div key={option.label} className="space-y-1">
                        <Button
                          size="sm-multiline"
                          variant={selected ? "default" : "outline"}
                          aria-pressed={selected}
                          disabled={busy || !mayRespond}
                          onClick={() =>
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
                        >
                          {option.label}
                        </Button>
                        {option.description ? (
                          <p className="text-xs text-muted-foreground">{option.description}</p>
                        ) : null}
                      </div>
                    );
                  })}
                </div>
                {question.allowCustomAnswer !== false ? (
                  <Input
                    id={`${props.request.requestId}:${question.id}`}
                    placeholder="Your answer"
                    value={String(answer ?? "")}
                    onChange={(event) =>
                      setAnswers((current) => ({ ...current, [question.id]: event.target.value }))
                    }
                    disabled={busy || !mayRespond}
                  />
                ) : null}
              </div>
            );
          })
        : null}
      {content?.kind === "approval" ? (
        <div className="space-y-3">
          <p className="font-medium">
            {content.appName ? `${content.appName}: ` : ""}
            {content.prompt ?? content.requestKind}
          </p>
          <div className="flex flex-wrap gap-2">
            {(
              content.options ?? [
                { decision: "accept" as const, label: "Approve" },
                { decision: "decline" as const, label: "Deny" },
              ]
            ).map((option) => (
              <div key={option.decision}>
                <Button
                  variant="outline"
                  disabled={busy || !mayRespond || content.revision === undefined || unavailable}
                  onClick={() => void act("respond", option.decision)}
                >
                  {option.label}
                </Button>
                {"warning" in option && option.warning ? <p role="note">{option.warning}</p> : null}
              </div>
            ))}
          </div>
        </div>
      ) : null}
      <div className="flex flex-wrap gap-2">
        {content?.kind === "user-input" ? (
          <Button
            disabled={
              busy || !mayRespond || content.revision === undefined || unavailable || !complete
            }
            onClick={() => void act("respond")}
          >
            {busy ? "Sending answer…" : "Send answer"}
          </Button>
        ) : null}
        {content?.kind === "user-input" && props.request.responseType === "message" ? (
          <Button
            variant="outline"
            disabled={busy || !mayRespond || content.revision === undefined || unavailable}
            onClick={() => void act("dismiss")}
          >
            Dismiss question
          </Button>
        ) : null}
        <Button
          variant="outline"
          disabled={busy}
          onClick={() => {
            setKey(randomUUID());
            refresh();
          }}
        >
          Refresh request
        </Button>
      </div>
      {error ? <p role="alert">{error}</p> : null}
      <Button
        variant="ghost"
        size="sm"
        aria-expanded={showDetails}
        onClick={() => setShowDetails((value) => !value)}
      >
        Request details
      </Button>
      {showDetails ? (
        <p className="break-all text-xs text-muted-foreground">
          Environment: {props.request.environmentId}
          <br />
          Thread: {props.request.threadId}
          <br />
          Request: {props.request.requestId}
          <br />
          Revision: {content?.revision ?? props.request.revision}
        </p>
      ) : null}
    </section>
  );
}
