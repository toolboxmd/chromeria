import type {
  OrchestrationV2RunAttempt,
  OrchestrationV2ThreadProjection,
  ProviderDriverKind,
  RunId,
  ThreadBare,
} from "@t3tools/contracts";

export type SteeringRequester = NonNullable<OrchestrationV2RunAttempt["requestedBy"]>;

export function isPersonSteering(requester: SteeringRequester): boolean {
  return (
    requester.createdBy === "user" &&
    (requester.creationSource === "web" || requester.creationSource === "mobile")
  );
}

/** Existing mobile clients encode their busy Steer action as auto; idle sends use start. */
export function isMobileSteer(
  requester: SteeringRequester,
  deliveryIntent: "auto" | "steer" | "restart" | undefined,
): boolean {
  return (
    deliveryIntent === "auto" &&
    isPersonSteering(requester) &&
    requester.creationSource === "mobile"
  );
}

export function canInterruptSteer(
  projection: Pick<
    OrchestrationV2ThreadProjection,
    "runs" | "providerThreads" | "providerSessions" | "providerTurns"
  >,
  runId: RunId,
): boolean {
  const run = projection.runs.find((candidate) => candidate.id === runId);
  if (run?.status !== "running" || run.activeAttemptId === null) return false;
  if (
    !projection.providerTurns.some(
      (turn) => turn.runAttemptId === run.activeAttemptId && turn.status === "running",
    )
  )
    return false;
  const thread = projection.providerThreads.find(
    (candidate) => candidate.id === run.providerThreadId,
  );
  const turns = projection.providerSessions.find(
    (session) => session.id === thread?.providerSessionId,
  )?.capabilities.turns;
  return turns?.supportsInterrupt === true && turns.supportsSteeringByInterruptRestart;
}

function senderName(sender: SteeringRequester): string {
  if (isPersonSteering(sender)) return "the user";
  if (sender.createdBy === "agent") {
    return sender.senderThreadId === undefined
      ? "another agent"
      : `the agent in T3 thread ${sender.senderThreadId}`;
  }
  return "T3 Code automation";
}

export function steeringRestartNote(input: {
  readonly requester: SteeringRequester;
  readonly author: SteeringRequester;
  readonly driver: ProviderDriverKind;
  /** Bare threads receive only the message; the attempt retains its requester. */
  readonly bare?: ThreadBare | undefined;
}): string {
  if (input.bare !== undefined) return "";
  const person = isPersonSteering(input.requester);
  const sameAuthor =
    (person && isPersonSteering(input.author)) ||
    (input.requester.createdBy === input.author.createdBy &&
      input.requester.creationSource === input.author.creationSource &&
      input.requester.senderThreadId === input.author.senderThreadId);
  if (person && sameAuthor && input.driver !== "claudeAgent") return "";
  const message = sameAuthor
    ? "the message below"
    : `the queued message below, which ${senderName(input.author)} wrote`;
  return `Note from T3 Code: ${senderName(input.requester)} interrupted your previous turn, if it was still running, to deliver ${message}. Tool calls stopped for this message were interrupted, not refused, even if their results say the user declined them or does not want them. Follow the message below before deciding whether to retry a stopped action. Stopped actions may have partly run, other running work may have stopped too, and actions that finished keep their effects.`;
}
