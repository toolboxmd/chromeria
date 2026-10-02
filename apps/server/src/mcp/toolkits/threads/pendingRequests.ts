import type { OrchestrationThreadActivity } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { PendingThreadRequest } from "./tools.ts";

const Payload = Schema.Record(Schema.String, Schema.Unknown);
const isPayload = Schema.is(Payload);
const decodeRequest = Schema.decodeOption(PendingThreadRequest);

/** The same terminal-request rules as the clients and projection pending accounting. */
export function pendingThreadRequests(activities: ReadonlyArray<OrchestrationThreadActivity>) {
  const pending = new Map<string, PendingThreadRequest>();
  const closed = new Set<string>();
  for (const activity of activities) {
    if (!isPayload(activity.payload)) continue;
    const payload = activity.payload;
    if (typeof payload.requestId !== "string") continue;
    const id = payload.requestId;
    const failure = typeof payload.detail === "string" ? payload.detail.toLowerCase() : "";
    if (
      activity.kind === "approval.resolved" ||
      activity.kind === "user-input.resolved" ||
      ((activity.kind === "provider.approval.respond.failed" ||
        activity.kind === "provider.user-input.respond.failed") &&
        [
          "stale pending approval request",
          "unknown pending approval request",
          "unknown pending permission request",
          "unknown pending codex approval request",
          "stale pending user-input request",
          "unknown pending user-input request",
          "unknown pending user input request",
          "unknown pending codex user input request",
        ].some((text) => failure.includes(text)))
    ) {
      closed.add(id);
      pending.delete(id);
    } else if (
      !closed.has(id) &&
      (activity.kind === "approval.requested" || activity.kind === "user-input.requested")
    ) {
      if (
        payload.requestType === "tool_user_input" ||
        payload.requestType === "auth_tokens_refresh"
      )
        continue;
      const request = decodeRequest({
        requestId: id,
        kind: activity.kind === "approval.requested" ? "approval" : "user-input",
        summary: activity.summary,
        detail: payload,
      });
      if (request._tag === "Some") pending.set(id, request.value);
    }
  }
  return [...pending.values()];
}
