import {
  type ChatAttachment,
  type ChildRequestNoticeAttribution,
  type ChatFileAttachment,
  type GuestMessageAttribution,
  type OrchestrationMessageContext,
  type OrchestrationV2Actor,
  type OrchestrationV2CreationSource,
  type OrchestrationV2IdleCompaction,
  type OrchestrationV2PendingRuntimeRequestSummary,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2UserMessageInputIntent,
  ScheduledTaskId,
} from "@t3tools/contracts";
import {
  isAttachmentOnlyBootstrapPrompt,
  replaceComposerContextReferences,
} from "@t3tools/shared/composerContextReferences";
import { filePreviewKind } from "@t3tools/shared/filePreview";
import { imageMimeType } from "@t3tools/shared/image";
import { videoMimeType } from "@t3tools/shared/video";
import { msg } from "gt-i18n";

/** The working row while T3 Code's own compaction runs. */
export const IDLE_COMPACTING_LABELS = {
  beforeCacheExpires: msg("Compacting before the cache expires…"),
  onReturn: msg("Compacting…"),
} satisfies Record<OrchestrationV2IdleCompaction, string>;

/** Leads the finished compaction's token counts, in place of "Context compacted". */
export const IDLE_COMPACTION_TITLES = {
  beforeCacheExpires: msg("Auto-compacted before cache expiry"),
  onReturn: msg("Context compacted before sending"),
} satisfies Record<OrchestrationV2IdleCompaction, string>;

const LEGACY_AUTOMATION_PREFIX = /^\[Triggered by schedule task: [^\r\n]+\]\r?\n\r?\n/;
const LEGACY_AUTOMATION_MESSAGE_ID = /^scheduled-task-message:(.+):\d+:(?:scheduled|manual)$/;

export function resolveUserMessageFileCards(message: {
  readonly text: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly context?: OrchestrationMessageContext | undefined;
}): ReadonlyArray<ChatFileAttachment> | null {
  const files = message.attachments.filter(
    (attachment): attachment is ChatFileAttachment =>
      attachment.type === "file" &&
      imageMimeType(attachment) === null &&
      videoMimeType(attachment) === null &&
      filePreviewKind(attachment) !== "audio",
  );
  if (files.length === 0 || files.length !== message.attachments.length) return null;
  const fileIds = new Set(files.map((file) => file.id));
  const remaining = replaceComposerContextReferences(message.text, (reference) => {
    const record = message.context?.records.find((item) => item.contextId === reference.contextId);
    return reference.kind === "file" &&
      record?.kind === "file" &&
      "attachmentId" in record &&
      fileIds.has(record.attachmentId)
      ? ""
      : reference.label || reference.kind;
  });
  return remaining.trim() === "" ? files : null;
}

// Fork: the descendant-request relay (apps/server/src/childThreads/requestWake.ts) wakes ancestors as the system.
const isDescendantRequestWake = (message: {
  readonly id?: string;
  readonly createdBy?: OrchestrationV2Actor;
  readonly creationSource?: OrchestrationV2CreationSource;
}) =>
  message.createdBy === "system" &&
  message.creationSource === "server" &&
  message.id?.startsWith("child-request:") === true;

export function resolveUserMessagePresentation(message: {
  readonly id?: string;
  readonly role: string;
  readonly text: string;
  readonly createdBy?: OrchestrationV2Actor;
  readonly creationSource?: OrchestrationV2CreationSource;
  readonly scheduledTaskId?: ScheduledTaskId | undefined;
  readonly context?:
    | {
        readonly guest?: GuestMessageAttribution | undefined;
        readonly childRequest?: ChildRequestNoticeAttribution | undefined;
      }
    | undefined;
}): {
  readonly text: string;
  readonly attribution: "automation" | "agent" | "t3code" | "guest" | "child" | null;
  readonly scheduledTaskId: ScheduledTaskId | undefined;
  /** Set when `attribution` is "guest". */
  readonly guestName?: string;
  /** Set when `attribution` is "child". */
  readonly childRequest?: ChildRequestNoticeAttribution;
} {
  if (message.role !== "user") {
    return { text: message.text, attribution: null, scheduledTaskId: undefined };
  }
  // Fork: only the server relay sends as the system; WebSocket intake stamps clients as the user.
  const childRequest = message.context?.childRequest;
  if (
    childRequest !== undefined &&
    message.createdBy === "system" &&
    message.creationSource === "server"
  ) {
    return { text: message.text, attribution: "child", scheduledTaskId: undefined, childRequest };
  }
  // The stored text names the guest for the agent; people see the guest's own words.
  const guest = message.context?.guest;
  if (guest !== undefined) {
    return {
      text: guest.text,
      attribution: "guest",
      scheduledTaskId: undefined,
      guestName: guest.name,
    };
  }
  if (message.scheduledTaskId !== undefined) {
    return {
      text: message.text,
      attribution: "automation",
      scheduledTaskId: message.scheduledTaskId,
    };
  }
  const legacyPrefix = LEGACY_AUTOMATION_PREFIX.exec(message.text);
  const legacyTaskId = legacyPrefix
    ? LEGACY_AUTOMATION_MESSAGE_ID.exec(message.id ?? "")?.[1]
    : undefined;
  if (legacyPrefix !== null && (legacyTaskId !== undefined || message.createdBy === "agent")) {
    return {
      text: message.text.slice(legacyPrefix[0].length),
      attribution: "automation",
      scheduledTaskId: legacyTaskId === undefined ? undefined : ScheduledTaskId.make(legacyTaskId),
    };
  }
  return {
    // The agent needs words to start a turn; people sent only the attachments.
    text: isAttachmentOnlyBootstrapPrompt(message.text) ? "" : message.text,
    attribution:
      message.createdBy === "agent"
        ? message.creationSource === "server"
          ? "t3code"
          : "agent"
        : isDescendantRequestWake(message)
          ? "t3code"
          : null,
    scheduledTaskId: undefined,
  };
}

type GroupableUserMessage = Parameters<typeof resolveUserMessagePresentation>[0] & {
  readonly inputIntent?: OrchestrationV2UserMessageInputIntent;
};

export function resolveUserMessageGrouping(
  message: GroupableUserMessage | undefined,
  previous: GroupableUserMessage | undefined,
  next: GroupableUserMessage | undefined,
) {
  const isOwnMessage = (value: GroupableUserMessage | undefined) =>
    value?.role === "user" && resolveUserMessagePresentation(value).attribution === null;
  const isSteer = (value: GroupableUserMessage | undefined) =>
    value?.inputIntent === "steer" || value?.inputIntent === "promoted_queued_to_steer";
  const groupedWithPrevious = isOwnMessage(message) && isOwnMessage(previous);
  return {
    groupedWithPrevious,
    groupedWithNext: isOwnMessage(message) && isOwnMessage(next),
    continuesSteer: groupedWithPrevious && isSteer(message) && isSteer(previous),
  };
}

/** Fork: where a descendant's request stands, for the one-line notice that relayed it. */
export type ChildRequestNoticeState =
  | "waiting"
  | "approved"
  | "declined"
  | "answered"
  | "closed"
  | "settled"
  | "unknown";

export const CHILD_REQUEST_NOTICE_LABELS = {
  approval: {
    waiting: msg("is waiting for approval"),
    approved: msg("approval was approved"),
    declined: msg("approval was declined"),
    answered: msg("approval was answered"),
    closed: msg("approval request closed"),
    settled: msg("approval was handled"),
    unknown: msg("asked for approval"),
  },
  question: {
    waiting: msg("is waiting for an answer"),
    approved: msg("question was answered"),
    declined: msg("question was dismissed"),
    answered: msg("question was answered"),
    closed: msg("question closed"),
    settled: msg("question was handled"),
    unknown: msg("asked a question"),
  },
} satisfies Record<ChildRequestNoticeAttribution["kind"], Record<ChildRequestNoticeState, string>>;

/**
 * The child's shell names its pending request, so a waiting notice needs no detail subscription;
 * `request` is read from the child's detail only once the shell no longer names it. It is
 * `undefined` until that detail loads and `null` when the loaded history no longer holds it.
 */
export function resolveChildRequestNoticeState(input: {
  readonly requestId: ChildRequestNoticeAttribution["requestId"];
  readonly pendingRuntimeRequest:
    | Pick<OrchestrationV2PendingRuntimeRequestSummary, "id">
    | null
    | undefined;
  readonly request:
    | Pick<OrchestrationV2RuntimeRequest, "kind" | "status" | "decision">
    | null
    | undefined;
}): ChildRequestNoticeState {
  if (input.pendingRuntimeRequest?.id === input.requestId) return "waiting";
  const request = input.request;
  if (request === undefined) return "unknown";
  // The child's snapshot keeps pending requests, so one missing from it was settled earlier.
  if (request === null) return "settled";
  switch (request.status) {
    case "pending":
      return "waiting";
    case "expired":
    case "cancelled":
      return "closed";
    case "resolved":
      if (request.kind === "user_input") return "answered";
      if (request.decision === undefined) return "answered";
      return request.decision === "decline" || request.decision === "cancel"
        ? "declined"
        : "approved";
  }
}
