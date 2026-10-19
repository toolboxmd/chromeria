import {
  SECRET_REQUEST_SINGLE_FIELD_NAME,
  type OrchestrationV2TurnItem,
  type SecretRequestAnswerInput,
  type SecretRequestField,
} from "@t3tools/contracts";

export type SecretRequestItem = Extract<
  OrchestrationV2TurnItem,
  { readonly type: "secret_request" }
>;

export const SECRET_REQUEST_PRIVACY_NOTE = "Sent privately to the agent, never shown in chat";
export const SECRET_REQUEST_DEFAULT_PLACEHOLDER = "Paste the secret";

export type SecretRequestDisplay =
  | { readonly kind: "pending" }
  | { readonly kind: "pending-elsewhere"; readonly label: string }
  | {
      readonly kind: "answered";
      readonly outcome: "saved" | "declined" | "ended";
      readonly label: string;
    };

const SAVED_DISPLAY: SecretRequestDisplay = {
  kind: "answered",
  outcome: "saved",
  label: "Saved securely and kept private",
};
const DECLINED_DISPLAY: SecretRequestDisplay = {
  kind: "answered",
  outcome: "declined",
  label: "Declined",
};
const ENDED_DISPLAY: SecretRequestDisplay = {
  kind: "answered",
  outcome: "ended",
  label: "Request ended",
};
const PENDING_DISPLAY: SecretRequestDisplay = { kind: "pending" };
const PENDING_ELSEWHERE_DISPLAY: SecretRequestDisplay = {
  kind: "pending-elsewhere",
  label: "Waiting for an answer in the original thread",
};

/**
 * `visibility` is the projected row's: a request inherited from another
 * thread (a fork) can only be answered where it was asked.
 */
export function secretRequestDisplay(
  item: Pick<SecretRequestItem, "secretStatus">,
  visibility: "local" | "inherited" | "synthetic",
): SecretRequestDisplay {
  switch (item.secretStatus) {
    case "pending":
      return visibility === "local" ? PENDING_DISPLAY : PENDING_ELSEWHERE_DISPLAY;
    case "saved":
      return SAVED_DISPLAY;
    case "declined":
      return DECLINED_DISPLAY;
    case "cancelled":
      return ENDED_DISPLAY;
  }
}

/** The inputs a card shows: its fields, or one field titled by the request itself. */
export function secretRequestFields(
  item: Pick<SecretRequestItem, "label" | "placeholder" | "fields">,
): ReadonlyArray<SecretRequestField> {
  return (
    item.fields ?? [
      {
        name: SECRET_REQUEST_SINGLE_FIELD_NAME,
        label: item.label,
        ...(item.placeholder === undefined ? {} : { placeholder: item.placeholder }),
      },
    ]
  );
}

/** Whether every field has a value; a blank one keeps Save disabled. */
export function secretRequestComplete(
  item: Pick<SecretRequestItem, "label" | "placeholder" | "fields">,
  values: ReadonlyMap<string, string>,
): boolean {
  return secretRequestFields(item).every(
    (field) => (values.get(field.name) ?? "").trim().length > 0,
  );
}

/**
 * Builds the RPC payload for an answer from values keyed by field name (a Map,
 * so a field named like an Object property reads its own value). Values are
 * sent exactly as typed. A save with any blank field returns null, since the
 * server saves all fields or none; callers keep Save disabled instead.
 */
export function secretRequestAnswerInput(
  item: Pick<SecretRequestItem, "id" | "threadId" | "label" | "placeholder" | "fields">,
  answer:
    | { readonly type: "save"; readonly values: ReadonlyMap<string, string> }
    | { readonly type: "decline" },
): SecretRequestAnswerInput | null {
  if (answer.type === "decline") {
    return { threadId: item.threadId, turnItemId: item.id, answer: { type: "decline" } };
  }
  if (!secretRequestComplete(item, answer.values)) return null;
  const entries = secretRequestFields(item).map(
    (field) => [field.name, answer.values.get(field.name) ?? ""] as const,
  );
  const secrets = Object.fromEntries(entries);
  const secret = item.fields === undefined ? entries[0]?.[1] : undefined;
  // A card without fields is answered as servers before multi-field cards expect.
  return {
    threadId: item.threadId,
    turnItemId: item.id,
    answer: secret !== undefined ? { type: "save", secret } : { type: "save", secrets },
  };
}

const USER_FACING_FAILURE_TAGS = new Set(["SecretRequestError", "EnvironmentAuthorizationError"]);

/**
 * Only known server errors pass their message through: anything else (transport or encoding
 * failures) gets the generic copy, so the typed value can never surface in the UI.
 */
export function secretRequestFailureMessage(failure: unknown): string {
  if (
    typeof failure === "object" &&
    failure !== null &&
    "_tag" in failure &&
    typeof failure._tag === "string" &&
    USER_FACING_FAILURE_TAGS.has(failure._tag) &&
    "message" in failure &&
    typeof failure.message === "string" &&
    failure.message.trim().length > 0
  ) {
    return failure.message;
  }
  return "Could not answer the request. Try again.";
}
