import {
  SecretReference,
  secretRequestPageUrl,
  SECRET_REQUEST_SINGLE_FIELD_NAME,
  type SecretRequestField,
  type OrchestrationV2TurnItem,
  type SecretProviderId,
  type SecretRequestAnswerInput,
  type ServerConfig,
  type ServerSettings,
} from "@t3tools/contracts";
import { builtInSecretProviderDefinitions } from "@t3tools/secret-provider-definitions/registry";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

export type SecretRequestItem = Extract<
  OrchestrationV2TurnItem,
  { readonly type: "secret_request" }
>;

const SECRET_REQUEST_PRIVACY_NOTE = "Sent privately to the agent, never shown in chat";
export const secretRequestPrivacyNote = (item: Pick<SecretRequestItem, "destination">) =>
  item.destination === undefined
    ? SECRET_REQUEST_PRIVACY_NOTE
    : `Sent privately to ${item.destination.environmentLabel} for project ${item.destination.projectId}, never shown in chat`;
export const SECRET_REQUEST_DEFAULT_PLACEHOLDER = "Paste the secret";

// Backticks, <angle brackets> and markdown link targets (balanced parentheses allowed) keep their
// exact text; a bare link drops trailing sentence punctuation.
const REASON_LINK =
  /`(https?:\/\/[^`\s]+)`|<(https?:\/\/[^>\s]+)>|\]\((https?:\/\/(?:[^()\s]|\([^()\s]*\))+)\)|(https?:\/\/[^\s<>"'`[\]]+)/giu;
/** Drops trailing sentence punctuation; a `)` stays when the link opened a matching `(`. */
function trimPunctuation(link: string): string {
  let end = link.length;
  while (end > 0 && ".,;:!?)".includes(link[end - 1]!)) {
    const kept = link.slice(0, end);
    if (link[end - 1] === ")" && kept.split("(").length > kept.split(")").length - 1) break;
    end -= 1;
  }
  return link.slice(0, end);
}

/** Drops a closing `*` or `_` run only when the same run opened emphasis before the link and is still open. */
function trimEmphasis(link: string, before: string) {
  const run = /(\*{1,3}|_{1,3})$/u.exec(link)?.[1];
  if (run === undefined) return link;
  const escaped = run.replaceAll("*", "\\*");
  // Nested emphasis (`**_url_**`) opens one run right after another.
  const opener = [...before.matchAll(new RegExp(`(?:^|[\\s(*_])${escaped}(?=\\S|$)`, "gu"))].at(-1);
  if (opener === undefined) return link;
  const closed = new RegExp(`\\S${escaped}(?:[\\s.,;:!?)]|$)`, "u").test(
    before.slice(opener.index + opener[0].length),
  );
  return closed ? link : trimPunctuation(link.slice(0, -run.length));
}

/** The one distinct http(s) link in `reason`, for agents that put it there instead of in `url`. */
function reasonLink(reason: string): string | undefined {
  const raw = Array.from(reason.matchAll(REASON_LINK), (match) => {
    const [, ticked, angled, target, bare] = match;
    if (bare === undefined) return ticked ?? angled ?? target ?? "";
    const before = reason.slice(0, match.index);
    let link = trimPunctuation(bare);
    for (let next = trimEmphasis(link, before); next !== link; next = trimEmphasis(link, before)) {
      link = next;
    }
    return link;
  });
  // Two spellings of one page (`https://a.dev` and `https://a.dev/`) count once.
  const links = new Set(raw.map((link) => secretRequestPageUrl(link)?.href ?? link));
  return links.size === 1 ? [...links][0] : undefined;
}

/** Only http(s), so an agent cannot make the card open another scheme. */
export function secretRequestLink(item: Pick<SecretRequestItem, "url" | "reason">) {
  const raw = item.url ?? reasonLink(item.reason);
  const url = raw === undefined ? null : secretRequestPageUrl(raw);
  return url === null ? null : { href: url.href, host: url.host };
}

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

/** A configured secret provider a pending card can read its answer from. */
export interface SecretRequestSource {
  readonly id: SecretProviderId;
  readonly label: string;
  readonly isReference: (reference: string) => boolean;
  readonly referencePlaceholder: string;
  readonly invalidReferenceMessage: string;
}

const NO_SOURCES: ReadonlyArray<SecretRequestSource> = [];
const decodeConfigByKind = new Map(
  builtInSecretProviderDefinitions.definitions.map((definition) => [
    definition.kind,
    Schema.decodeUnknownOption(definition.configSchema),
  ]),
);

/** Empty unless the server accepts reference answers; providers this build cannot read are left out. */
export function secretRequestSources(
  config: Pick<ServerConfig, "secretRequestReferences"> | null | undefined,
  providers: ServerSettings["secretProviders"],
): ReadonlyArray<SecretRequestSource> {
  if (config?.secretRequestReferences !== true) return NO_SOURCES;
  return configuredSecretSources(providers);
}

export function configuredSecretSources(
  providers: ServerSettings["secretProviders"],
): ReadonlyArray<SecretRequestSource> {
  return (Object.keys(providers) as Array<SecretProviderId>).flatMap((id) => {
    const provider = providers[id];
    const definition =
      provider === undefined ? undefined : builtInSecretProviderDefinitions.find(provider.kind);
    if (
      provider === undefined ||
      definition === undefined ||
      decodeConfigByKind.get(definition.kind)?.(provider.config)._tag !== "Some"
    ) {
      return [];
    }
    return [
      {
        id,
        label: provider.label,
        isReference: definition.isReference,
        referencePlaceholder: definition.referencePlaceholder,
        invalidReferenceMessage: definition.invalidReferenceMessage,
      },
    ];
  });
}

const decodeSecretReference = Schema.decodeUnknownOption(SecretReference);

/** The reference `text` names, or undefined when it is not `source`'s syntax. */
export function secretRequestReference(
  source: SecretRequestSource,
  text: string,
): SecretReference | undefined {
  const decoded = decodeSecretReference({ provider: source.id, reference: text.trim() });
  return Option.isSome(decoded) && source.isReference(decoded.value.reference)
    ? decoded.value
    : undefined;
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

export type SecretRequestReply =
  | { readonly type: "save"; readonly values: ReadonlyMap<string, string> }
  | { readonly type: "reference"; readonly source: SecretRequestSource; readonly reference: string }
  | { readonly type: "decline" };

export function secretRequestAnswerInput(
  item: Pick<SecretRequestItem, "id" | "threadId"> &
    Partial<Pick<SecretRequestItem, "label" | "placeholder" | "fields">>,
  answer: SecretRequestReply,
): SecretRequestAnswerInput | null {
  if (answer.type === "decline") {
    return { threadId: item.threadId, turnItemId: item.id, answer: { type: "decline" } };
  }
  if (answer.type === "reference") {
    if (item.fields !== undefined) return null;
    const reference = secretRequestReference(answer.source, answer.reference);
    return reference === undefined
      ? null
      : { threadId: item.threadId, turnItemId: item.id, answer: { type: "reference", reference } };
  }
  const card = { ...item, label: item.label ?? "Secret" };
  if (!secretRequestComplete(card, answer.values)) return null;
  const entries = secretRequestFields(card).map(
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
