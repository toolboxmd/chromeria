import * as Schema from "effect/Schema";

import { ThreadId, TrimmedNonEmptyString, TurnItemId } from "./baseSchemas.ts";

/** A field's name doubles as its file name, so it stays a plain lowercase word. */
export const SecretRequestFieldName = TrimmedNonEmptyString.check(
  Schema.isMaxLength(64),
  Schema.isPattern(/^[a-z0-9_-]+$/),
  // The one key a plain object cannot hold as its own property.
  Schema.makeFilter((name: string) => name !== "__proto__", { expected: "not __proto__" }),
);

/** A typed secret, kept exactly as entered: spaces can be part of a password. */
const SecretValue = Schema.String.check(
  Schema.makeFilter((value: string) => value.trim().length > 0, {
    expected: "a non-blank secret",
  }),
);

/** One input on a secret request card. Carries what is asked, never the value. */
export const SecretRequestField = Schema.Struct({
  name: SecretRequestFieldName,
  label: TrimmedNonEmptyString,
  placeholder: Schema.optional(TrimmedNonEmptyString),
});
export type SecretRequestField = typeof SecretRequestField.Type;

/** The name of the one field a request without `fields` asks for. */
export const SECRET_REQUEST_SINGLE_FIELD_NAME = "secret";

/**
 * The user's answer to an agent's request for a secret. A card without
 * `fields` is saved with `secret`; a card with `fields` is saved with every
 * field's value in `secrets`, keyed by name. The server writes the values to
 * private files for the agent; the thread learns only the status.
 */
export const SecretRequestAnswerInput = Schema.Struct({
  threadId: ThreadId,
  turnItemId: TurnItemId,
  answer: Schema.Union([
    Schema.Struct({ type: Schema.Literal("save"), secret: SecretValue }),
    Schema.Struct({
      type: Schema.Literal("save"),
      secrets: Schema.Record(SecretRequestFieldName, SecretValue),
    }),
    Schema.Struct({ type: Schema.Literal("decline") }),
  ]),
});
export type SecretRequestAnswerInput = typeof SecretRequestAnswerInput.Type;

const SECRET_REQUEST_FAILURE_MESSAGES = {
  load_failed: "Could not load the secret request.",
  not_found: "This secret request no longer exists.",
  already_answered: "This secret request was already answered.",
  agent_stopped: "The agent that asked has stopped, so this secret can't be used.",
  fields_mismatch: "Fill in every field of this request.",
  store_failed: "Could not store the secret.",
  record_failed: "Saved the secret, but could not update the request.",
  invalid_ref: "That secretRef is not valid.",
  read_failed: "Could not read the secret.",
  ref_unavailable:
    "That secretRef was already used or does not exist. Ask the user again with request_secret.",
  ref_expired: "That secretRef expired. Ask the user again with request_secret.",
  consume_failed: "Could not use that secretRef. Try again.",
} as const;

export const SecretRequestFailureReason = Schema.Literals(
  Object.keys(SECRET_REQUEST_FAILURE_MESSAGES) as Array<
    keyof typeof SECRET_REQUEST_FAILURE_MESSAGES
  >,
);
export type SecretRequestFailureReason = typeof SecretRequestFailureReason.Type;

export class SecretRequestError extends Schema.TaggedError<SecretRequestError>()(
  "SecretRequestError",
  {
    reason: SecretRequestFailureReason,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {
  override get message(): string {
    return SECRET_REQUEST_FAILURE_MESSAGES[this.reason];
  }
}
