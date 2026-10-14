import * as Schema from "effect/Schema";
import { TrimmedNonEmptyString, NonNegativeInt, ThreadId } from "./baseSchemas.ts";
import { ProviderDriverKind } from "./providerInstance.ts";
import { RuntimeMode } from "./providerPolicy.ts";

const AbsolutePath = TrimmedNonEmptyString.check(Schema.isPattern(/^(?:\/|[A-Za-z]:[\\/])/));
export const BareAdditionRequest = Schema.Union([
  Schema.Struct({ type: Schema.Literal("skill"), name: TrimmedNonEmptyString }),
  Schema.Struct({ type: Schema.Literal("skillFile"), path: AbsolutePath }),
  Schema.Struct({ type: Schema.Literal("file"), path: AbsolutePath }),
  Schema.Struct({
    type: Schema.Literal("mcp"),
    server: TrimmedNonEmptyString,
    tool: Schema.optional(TrimmedNonEmptyString),
  }),
  Schema.Struct({ type: Schema.Literal("plugin"), name: TrimmedNonEmptyString }),
  Schema.Struct({ type: Schema.Literal("app"), name: TrimmedNonEmptyString }),
]);
export type BareAdditionRequest = typeof BareAdditionRequest.Type;
export const BareLaunchRequest = Schema.Struct({
  additions: Schema.optional(Schema.Array(BareAdditionRequest).check(Schema.isMaxLength(16))),
});
export type BareLaunchRequest = typeof BareLaunchRequest.Type;
export const ResolvedBareAddition = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("skill"),
    name: Schema.optional(TrimmedNonEmptyString),
    path: AbsolutePath,
    sha256: Schema.String,
    byteCount: NonNegativeInt,
  }),
  Schema.Struct({
    type: Schema.Literal("file"),
    path: AbsolutePath,
    sha256: Schema.String,
    byteCount: NonNegativeInt,
  }),
  Schema.Struct({ type: Schema.Literal("mcp"), server: Schema.Literal("t3-code") }),
]);
export type ResolvedBareAddition = typeof ResolvedBareAddition.Type;
export const ThreadBare = Schema.Struct({
  additions: Schema.Array(ResolvedBareAddition),
  // Stamped at fresh thread creation, inherited only by native forks. It is
  // absent while additions are resolved, and never accepted in BareLaunchRequest.
  nativeHomeOwnerThreadId: Schema.optional(ThreadId),
});
export type ThreadBare = typeof ThreadBare.Type;
export const BareSupport = Schema.Union([
  Schema.Struct({ supported: Schema.Literal(true) }),
  Schema.Struct({ supported: Schema.Literal(false), reason: Schema.String }),
]);
export type BareSupport = typeof BareSupport.Type;
export const BareEvidenceCategory = Schema.Literals([
  "tools",
  "mcp",
  "skills",
  "plugins",
  "slash_commands",
  "system_text",
  "instructions",
  "hooks",
  "environment",
]);
export const BareEvidence = Schema.Struct({
  category: BareEvidenceCategory,
  status: Schema.Literals([
    "observed_clean",
    "observed_retained",
    "observed_unexpected",
    "unobserved",
  ]),
  identities: Schema.Array(Schema.String),
  source: Schema.String,
});
export type BareEvidence = typeof BareEvidence.Type;
export const BareReceipt = Schema.Struct({
  driver: ProviderDriverKind,
  permissions: Schema.Struct({
    requested: Schema.NullOr(RuntimeMode),
    admitted: RuntimeMode,
    effective: Schema.String,
  }),
  additions: Schema.Array(ResolvedBareAddition),
  planned: Schema.Struct({
    controls: Schema.Array(Schema.String),
    removes: Schema.Array(Schema.String),
  }),
  knownRetained: Schema.Array(Schema.String),
  evidence: Schema.Array(BareEvidence),
  nativeRecord: Schema.Struct({
    harness: Schema.String,
    sessionId: Schema.NullOr(Schema.String),
    root: Schema.NullOr(Schema.String),
  }),
});
export type BareReceipt = typeof BareReceipt.Type;

/** A partial inventory cannot certify the whole launch. */
export function isBareReceiptVerified(receipt: BareReceipt): boolean {
  return BareEvidenceCategory.literals.every(
    (category) =>
      receipt.evidence.some(
        (entry) => entry.category === category && entry.status !== "unobserved",
      ) &&
      !receipt.evidence.some(
        (entry) =>
          entry.category === category &&
          (entry.status === "unobserved" || entry.status === "observed_unexpected"),
      ),
  );
}
