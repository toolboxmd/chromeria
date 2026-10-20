import * as Schema from "effect/Schema";

import { TrimmedNonEmptyString } from "./baseSchemas.ts";
import { PreviewTabId } from "./preview.ts";

/**
 * A vault item name as rbw matches it. No `:` or `/`, so rbw never reads it as
 * a URI, and no leading `-`, so it can never become an option.
 */
export const CREDENTIAL_ITEM_PATTERN = /^[\p{L}\p{N}][\p{L}\p{N} ._@+&()',#-]{0,127}$/u;

const ITEM_GUIDANCE =
  "Vault item name, for example Shopify. Letters, digits, spaces and . _ @ + & ( ) ' , # -; at most 128 characters.";

// A filter, not isPattern: a \p{...} class in a tool's JSON schema is not portable
// across provider validators. MCP tools drop struct-level checks, so it sits on the field.
const PreviewCredentialItem = Schema.String.check(
  Schema.makeFilter(
    (item: string) =>
      CREDENTIAL_ITEM_PATTERN.test(item) ||
      "item must start with a letter or digit and use only letters, digits, spaces and . _ @ + & ( ) ' , # -, at most 128 characters.",
    { description: ITEM_GUIDANCE },
  ),
).annotateKey({ description: ITEM_GUIDANCE });

export const PreviewCredentialField = Schema.Literals(["username", "password", "totp"]);
export type PreviewCredentialField = typeof PreviewCredentialField.Type;

export const CredentialVaultSettings = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("none") }),
  Schema.Struct({
    kind: Schema.Literal("local"),
    /** An rbw executable, or a wrapper that runs rbw on the vault's machine. */
    rbwPath: TrimmedNonEmptyString.check(Schema.isPattern(/^\//)),
  }),
]);
export type CredentialVaultSettings = typeof CredentialVaultSettings.Type;
export const DEFAULT_CREDENTIAL_VAULT_SETTINGS: CredentialVaultSettings = { kind: "none" };

const TAB_GUIDANCE =
  "Exact browser tab to fill in. Omit to use this agent session's current tab (see preview_status tabs).";
const LOCATOR_GUIDANCE =
  "The input to fill: locator='aria-ref=<ref>' from the latest snapshot (including iframe elements), or a unique Playwright selector.";
const TIMEOUT_GUIDANCE = "Maximum wait in milliseconds. Defaults to 15000; maximum 60000.";

export const PreviewAutomationFillCredentialInput = Schema.Struct({
  tabId: Schema.optional(PreviewTabId.annotate({ description: TAB_GUIDANCE })).annotate({
    description: TAB_GUIDANCE,
  }),
  item: PreviewCredentialItem,
  field: PreviewCredentialField.annotate({
    description: "Which value to fill: username, password, or totp (the current one-time code).",
  }),
  locator: Schema.String.check(Schema.isTrimmed())
    .check(Schema.isNonEmpty({ description: LOCATOR_GUIDANCE }))
    .annotateKey({ description: LOCATOR_GUIDANCE }),
  timeoutMs: Schema.optional(
    Schema.Int.check(Schema.isGreaterThan(0))
      .check(Schema.isLessThanOrEqualTo(60_000))
      .annotate({ description: TIMEOUT_GUIDANCE }),
  ).annotate({ description: TIMEOUT_GUIDANCE }),
}).annotate({
  description:
    "Fills one login input from the environment's credential vault. The value never enters the request or the result.",
});
export type PreviewAutomationFillCredentialInput = typeof PreviewAutomationFillCredentialInput.Type;

export const PreviewAutomationFillCredentialResult = Schema.Struct({
  filled: Schema.Boolean,
  item: Schema.String,
  field: PreviewCredentialField,
  site: Schema.String.annotate({
    description: "The vault item's host that the input's page matched, such as shopify.com.",
  }),
  tabId: Schema.optional(PreviewTabId),
});
export type PreviewAutomationFillCredentialResult =
  typeof PreviewAutomationFillCredentialResult.Type;

export const PreviewAutomationEndLoginResult = Schema.Struct({
  closed: Schema.Array(PreviewTabId).annotate({
    description: "The login's tabs that were closed. Empty when the tab was not in a login.",
  }),
});
export type PreviewAutomationEndLoginResult = typeof PreviewAutomationEndLoginResult.Type;

export const PreviewCredentialReason = Schema.Literals([
  "vaultNotConfigured",
  "vaultUnavailable",
  "vaultLocked",
  "itemNotFound",
  "itemAmbiguous",
  "fieldMissing",
  "originMismatch",
  "insecureOrigin",
  "protocolLogging",
  "fillFailed",
  "loginInProgress",
  "serverBrowserOnly",
]);
export type PreviewCredentialReason = typeof PreviewCredentialReason.Type;

const REASON_MESSAGES: Record<PreviewCredentialReason, string> = {
  vaultNotConfigured:
    "No credential vault is configured for this environment. Ask the user to set credentialVault in its server settings.",
  vaultUnavailable: "The credential vault could not be reached. Report it to the user.",
  vaultLocked: "The credential vault is locked. Ask the user to unlock it.",
  itemNotFound: "No vault item has that name.",
  itemAmbiguous: "Several vault items match that name. Use the item's exact name.",
  fieldMissing: "The vault item has no value for that field.",
  originMismatch:
    "The input's page is not one of the vault item's sites, so nothing was read or filled.",
  insecureOrigin: "Credentials are filled only on https pages, so nothing was read or filled.",
  protocolLogging:
    "Browser protocol logging (DEBUG=pw:*) is on in this server, so credentials are not filled.",
  fillFailed:
    "The input did not take the value. The tab stays in its login; finish it with clicks or call preview_end_login.",
  serverBrowserOnly:
    "Credentials are filled only in server browser tabs, not in tabs the desktop app hosts. Open the page with preview_open.",
  loginInProgress:
    "This tab is in a login after preview_fill_credential, so nothing in it or its related tabs can be read. Use preview_fill_credential, preview_click or preview_type with a locator, preview_press and preview_wait_for to finish signing in, then call preview_end_login and open a new tab.",
};

export class PreviewCredentialError extends Schema.TaggedError<PreviewCredentialError>()(
  "PreviewCredentialError",
  {
    reason: PreviewCredentialReason,
    tabId: Schema.optional(PreviewTabId),
  },
) {
  override get message(): string {
    return REASON_MESSAGES[this.reason];
  }
}
