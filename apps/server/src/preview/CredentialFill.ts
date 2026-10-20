import {
  PreviewCredentialError,
  type PreviewAutomationFillCredentialInput,
  type PreviewAutomationFillCredentialResult,
  type PreviewAutomationOperation,
  type PreviewCredentialReason,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import type { Frame, Locator } from "playwright-core";

import type { CredentialVault } from "./CredentialVault.ts";
import { ServerBrowserOperationError } from "./ServerBrowserPage.ts";

const DEFAULT_TIMEOUT_MS = 15_000;
const FILLABLE_INPUT_TYPES = new Set(["", "text", "email", "password", "tel", "number", "search"]);

// Refusals carry a fixed reason and the tab, never page data such as its origin.
const refused = (reason: PreviewCredentialReason, tabId?: string) => {
  const error = new PreviewCredentialError({ reason });
  return new ServerBrowserOperationError("PreviewCredentialError", error.message, {
    reason,
    ...(tabId === undefined ? {} : { tabId }),
  });
};

/** The one answer to every read of a tab in a login. */
export const loginInProgress = () => refused("loginInProgress");
export const serverBrowserOnly = () => refused("serverBrowserOnly");

/**
 * What a login tab still allows: more fills, and actions that return no page
 * content. Status, dialogs, closing and preview_end_login are handled outside the
 * tab's queue and return no page content either.
 */
export const LOGIN_OPERATIONS: ReadonlySet<PreviewAutomationOperation> = new Set([
  "fillCredential",
  "click",
  "press",
  "type",
  "waitFor",
]);

const hostOf = (uri: string) => {
  // Bitwarden stores bare hosts such as shopify.com as well as full URLs.
  const withScheme = /^[a-z][a-z0-9+.-]*:\/\//i.test(uri) ? uri : `https://${uri}`;
  try {
    const url = new URL(withScheme);
    if (url.protocol !== "https:" && url.protocol !== "http:") return undefined;
    return url.hostname.toLowerCase().replace(/\.$/, "") || undefined;
  } catch {
    return undefined;
  }
};

const isIpHost = (host: string) => host.startsWith("[") || /^\d+(?:\.\d+){3}$/.test(host);

/**
 * Bitwarden's base-domain match: the item URI host that the page's host equals, or
 * is a subdomain of on a dot boundary. Schemes and ports are not compared; the
 * page must be https on its own.
 */
export const matchingItemHost = (pageUrl: string, uris: ReadonlyArray<string>) => {
  const host = hostOf(pageUrl);
  if (host === undefined) return undefined;
  return uris
    .map(hostOf)
    .find(
      (itemHost) =>
        itemHost !== undefined &&
        (host === itemHost ||
          (itemHost.includes(".") && !isIpHost(itemHost) && host.endsWith(`.${itemHost}`))),
    );
};

const vaultCall = async <A>(effect: Effect.Effect<A, PreviewCredentialError>, tabId: string) => {
  const result = await Effect.runPromise(Effect.result(effect));
  if (Result.isFailure(result)) throw refused(result.failure.reason, tabId);
  return result.success;
};

const originOf = (frame: Frame) => {
  try {
    return new URL(frame.url()).origin;
  } catch {
    return "null";
  }
};

/**
 * Fills one input from the vault. The value is read only after the input's own
 * frame proved to be an https page of the item's sites, and is written only if
 * that frame still shows the same origin.
 */
export const fillCredential = async (input: {
  readonly tabId: string;
  readonly vault: CredentialVault["Service"];
  readonly request: PreviewAutomationFillCredentialInput;
  readonly locate: (locator: string) => Locator;
  /**
   * Runs just before the value enters the page, and may refuse. Until then a failed
   * fill has put nothing in the page, so the caller can lift its quarantine.
   */
  readonly beforeTyping?: () => Promise<void>;
}): Promise<PreviewAutomationFillCredentialResult> => {
  const { request, tabId } = input;
  const timeout = request.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const handle = await input.locate(request.locator).elementHandle({ timeout });
  if (handle === null)
    throw new ServerBrowserOperationError(
      "PreviewAutomationInvalidSelectorError",
      "No element matches this locator. Take a fresh snapshot and use its locator.",
    );
  try {
    const target = await handle.evaluate((element) => ({
      tag: element.localName,
      type: element.localName === "input" ? (element.getAttribute("type") ?? "").toLowerCase() : "",
    }));
    const fillable =
      target.tag === "textarea" ||
      (target.tag === "input" && FILLABLE_INPUT_TYPES.has(target.type));
    if (!fillable || !(await handle.isEditable())) {
      throw new ServerBrowserOperationError(
        "PreviewAutomationTargetNotEditableError",
        "The target is not an enabled text input, so no credential was read.",
      );
    }
    const frame = await handle.ownerFrame();
    if (frame === null) throw refused("fillFailed", tabId);
    const origin = originOf(frame);
    if (!frame.url().startsWith("https:")) throw refused("insecureOrigin", tabId);
    const uris = await vaultCall(input.vault.uris(request.item), tabId);
    // The result names this vault host, never the live page.
    const site = matchingItemHost(frame.url(), uris);
    if (site === undefined) throw refused("originMismatch", tabId);
    const value = await vaultCall(input.vault.read(request.item, request.field), tabId);
    // The page may have navigated or moved the input while the vault answered.
    if (frame.isDetached() || originOf(frame) !== origin || (await handle.ownerFrame()) !== frame)
      throw refused("originMismatch", tabId);
    // The value is typed and dropped: nothing keeps it after this call.
    await input.beforeTyping?.();
    try {
      await handle.fill(value, { timeout });
    } catch {
      // Playwright's message and call log can quote the value.
      throw refused("fillFailed", tabId);
    }
    const verified = await handle.inputValue({ timeout }).then(
      (shown) => shown === value,
      () => false,
    );
    if (!verified) throw refused("fillFailed", tabId);
    return { filled: true, item: request.item, field: request.field, site, tabId };
  } finally {
    void handle.dispose().catch(() => undefined);
  }
};
