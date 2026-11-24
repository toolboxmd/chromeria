import {
  PreviewSaveSecretError,
  type PreviewAutomationSaveSecretInput,
  type PreviewAutomationSaveSecretResult,
  type PreviewSaveSecretReason,
  type PreviewTabId,
} from "@t3tools/contracts";
import type { ElementHandle, Locator } from "playwright-core";

import { ServerBrowserOperationError } from "./ServerBrowserPage.ts";

const DEFAULT_TIMEOUT_MS = 15_000;
const MAX_VALUE_BYTES = 4096;

// Every failure carries a fixed reason: Playwright's messages and call logs can quote the page.
const refused = (reason: PreviewSaveSecretReason) =>
  new ServerBrowserOperationError({
    tag: "PreviewSaveSecretError",
    message: new PreviewSaveSecretError({ reason }).message,
    detail: { reason },
  });

export const saveSecretServerBrowserOnly = () => refused("serverBrowserOnly");

const attempt = async <A>(run: () => Promise<A>, reason: PreviewSaveSecretReason) => {
  try {
    return await run();
  } catch {
    throw refused(reason);
  }
};

/** The element's own value: a field's value, or the text a person sees. */
const readValue = async (handle: ElementHandle, timeout: number) => {
  const field = await attempt(
    () =>
      handle.evaluate(
        (element) => element.localName === "input" || element.localName === "textarea",
      ),
    "readFailed",
  );
  if (field) return attempt(() => handle.inputValue({ timeout }), "readFailed");
  if (!(await attempt(() => handle.isVisible(), "readFailed"))) throw refused("elementHidden");
  const text = await attempt(() => handle.innerText(), "readFailed");
  return text.replace(/[\u200b\u00ad]/g, "").trim();
};

/**
 * Reads one element's value in an https frame and hands it to `save`. The value
 * goes nowhere else: the result and every error carry no page data.
 */
export const saveSecret = async (input: {
  readonly tabId: PreviewTabId;
  readonly request: PreviewAutomationSaveSecretInput;
  /** Playwright logs page values with these on, so nothing is read. */
  readonly protocolLogging: boolean;
  readonly locate: (locator: string) => Locator;
  /** Runs just before the value is written, and may refuse with its own error. */
  readonly beforeSave: () => Promise<void>;
  readonly save: (value: string) => Promise<string>;
}): Promise<PreviewAutomationSaveSecretResult> => {
  if (input.protocolLogging) throw refused("protocolLogging");
  const timeout = input.request.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const handle = await attempt(
    () => input.locate(input.request.locator).elementHandle({ timeout }),
    "elementNotFound",
  );
  if (handle === null) throw refused("elementNotFound");
  try {
    const frame = await attempt(() => handle.ownerFrame(), "readFailed");
    if (frame === null) throw refused("readFailed");
    if (!frame.url().startsWith("https:")) throw refused("insecureOrigin");
    const value = await readValue(handle, timeout);
    if (value === "") throw refused("emptyValue");
    if (/[\r\n]/.test(value)) throw refused("multiLine");
    if (new TextEncoder().encode(value).byteLength > MAX_VALUE_BYTES) throw refused("tooLong");
    await input.beforeSave();
    const path = await attempt(() => input.save(value), "storeFailed");
    return { saved: true, path, length: value.length, tabId: input.tabId };
  } finally {
    void handle.dispose().catch(() => undefined);
  }
};
