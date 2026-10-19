// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect's Crypto has no createHmac or timingSafeEqual.
import * as NodeCrypto from "node:crypto";

/** Accept previous/current windows for token lifetime and the next window for WSL clock lag. */
export const DESKTOP_BOOTSTRAP_TOKEN_WINDOW_MS = 12 * 60 * 60 * 1000;

function windowIndex(nowMs: number): number {
  return Math.floor(nowMs / DESKTOP_BOOTSTRAP_TOKEN_WINDOW_MS);
}

type DesktopBootstrapPurpose = "client" | "cookie-mirror";

function deriveToken(secret: string, window: number, purpose: DesktopBootstrapPurpose): string {
  return NodeCrypto.createHmac("sha256", secret)
    .update(`t3-desktop-bootstrap:${purpose === "client" ? "" : `${purpose}:`}${window}`)
    .digest("hex");
}

export function currentDesktopBootstrapToken(
  secret: string,
  nowMs: number,
  purpose: DesktopBootstrapPurpose = "client",
): string {
  return deriveToken(secret, windowIndex(nowMs), purpose);
}

export function isValidDesktopBootstrapToken(
  secret: string,
  token: string,
  nowMs: number,
  purpose: DesktopBootstrapPurpose = "client",
): boolean {
  const presented = Buffer.from(token);
  const current = windowIndex(nowMs);
  return [current - 1, current, current + 1].some((window) => {
    const expected = Buffer.from(deriveToken(secret, window, purpose));
    return expected.length === presented.length && NodeCrypto.timingSafeEqual(expected, presented);
  });
}
