// @effect-diagnostics nodeBuiltinImport:off - The synchronous native Keychain binding must run outside Electron main.
import * as NodeWorkerThreads from "node:worker_threads";
import * as Keyring from "@napi-rs/keyring";

import type { ChromiumKeyFailure } from "./ChromiumKeys.ts";

let result: { secret: string } | { reason: ChromiumKeyFailure };
try {
  const { service, account } = NodeWorkerThreads.workerData as { service: string; account: string };
  const secret = new Keyring.Entry(service, account).getPassword();
  result = secret ? { secret } : { reason: "keychainItemMissing" };
} catch (cause) {
  const message = String((cause as { message?: unknown } | undefined)?.message ?? "");
  result = {
    reason: /no (matching )?entry|not found/i.test(message)
      ? "keychainItemMissing"
      : "needsKeychainApproval",
  };
}
NodeWorkerThreads.parentPort?.postMessage(result);
NodeWorkerThreads.parentPort?.close();
