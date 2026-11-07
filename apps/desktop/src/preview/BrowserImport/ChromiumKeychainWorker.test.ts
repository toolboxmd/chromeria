import { beforeEach, expect, it, vi } from "vite-plus/test";

const { getPassword, reply, close, Entry } = vi.hoisted(() => {
  const getPassword = vi.fn();
  return {
    getPassword,
    reply: vi.fn(),
    close: vi.fn(),
    Entry: vi.fn(function () {
      return { getPassword };
    }),
  };
});
vi.mock("node:worker_threads", () => ({
  parentPort: { postMessage: reply, close },
  workerData: { service: "Synthetic Safe Storage", account: "Synthetic" },
}));
vi.mock("@napi-rs/keyring", () => ({ Entry }));

beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
});

it.each([
  ["synthetic-secret", { secret: "synthetic-secret" }],
  [null, { reason: "keychainItemMissing" }],
  ["", { reason: "keychainItemMissing" }],
])("returns the native result without exposing it in an error", async (secret, result) => {
  getPassword.mockReturnValue(secret);
  await import("./ChromiumKeychainWorker.ts");
  expect(reply).toHaveBeenCalledWith(result);
  expect(Entry).toHaveBeenCalledWith("Synthetic Safe Storage", "Synthetic");
  expect(close).toHaveBeenCalledOnce();
});

it.each([
  ["No matching entry found", "keychainItemMissing"],
  ["User denied synthetic-secret-sentinel", "needsKeychainApproval"],
])("preserves failure classification and redacts native errors", async (message, reason) => {
  getPassword.mockImplementation(() => {
    throw new Error(message);
  });
  await import("./ChromiumKeychainWorker.ts");
  expect(reply).toHaveBeenCalledWith({ reason });
  expect(JSON.stringify(reply.mock.calls)).not.toContain("synthetic-secret-sentinel");
});
