import { ProviderDriverKind } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { subagentIconDriver } from "./subagentRuntime.ts";

const kind = ProviderDriverKind.make;

describe("subagentIconDriver", () => {
  it("uses the destination kind after a remote task attaches", () => {
    expect(
      subagentIconDriver({
        driver: kind("remote"),
        remote: { driver: kind("claudeAgent") },
        resolvedDriver: kind("codex"),
      }),
    ).toBe(kind("claudeAgent"));
  });

  it("draws nothing for a remote task before its destination reports a kind", () => {
    expect(subagentIconDriver({ driver: kind("remote"), remote: {} })).toBeUndefined();
    expect(
      subagentIconDriver({
        driver: kind("remote"),
        remote: { driver: kind("remote") },
        resolvedDriver: kind("remote"),
      }),
    ).toBeUndefined();
  });

  it("uses a provider resolved in the owning environment until the destination reports one", () => {
    expect(
      subagentIconDriver({
        driver: kind("remote"),
        remote: {},
        resolvedDriver: kind("claudeAgent"),
      }),
    ).toBe(kind("claudeAgent"));
  });

  it("keeps a local subagent on its own kind", () => {
    expect(subagentIconDriver({ driver: kind("codex") })).toBe(kind("codex"));
    expect(subagentIconDriver({ driver: kind("remote") })).toBeUndefined();
  });
});
