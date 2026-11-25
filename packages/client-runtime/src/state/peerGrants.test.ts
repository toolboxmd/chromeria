import { describe, expect, it } from "@effect/vitest";
import { AuthSessionId, type PeerGrant } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { AsyncResult } from "effect/reactivity";

import { autosavePeerGrant, setPeerGrantPermission } from "./peerGrants.ts";
import type { AtomCommandResult } from "./runtime.ts";

const saved: PeerGrant = {
  sessionId: AuthSessionId.make("link"),
  resourceScope: "created-work",
  onward: false,
  preview: false,
  device: false,
  browserExport: false,
  vault: false,
};

async function toggleBrowserAccess(result: AtomCommandResult<void, unknown>) {
  const shown: Array<PeerGrant> = [];
  const errors: Array<unknown> = [];
  const sent: Array<PeerGrant> = [];
  await autosavePeerGrant({
    previous: saved,
    next: setPeerGrantPermission(saved, "preview", true),
    show: (grant) => shown.push(grant),
    save: async (grant) => {
      sent.push(grant);
      return result;
    },
    onError: (error) => errors.push(error),
  });
  return { shown, errors, sent };
}

describe("autosavePeerGrant", () => {
  it("keeps a saved toggle", async () => {
    const { shown, errors, sent } = await toggleBrowserAccess(AsyncResult.success(undefined));
    expect(sent).toEqual([{ ...saved, preview: true, vault: true }]);
    expect(shown).toEqual([{ ...saved, preview: true, vault: true }]);
    expect(errors).toEqual([]);
  });

  it("reverts a failed toggle and reports the error", async () => {
    const failure = new Error("permission denied");
    const { shown, errors } = await toggleBrowserAccess(AsyncResult.failure(Cause.fail(failure)));
    expect(shown).toEqual([{ ...saved, preview: true, vault: true }, saved]);
    expect(errors).toEqual([failure]);
  });

  it("reverts an interrupted toggle without an error", async () => {
    const { shown, errors } = await toggleBrowserAccess(AsyncResult.failure(Cause.interrupt()));
    expect(shown).toEqual([{ ...saved, preview: true, vault: true }, saved]);
    expect(errors).toEqual([]);
  });
});

describe("setPeerGrantPermission", () => {
  it("turns Use credential vault on and off with Browser access, and alone", () => {
    const browsing = setPeerGrantPermission(saved, "preview", true);
    expect(browsing).toMatchObject({ preview: true, vault: true });
    expect(setPeerGrantPermission(browsing, "vault", false)).toMatchObject({
      preview: true,
      vault: false,
    });
    expect(setPeerGrantPermission(browsing, "preview", false)).toMatchObject({
      preview: false,
      vault: false,
    });
  });
});
