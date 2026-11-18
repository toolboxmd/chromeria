import { describe, expect, it } from "vite-plus/test";

import {
  bareDraftIntent,
  bareLaunchRequest,
  draftBareLaunchRequest,
  resolveBareToggle,
} from "./bareLaunch.ts";

const supported = { bare: { supported: true as const } };
const unsupported = { bare: { supported: false as const, reason: "No stripping interface." } };

describe("resolveBareToggle", () => {
  it.each([
    ["a started thread", false, supported, true, { kind: "hidden" }],
    ["a new thread, off", true, supported, false, { kind: "available", on: false }],
    ["a new thread, on", true, supported, true, { kind: "available", on: true }],
    [
      "an unsupported provider",
      true,
      unsupported,
      true,
      { kind: "unsupported", reason: "No stripping interface." },
    ],
    ["a server without bare reporting", true, {}, true, { kind: "unsupported", reason: null }],
    ["no provider", true, null, true, { kind: "unsupported", reason: null }],
  ] as const)("%s", (_name, isNewThread, provider, requested, expected) => {
    expect(resolveBareToggle({ isNewThread, provider, requested })).toEqual(expected);
  });
});

describe("bareLaunchRequest", () => {
  it("requests bare only when the toggle is on and supported", () => {
    expect(bareLaunchRequest({ kind: "available", on: true })).toEqual({});
    expect(bareLaunchRequest({ kind: "available", on: false })).toBeUndefined();
    expect(bareLaunchRequest({ kind: "unsupported", reason: "x" })).toBeUndefined();
    expect(bareLaunchRequest({ kind: "hidden" })).toBeUndefined();
  });
});

describe("draftBareLaunchRequest", () => {
  it("launches a draft bare only while its toggle is on and the provider supports it", () => {
    const draftKey = "draft-bare-test";
    expect(draftBareLaunchRequest({ draftKey, provider: supported })).toBeUndefined();
    bareDraftIntent.set(draftKey, true);
    expect(draftBareLaunchRequest({ draftKey, provider: supported })).toEqual({});
    expect(draftBareLaunchRequest({ draftKey, provider: unsupported })).toBeUndefined();
    expect(
      draftBareLaunchRequest({ draftKey: "another-draft", provider: supported }),
    ).toBeUndefined();
    bareDraftIntent.set(draftKey, false);
    expect(draftBareLaunchRequest({ draftKey, provider: supported })).toBeUndefined();
  });
});
