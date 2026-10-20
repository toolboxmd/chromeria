import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { type EnvironmentId, ThreadId } from "@t3tools/contracts";
import { beforeEach, describe, expect, it } from "vite-plus/test";

import {
  browserMiniPlayerSource,
  previewMiniPlayerSourceKey,
  selectThreadPreviewMiniPlayer,
  usePreviewMiniPlayerStore,
} from "./previewMiniPlayerStore";

const refA = scopeThreadRef("env-1" as EnvironmentId, ThreadId.make("thread-A"));
const tabA = browserMiniPlayerSource("tab-a");
const tabB = browserMiniPlayerSource("tab-b");

beforeEach(() => {
  usePreviewMiniPlayerStore.setState({ byThreadKey: {} });
});

describe("previewMiniPlayerStore", () => {
  it("ignores stale drag updates after the floating tab changes", () => {
    usePreviewMiniPlayerStore.getState().open(refA, tabA);
    usePreviewMiniPlayerStore.getState().open(refA, tabB);
    usePreviewMiniPlayerStore.getState().move(refA, "browser:tab-a", { x: 100, y: 100 });

    expect(
      selectThreadPreviewMiniPlayer(usePreviewMiniPlayerStore.getState().byThreadKey, refA),
    ).toEqual({
      source: tabB,
      position: null,
      width: null,
      lastInteraction: "drag",
    });
  });
});

it("keeps qualified openings distinct and rejects a late drag on the previous opening", () => {
  const resource = {
    type: "device" as const,
    environmentId: "destination-b" as EnvironmentId,
    threadId: ThreadId.make("b-thread"),
    hostId: "local",
    deviceId: "same-device",
    platform: "android" as const,
    toolSessionId: "11111111-1111-4111-8111-111111111111",
  };
  const first: PreviewMiniPlayerSource = { kind: "device", ...resource, name: "Device", resource };
  const next: PreviewMiniPlayerSource = {
    ...first,
    resource: { ...resource, toolSessionId: "22222222-2222-4222-8222-222222222222" },
  };
  expect(previewMiniPlayerSourceKey(first)).not.toBe(previewMiniPlayerSourceKey(next));
  const store = usePreviewMiniPlayerStore.getState();
  store.open(refA, first);
  store.open(refA, next);
  store.move(refA, previewMiniPlayerSourceKey(first), { x: 900, y: 800 });
  const current = selectThreadPreviewMiniPlayer(
    usePreviewMiniPlayerStore.getState().byThreadKey,
    refA,
  );
  expect(current?.source).toEqual(next);
  expect(current?.position).toBeNull();
  const browser = {
    kind: "browser" as const,
    tabId: "same-tab",
    resource: {
      type: "preview" as const,
      environmentId: resource.environmentId,
      threadId: resource.threadId,
      tabId: "same-tab",
      serverEpoch: "epoch-one",
    },
  };
  expect(previewMiniPlayerSourceKey(browser)).not.toBe(
    previewMiniPlayerSourceKey({
      ...browser,
      resource: { ...browser.resource, environmentId: "destination-c" as EnvironmentId },
    }),
  );
});
