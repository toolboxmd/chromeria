import { afterEach, expect, it, vi } from "vite-plus/test";
import { captureDeviceScreenshot } from "./screenshot.ts";

// Signature and an empty IEND chunk: the smallest bytes a complete PNG check accepts.
const PNG = [137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130];

afterEach(() => vi.unstubAllGlobals());
const target = {
  platform: "android" as const,
  deviceId: "phone with spaces",
  access: {
    httpBase: "https://remote.example/api/device-hub/hosts/remote",
    wsBase: "wss://remote.example",
    query: { ticket: "capture-ticket" },
    credentials: false,
  },
};

it("requests current same-viewer admission before a late screenshot and never retries a retired request", async () => {
  const fetch = vi.fn(
    async (_url: string) =>
      new Response(new Uint8Array(PNG), { headers: { "content-type": "image/png" } }),
  );
  vi.stubGlobal("fetch", fetch);
  let refused = false;
  const current = vi.fn(async () => {
    if (refused) throw new Error("Original device authority retired");
    return { ...target.access, query: { viewerTicket: "fresh-screenshot-ticket" } };
  });
  const qualified = {
    ...target,
    access: { ...target.access, query: { viewerTicket: "expired" }, requestCurrentAccess: current },
  };
  const shot = await captureDeviceScreenshot(qualified, new AbortController().signal);
  expect(Array.from(new Uint8Array(await shot.arrayBuffer()))).toEqual(PNG);
  expect(fetch).toHaveBeenCalledTimes(1);
  expect(new URL(fetch.mock.calls[0]![0]!).searchParams.get("viewerTicket")).toBe(
    "fresh-screenshot-ticket",
  );
  refused = true;
  await expect(captureDeviceScreenshot(qualified, new AbortController().signal)).rejects.toThrow(
    "Original device authority retired",
  );
  expect(fetch).toHaveBeenCalledTimes(1);
});
