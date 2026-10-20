import { EnvironmentId, ThreadId, PreviewStreamInput } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import {
  createPreviewStreamClient,
  type PreviewStreamControl,
  uploadPreviewStreamFiles,
  type PreviewStreamFileChooser,
  type PreviewStreamDownload,
} from "./serverBrowserStream.ts";

class FakeSocket extends EventTarget {
  static readonly OPEN = 1;
  static current: FakeSocket;
  readyState = 1;
  binaryType = "";
  readonly sent: string[] = [];
  readonly url: string;
  constructor(url: string) {
    super();
    this.url = url;
    FakeSocket.current = this;
  }
  send(value: string) {
    this.sent.push(value);
  }
  close() {
    this.readyState = 3;
  }
  message(data: unknown) {
    this.dispatchEvent(new MessageEvent("message", { data }));
  }
  control(value: PreviewStreamControl) {
    this.message(JSON.stringify({ type: "control", ...value }));
  }
}

const target = {
  access: {
    httpBase: "http://preview.test/api/preview-stream",
    wsBase: "ws://preview.test/api/preview-stream",
    query: { viewerTicket: "qualified-fixture" },
    credentials: true,
  },
  threadId: "thread",
  tabId: "tab",
  maxWidth: 800,
  maxHeight: 600,
};

describe("preview stream control", () => {
  beforeEach(() => vi.stubGlobal("WebSocket", FakeSocket));
  afterEach(() => vi.unstubAllGlobals());
  const connect = () => {
    const onFrame = vi.fn();
    const onControl = vi.fn();
    const client = createPreviewStreamClient(target, {
      onFrame,
      onControl,
      onViewport: vi.fn(),
      onConnectedChange: vi.fn(),
      onUnauthorized: vi.fn(),
    });
    return { client, socket: FakeSocket.current, onFrame, onControl };
  };
  const agent: PreviewStreamControl = {
    canOperate: true,
    controller: "agent",
    generation: 1,
    dialog: null,
  };

  it.each([10_000, 10_001, 20_000])(
    "preserves native paste output and the qualified session at %i characters",
    (length) => {
      const { client, socket } = connect();
      socket.control({ ...agent, controller: "you" });
      const text = "界".repeat(length);
      expect(client.send({ type: "text", text })).toBe(true);
      const decode = Schema.decodeUnknownSync(Schema.fromJsonString(PreviewStreamInput));
      expect(decode(socket.sent.at(-1)!)).toEqual({ type: "text", text: "界".repeat(10_000) });
      expect(client.send({ type: "text", text: "after-paste" })).toBe(true);
      expect(decode(socket.sent.at(-1)!)).toEqual({ type: "text", text: "after-paste" });
      expect(socket.readyState).toBe(FakeSocket.OPEN);
      client.stop();
    },
  );

  it("preserves native auxiliary button bits and accumulated wheel displacement", () => {
    const { client, socket } = connect();
    socket.control({ ...agent, controller: "you" });
    const decode = Schema.decodeUnknownSync(Schema.fromJsonString(PreviewStreamInput));
    const inputs: PreviewStreamInput[] = [
      {
        type: "mouse",
        action: "move",
        x: 10,
        y: 10,
        button: "none",
        buttons: 31,
        clickCount: 0,
        modifiers: 15,
      },
      { type: "wheel", x: 10, y: 10, deltaX: 100001, deltaY: -200000, modifiers: 0 },
    ];
    for (const input of inputs) {
      expect(client.send(input)).toBe(true);
      expect(decode(socket.sent.at(-1)!)).toEqual(input);
    }
    client.stop();
  });

  it("matches native measured resize semantics without sending invalid tiny sizes", () => {
    const { client, socket } = connect();
    socket.control({ ...agent, controller: "you" });
    expect(client.send({ type: "resize", width: 80, height: 600 })).toBe(true);
    expect(socket.sent).toHaveLength(0);
    expect(client.send({ type: "resize", width: 5000.4, height: 3000.7 })).toBe(true);
    const decode = Schema.decodeUnknownSync(Schema.fromJsonString(PreviewStreamInput));
    expect(decode(socket.sent.at(-1)!)).toEqual({ type: "resize", width: 3840, height: 2160 });
    client.stop();
  });

  it("reports oversized or private input without sending it or closing the session", () => {
    const rejected = vi.fn();
    const client = createPreviewStreamClient(target, {
      onFrame: vi.fn(),
      onViewport: vi.fn(),
      onConnectedChange: vi.fn(),
      onUnauthorized: vi.fn(),
      onInputRejected: rejected,
    });
    const socket = FakeSocket.current;
    socket.control({ ...agent, controller: "you" });
    const draft = "界".repeat(16385);
    for (const input of [
      { type: "dialog" as const, accept: true, promptText: draft },
      { type: "dialog" as const, accept: true, promptText: "\u0000".repeat(16384) },
      { type: "navigate" as const, url: `https://example.test/${"a".repeat(2048)}` },
      {
        type: "mouse" as const,
        action: "down" as const,
        x: 0,
        y: 0,
        button: "left" as const,
        buttons: 1,
        clickCount: 1,
        modifiers: 0,
        private: "must-not-cross",
      },
    ]) {
      expect(client.send(input)).toBe(false);
    }
    expect(rejected).toHaveBeenCalledTimes(4);
    expect(socket.sent).toHaveLength(0);
    expect(socket.readyState).toBe(FakeSocket.OPEN);
    expect(client.send({ type: "dialog", accept: true, promptText: "界".repeat(16384) })).toBe(
      true,
    );
    expect(
      Schema.decodeUnknownSync(Schema.fromJsonString(PreviewStreamInput))(socket.sent.at(-1)!),
    ).toEqual({ type: "dialog", accept: true, promptText: "界".repeat(16384) });
    expect(client.send({ type: "text", text: "after-refusal" })).toBe(true);
    client.stop();
  });

  it("admits exactly 64 KiB of qualified dialog JSON and visibly refuses one more byte", () => {
    const rejected = vi.fn();
    const client = createPreviewStreamClient(target, {
      onFrame: vi.fn(),
      onViewport: vi.fn(),
      onConnectedChange: vi.fn(),
      onUnauthorized: vi.fn(),
      onInputRejected: rejected,
    });
    const socket = FakeSocket.current;
    socket.control({ ...agent, controller: "you" });
    const promptText = "\u0000".repeat(10000) + "x".repeat(65536 - 60000 - 47);
    expect(promptText.length).toBeLessThan(16384);
    expect(client.send({ type: "dialog", accept: true, promptText })).toBe(true);
    expect(new TextEncoder().encode(socket.sent.at(-1)!)).toHaveLength(65536);
    expect(client.send({ type: "dialog", accept: true, promptText: `${promptText}x` })).toBe(false);
    expect(socket.sent).toHaveLength(1);
    expect(rejected).toHaveBeenCalledOnce();
    expect(socket.readyState).toBe(FakeSocket.OPEN);
    client.stop();
  });

  it("preserves local native prompt values outside qualified limits", () => {
    const rejected = vi.fn();
    const client = createPreviewStreamClient(
      { ...target, access: { ...target.access, query: {} } },
      {
        onFrame: vi.fn(),
        onViewport: vi.fn(),
        onConnectedChange: vi.fn(),
        onUnauthorized: vi.fn(),
        onInputRejected: rejected,
      },
    );
    const socket = FakeSocket.current;
    socket.control({ ...agent, controller: "you" });
    const input = { type: "dialog" as const, accept: true, promptText: "local".repeat(4000) };
    expect(client.send(input)).toBe(true);
    expect(JSON.parse(socket.sent.at(-1)!)).toEqual(input);
    expect(rejected).not.toHaveBeenCalled();
    client.stop();
  });

  it("keeps popup identities qualified to the current source viewer's destination and epoch", () => {
    const resource = {
      type: "preview" as const,
      environmentId: EnvironmentId.make("B"),
      threadId: ThreadId.make("thread"),
      tabId: "tab",
      serverEpoch: "epoch-b",
    };
    const onPopup = vi.fn();
    const client = createPreviewStreamClient(
      { ...target, resource },
      {
        onFrame: vi.fn(),
        onViewport: vi.fn(),
        onConnectedChange: vi.fn(),
        onUnauthorized: vi.fn(),
        onPopup,
      },
    );
    const socket = FakeSocket.current;
    const popup = { ...resource, tabId: "popup" };
    socket.message(JSON.stringify({ type: "popup", tabId: "popup", resource: popup }));
    expect(onPopup.mock.calls).toEqual([["popup", popup]]);
    for (const invalid of [
      undefined,
      { ...popup, tabId: "other" },
      { ...popup, environmentId: "C" },
      { ...popup, threadId: "other-thread" },
      { ...popup, serverEpoch: "later" },
      { ...popup, ticket: "private" },
    ]) {
      socket.message(JSON.stringify({ type: "popup", tabId: "popup", resource: invalid }));
    }
    expect(onPopup).toHaveBeenCalledTimes(1);
    client.stop();
    const legacy = createPreviewStreamClient(target, {
      onFrame: vi.fn(),
      onViewport: vi.fn(),
      onConnectedChange: vi.fn(),
      onUnauthorized: vi.fn(),
      onPopup,
    });
    FakeSocket.current.message(JSON.stringify({ type: "popup", tabId: "legacy-popup" }));
    expect(onPopup).toHaveBeenLastCalledWith("legacy-popup", undefined);
    legacy.stop();
  });

  it("marks passive viewers without changing active viewer URLs", () => {
    const { client, socket } = connect();
    expect(new URL(socket.url).searchParams.has("interactive")).toBe(false);
    client.stop();
    const passive = createPreviewStreamClient(
      { ...target, interactive: false },
      {
        onFrame: vi.fn(),
        onViewport: vi.fn(),
        onConnectedChange: vi.fn(),
        onUnauthorized: vi.fn(),
      },
    );
    expect(new URL(FakeSocket.current.url).searchParams.get("interactive")).toBe("false");
    passive.stop();
  });

  it("does not resize or navigate before ownership is granted", () => {
    const { client, socket } = connect();
    expect(client.send({ type: "resize", width: 390, height: 844 })).toBe(false);
    expect(client.send({ type: "navigate", url: "http://example.test" })).toBe(false);
    socket.control(agent);
    expect(client.send({ type: "takeControl" })).toBe(true);
    expect(client.send({ type: "text", text: "premature" })).toBe(false);
    socket.control({ ...agent, controller: "you", generation: 2 });
    expect(client.send({ type: "text", text: "owned" })).toBe(true);
    expect(socket.sent.map((entry) => JSON.parse(entry))).toEqual([
      { type: "takeControl" },
      { type: "text", text: "owned" },
    ]);
    client.stop();
  });

  it("preserves a source view-only admission even when destination control metadata offers operation", () => {
    const onFrame = vi.fn();
    const client = createPreviewStreamClient(
      { ...target, access: { ...target.access, canOperate: false } },
      {
        onFrame,
        onViewport: vi.fn(),
        onConnectedChange: vi.fn(),
        onUnauthorized: vi.fn(),
      },
    );
    const socket = FakeSocket.current;
    socket.control({ ...agent, controller: "you" });
    const frame = new ArrayBuffer(3);
    socket.message(frame);
    expect(onFrame).toHaveBeenCalledWith(frame);
    expect(client.send({ type: "takeControl" })).toBe(false);
    expect(client.send({ type: "text", text: "must not send" })).toBe(false);
    expect(socket.sent).toEqual(['{"type":"ack"}']);
    client.stop();
  });

  it("does not grant control from malformed dialog metadata", () => {
    const { client, socket, onControl } = connect();
    socket.message(
      JSON.stringify({
        type: "control",
        ...agent,
        controller: "you",
        dialog: { type: "prompt", message: 42, defaultValue: "" },
      }),
    );
    expect(onControl).not.toHaveBeenCalled();
    expect(client.send({ type: "reload" })).toBe(false);
    client.stop();
  });

  it("keeps read-only frames flowing while refusing takeover and every mutation", () => {
    const { client, socket, onFrame } = connect();
    socket.control({ ...agent, canOperate: false });
    const frame = new ArrayBuffer(3);
    socket.message(frame);
    expect(onFrame).toHaveBeenCalledWith(frame);
    expect(client.send({ type: "takeControl" })).toBe(false);
    expect(client.send({ type: "resize", width: 390, height: 844 })).toBe(false);
    expect(client.send({ type: "reload" })).toBe(false);
    expect(socket.sent).toEqual(['{"type":"ack"}']);
    client.stop();
  });

  it("revokes input immediately on release and exposes pending dialog state", () => {
    const { client, socket, onControl } = connect();
    const owned = {
      ...agent,
      controller: "you" as const,
      dialog: { type: "prompt", message: "Name?", defaultValue: "initial" },
    };
    socket.control(owned);
    expect(onControl).toHaveBeenLastCalledWith(owned);
    expect(client.send({ type: "dialog", accept: true, promptText: "answer" })).toBe(true);
    expect(client.send({ type: "releaseControl" })).toBe(true);
    socket.control({ ...agent, generation: 2 });
    expect(client.send({ type: "text", text: "late input" })).toBe(false);
    expect(client.send({ type: "dialog", accept: false })).toBe(false);
    client.stop();
  });
});

describe("preview stream downloads", () => {
  beforeEach(() => vi.stubGlobal("WebSocket", FakeSocket));
  afterEach(() => vi.unstubAllGlobals());

  it("offers a download at a URL carrying the stream's ticket", () => {
    const onDownload = vi.fn();
    createPreviewStreamClient(
      {
        ...target,
        access: { ...target.access, query: { wsTicket: "ticket" }, credentials: false },
      },
      {
        onFrame: vi.fn(),
        onDownload,
        onViewport: vi.fn(),
        onConnectedChange: vi.fn(),
        onUnauthorized: vi.fn(),
      },
    );
    FakeSocket.current.message(
      JSON.stringify({ type: "download", id: "d1", fileName: "a b.csv", sizeBytes: 3 }),
    );
    expect(onDownload).toHaveBeenCalledExactlyOnceWith({
      fileName: "a b.csv",
      sizeBytes: 3,
      url: "http://preview.test/api/preview-stream/download?threadId=thread&tabId=tab&id=d1&wsTicket=ticket",
    });
  });
});

describe("preview stream agent pointer", () => {
  beforeEach(() => vi.stubGlobal("WebSocket", FakeSocket));
  afterEach(() => vi.unstubAllGlobals());

  it("reports where the agent moves and clicks, ignoring malformed points", () => {
    const onPointer = vi.fn();
    createPreviewStreamClient(target, {
      onFrame: vi.fn(),
      onPointer,
      onViewport: vi.fn(),
      onConnectedChange: vi.fn(),
      onUnauthorized: vi.fn(),
    });
    FakeSocket.current.message(
      JSON.stringify({ type: "pointer", phase: "drag", x: 1, y: 2, sequence: 1 }),
    );
    FakeSocket.current.message(
      JSON.stringify({ type: "pointer", phase: "click", x: 140, y: 50, sequence: 2 }),
    );
    expect(onPointer).toHaveBeenCalledExactlyOnceWith({
      phase: "click",
      x: 140,
      y: 50,
      sequence: 2,
    });
  });
});

it("obtains current same-viewer admission when a captured picker or download is actually used", async () => {
  vi.stubGlobal("WebSocket", FakeSocket);
  const requests: Array<{ url: string; body: FormData | undefined }> = [];
  vi.stubGlobal("fetch", async (url: string, input?: { body: FormData }) => {
    requests.push({ url, body: input?.body });
    return new Response(null, { status: 204 });
  });
  let ticket = "expired-opening-ticket";
  let refused = false;
  const renew = vi.fn(async () => {
    if (refused) throw new Error("Original authority retired");
    return { ...target.access, query: { viewerTicket: ticket } };
  });
  let chooser: PreviewStreamFileChooser | null = null;
  let download: PreviewStreamDownload | undefined;
  const client = createPreviewStreamClient(
    {
      ...target,
      access: {
        ...target.access,
        query: { viewerTicket: "expired-opening-ticket" },
        requestCurrentAccess: renew,
      },
    },
    {
      onFrame: vi.fn(),
      onViewport: vi.fn(),
      onConnectedChange: vi.fn(),
      onUnauthorized: vi.fn(),
      onFileChooser: (value) => {
        chooser = value;
      },
      onDownload: (value) => {
        download = value;
      },
    },
  );
  try {
    FakeSocket.current.message(
      JSON.stringify({ type: "fileChooser", id: "exact-picker", multiple: false, accept: "" }),
    );
    FakeSocket.current.message(
      JSON.stringify({
        type: "download",
        id: "exact-download",
        fileName: "report.txt",
        sizeBytes: 4,
      }),
    );
    expect(renew).not.toHaveBeenCalled();
    ticket = "fresh-upload-ticket";
    await uploadPreviewStreamFiles(chooser!, [new File(["body"], "chosen.txt")]);
    expect(requests).toHaveLength(1);
    expect(new URL(requests[0]!.url).searchParams.get("viewerTicket")).toBe("fresh-upload-ticket");
    expect(new URL(requests[0]!.url).searchParams.get("chooser")).toBe("exact-picker");
    const file = requests[0]!.body!.get("file");
    expect(file).toBeInstanceOf(File);
    expect(await (file as File).text()).toBe("body");
    ticket = "fresh-cancel-ticket";
    await uploadPreviewStreamFiles(chooser!, []);
    expect(requests).toHaveLength(2);
    expect(new URL(requests[1]!.url).searchParams.get("viewerTicket")).toBe("fresh-cancel-ticket");
    expect([...requests[1]!.body!.keys()]).toEqual([]);
    ticket = "fresh-download-ticket";
    const url = new URL(await download!.resolveUrl!());
    expect(url.searchParams.get("viewerTicket")).toBe("fresh-download-ticket");
    expect(url.searchParams.get("id")).toBe("exact-download");
    refused = true;
    await expect(uploadPreviewStreamFiles(chooser!, [])).rejects.toThrow(
      "Original authority retired",
    );
    await expect(download!.resolveUrl!()).rejects.toThrow("Original authority retired");
    expect(requests).toHaveLength(2);
    expect(FakeSocket.current.readyState).toBe(FakeSocket.OPEN);
  } finally {
    client.stop();
    vi.unstubAllGlobals();
  }
});
