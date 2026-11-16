import { CookieMirrorBatch, type MirrorCookie } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { afterEach, describe, expect, it, vi } from "vite-plus/test";

import { CookieMirror } from "./CookieMirror.ts";
import { CookieMirrorSocket } from "./CookieMirrorSocket.ts";

const decode = Schema.decodeUnknownSync(Schema.fromJsonString(CookieMirrorBatch));
class SocketFixture extends EventTarget {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSED = 3;
  static current: SocketFixture;
  readyState = 0;
  readonly messages: ReturnType<typeof decode>[] = [];
  private readonly queued: ReturnType<typeof decode>[] = [];
  private nextMessage: ((message: ReturnType<typeof decode>) => void) | undefined;
  constructor(_url: string) {
    super();
    SocketFixture.current = this;
  }
  send(message: string) {
    const batch = decode(message);
    this.messages.push(batch);
    if (this.nextMessage) {
      this.nextMessage(batch);
      this.nextMessage = undefined;
    } else this.queued.push(batch);
  }
  next() {
    const queued = this.queued.shift();
    return queued
      ? Promise.resolve(queued)
      : new Promise<ReturnType<typeof decode>>((resolve) => {
          this.nextMessage = resolve;
        });
  }
  receive(message: unknown) {
    this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(message) }));
  }
  ready(environmentId = "env") {
    this.readyState = 1;
    this.receive({ type: "ready", environmentId });
  }
  close() {
    this.readyState = 3;
    this.dispatchEvent(new Event("close"));
  }
}
const profile = { id: "default", name: "Default", kind: "persistent" } as const;
const cookie: MirrorCookie = {
  name: "login",
  value: "synthetic",
  domain: "example.test",
  path: "/",
  secure: true,
  httpOnly: true,
  sameSite: "unspecified",
};
const fixture = async () => {
  vi.stubGlobal("WebSocket", SocketFixture);
  let socket: CookieMirrorSocket | undefined;
  const mirror = new CookieMirror({
    changed: () => {
      void socket?.flush();
    },
  });
  await mirror.addPartition("partition", "env", profile, {
    read: async () => [cookie],
    apply: async () => {},
  });
  socket = new CookieMirrorSocket("wss://synthetic.test/mirror", "env", mirror);
  return { mirror, socket, wire: SocketFixture.current };
};
afterEach(() => {
  SocketFixture.current?.close();
  vi.unstubAllGlobals();
});

describe("desktop cookie mirror stream", () => {
  it("serializes writes behind acknowledgements and sends nothing for an unchanged source generation", async () => {
    const { mirror, socket, wire } = await fixture();
    wire.ready();
    const first = await wire.next();
    expect(first.environmentId).toBe("env");
    expect(first.sequence).toBe(0);
    await mirror.observeChrome("chrome", profile, {
      cookies: [{ ...cookie, value: "new" }],
      identities: [],
      complete: false,
    });
    expect(wire.messages).toHaveLength(1);
    wire.receive({ type: "ack", sequence: 0 });
    const second = await wire.next();
    expect(second.sequence).toBe(1);
    expect(second.mutations).toEqual([{ type: "set", cookie: { ...cookie, value: "new" } }]);
    wire.receive({ type: "ack", sequence: 1 });
    await socket.flush();
    await socket.flush();
    expect(wire.messages).toHaveLength(2);
    socket.close();
  });

  it("sends a full state when the destination context changes, including a reset racing an acknowledgement", async () => {
    const { socket, wire } = await fixture();
    wire.ready();
    const first = await wire.next();
    wire.receive({ type: "resync", profileId: "default" });
    wire.receive({ type: "ack", sequence: first.sequence });
    const replay = await wire.next();
    expect(replay.sequence).toBe(1);
    expect(replay.mutations).toEqual(first.mutations);
    wire.receive({ type: "ack", sequence: replay.sequence });
    await socket.flush();
    expect(wire.messages).toHaveLength(2);
    socket.close();
  });

  it("does not release cookies to a different environment", async () => {
    const { socket, wire } = await fixture();
    wire.ready("wrong-environment");
    await socket.flush();
    expect(wire.messages).toEqual([]);
    expect(socket.connected).toBe(false);
  });
});
