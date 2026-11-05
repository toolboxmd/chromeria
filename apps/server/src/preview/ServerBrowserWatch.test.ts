// @effect-diagnostics nodeBuiltinImport:off - Emulates the CDP transport and executes the real page bootstrap in a separate JS realm.
import * as NodeEvents from "node:events";
import * as NodeVM from "node:vm";
import { expect, it } from "vite-plus/test";
import { ThreadId } from "@t3tools/contracts";
import type { CDPSession } from "playwright-core";
import { PageWatchStore } from "./PageWatchStore.ts";
import { installPageWatch } from "./ServerBrowserWatch.ts";

const fixture = async (subscription: string) => {
  const events = new NodeEvents.EventEmitter();
  const realm = NodeVM.createContext({ TextEncoder });
  let bindingName = "";
  let packet = "";
  let cleanup: (() => void) | undefined;
  const session = {
    on: (name: string, listener: (...args: unknown[]) => void) => events.on(name, listener),
    off: (name: string, listener: (...args: unknown[]) => void) => events.off(name, listener),
    send: async (method: string, input?: Record<string, unknown>) => {
      if (method === "Page.getFrameTree") return { frameTree: { frame: { id: "main" } } };
      if (method === "Runtime.enable") {
        events.emit("Runtime.executionContextCreated", {
          context: { id: 17, auxData: { isDefault: true, frameId: "main" } },
        });
        return {};
      }
      if (method === "Runtime.addBinding") {
        bindingName = String(input?.name);
        realm[bindingName] = (payload: string) => {
          packet = payload;
          events.emit("Runtime.bindingCalled", {
            name: bindingName,
            payload,
            executionContextId: 17,
          });
        };
        return {};
      }
      if (method === "Runtime.evaluate") {
        cleanup = NodeVM.runInContext(String(input?.expression), realm) as () => void;
        return { result: { type: "function", objectId: "cleanup-17" } };
      }
      if (method === "Runtime.callFunctionOn") {
        cleanup?.();
        return { result: {} };
      }
      return {};
    },
  };
  const store = new PageWatchStore("epoch");
  const threadId = ThreadId.make("synthetic");
  let disposed: (() => Promise<void>) | undefined;
  store.register("watch", threadId, {
    authorize: async () => {},
    fence: () => undefined,
    cleanup: async () => {
      await disposed?.();
    },
  });
  disposed = await installPageWatch({
    cdp: session as unknown as CDPSession,
    watchId: "watch",
    subscription,
    store,
    authorize: async () => {},
  });
  const read = () =>
    store.read(
      { threadId, watchId: "watch", epoch: "epoch", after: 0 },
      async () => {},
      new AbortController().signal,
    );
  return {
    store,
    read,
    realm,
    events,
    packet: () => packet,
    bindingName: () => bindingName,
    stop: disposed,
  };
};

it("a main-frame subscription cannot receive callbacks from an old or child execution context", async () => {
  const f = await fixture(
    "(emit) => { emit({ baseline: 1 }); return () => { globalThis.cleaned = true; }; }",
  );
  f.events.emit("Runtime.bindingCalled", {
    name: f.bindingName(),
    executionContextId: 16,
    payload: f.packet(),
  });
  f.events.emit("Runtime.bindingCalled", {
    name: "old-binding",
    executionContextId: 17,
    payload: f.packet(),
  });
  expect((await f.read()).events).toEqual([{ sequence: 1, value: { baseline: 1 } }]);
  await f.stop();
  expect(f.realm.cleaned).toBe(true);
  expect(f.events.listenerCount("Runtime.bindingCalled")).toBe(0);
});

it("malformed authenticated callback packets fail terminally instead of dropping them", async () => {
  const f = await fixture("(emit) => { emit(1); return () => {}; }");
  f.events.emit("Runtime.bindingCalled", {
    name: f.bindingName(),
    executionContextId: 17,
    payload: '{"watchId":"watch","token":"forged","json":"2"}',
  });
  await expect(f.read()).rejects.toMatchObject({ reason: "malformed_payload" });
});

it("destroying the opening's main context fences all later callbacks", async () => {
  const f = await fixture("(emit) => { emit(1); return () => {}; }");
  f.events.emit("Runtime.executionContextDestroyed", { executionContextId: 17 });
  f.events.emit("Runtime.bindingCalled", {
    name: f.bindingName(),
    executionContextId: 17,
    payload: f.packet(),
  });
  await expect(f.read()).rejects.toMatchObject({ reason: "navigation" });
});

it.each([
  "undefined",
  "NaN",
  "Infinity",
  "() => {}",
  "new Date()",
  "{toJSON() { return 1; }}",
  "[,1]",
  "({get secret() { return 1; }})",
])("rejects lossy JSON input %s at the page boundary", async (value) => {
  const f = await fixture(`(emit) => { globalThis.send = () => emit(${value}); return () => {}; }`);
  NodeVM.runInContext("send()", f.realm);
  await expect(f.read()).rejects.toMatchObject({ reason: "malformed_payload" });
});

it("a lost native callback session is explicit terminal failure", async () => {
  const f = await fixture("(emit) => { emit(1); return () => {}; }");
  f.events.emit("close");
  await expect(f.read()).rejects.toMatchObject({ reason: "closed" });
});

it("array properties cannot be silently lost through JSON serialization", async () => {
  const f = await fixture(
    "(emit) => { globalThis.send = () => { const value = [1]; value.extra = 2; emit(value); }; return () => {}; }",
  );
  NodeVM.runInContext("send()", f.realm);
  await expect(f.read()).rejects.toMatchObject({ reason: "malformed_payload" });
});
