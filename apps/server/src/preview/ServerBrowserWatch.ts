// @effect-diagnostics nodeBuiltinImport:off - CDP is the native callback boundary.
import {
  PREVIEW_WATCH_MAX_PAYLOAD_BYTES,
  PREVIEW_WATCH_MAX_EVENTS,
  PREVIEW_WATCH_MAX_BYTES,
  PreviewWatchError,
} from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";
import * as Schema from "effect/Schema";
import type { CDPSession } from "playwright-core";
import type { PageWatchStore } from "./PageWatchStore.ts";

const Packet = Schema.Union([
  Schema.Struct({ watchId: Schema.String, token: Schema.String, json: Schema.String }),
  Schema.Struct({
    watchId: Schema.String,
    token: Schema.String,
    error: Schema.Literals(["malformed_payload", "overflow"]),
  }),
]);
const decodePacket = Schema.decodeUnknownSync(Schema.fromJsonString(Packet));

/** One private binding in the exact main-frame execution context; never a DOM observer or poller. */
export async function installPageWatch(input: {
  cdp: CDPSession;
  watchId: string;
  subscription: string;
  store: PageWatchStore;
  authorize: () => Promise<void>;
}) {
  const { cdp, watchId, store } = input;
  const name = `__chromeria_watch_${NodeCrypto.randomBytes(16).toString("hex")}`;
  const token = NodeCrypto.randomBytes(32).toString("hex");
  let contextId: number | undefined;
  let objectId: string | undefined;
  let disposed = false;
  const { frameTree } = await cdp.send("Page.getFrameTree");
  const context = (event: { context: { id: number; auxData?: unknown } }) => {
    const aux = event.context.auxData;
    if (
      typeof aux === "object" &&
      aux !== null &&
      "isDefault" in aux &&
      aux.isDefault === true &&
      "frameId" in aux &&
      aux.frameId === frameTree.frame.id
    )
      contextId = event.context.id;
  };
  cdp.on("Runtime.executionContextCreated", context);
  try {
    await cdp.send("Runtime.enable");
  } finally {
    cdp.off("Runtime.executionContextCreated", context);
  }
  if (contextId === undefined) throw new PreviewWatchError({ reason: "subscription_failed" });
  const openingContextId = contextId;
  const terminal = (reason: PreviewWatchError["reason"]) => {
    void store.terminate(watchId, reason).catch(() => {});
  };
  const binding = (event: { name: string; payload: string; executionContextId: number }) => {
    if (
      disposed ||
      event.name !== name ||
      event.executionContextId !== openingContextId ||
      !store.active(watchId)
    )
      return;
    if (Buffer.byteLength(event.payload, "utf8") > PREVIEW_WATCH_MAX_PAYLOAD_BYTES * 6 + 1024) {
      terminal("overflow");
      return;
    }
    try {
      const packet = decodePacket(event.payload);
      if (packet.watchId !== watchId || packet.token !== token) {
        terminal("malformed_payload");
        return;
      }
      if ("error" in packet) {
        terminal(packet.error);
        return;
      }
      void store.emit(watchId, packet.json);
    } catch {
      terminal("malformed_payload");
    }
  };
  const destroyed = (event: { executionContextId: number }) => {
    if (event.executionContextId === openingContextId) terminal("navigation");
  };
  const cleared = () => terminal("navigation");
  const closed = () => terminal("closed");
  cdp.on("close", closed);
  cdp.on("Runtime.bindingCalled", binding);
  cdp.on("Runtime.executionContextDestroyed", destroyed);
  cdp.on("Runtime.executionContextsCleared", cleared);
  const cleanup = async () => {
    if (disposed) return;
    disposed = true;
    cdp.off("close", closed);
    cdp.off("Runtime.bindingCalled", binding);
    cdp.off("Runtime.executionContextDestroyed", destroyed);
    cdp.off("Runtime.executionContextsCleared", cleared);
    try {
      if (objectId)
        await cdp.send("Runtime.callFunctionOn", {
          objectId,
          functionDeclaration: "function() { this(); }",
          returnByValue: true,
        });
    } finally {
      if (objectId) await cdp.send("Runtime.releaseObject", { objectId }).catch(() => {});
      await cdp.send("Runtime.removeBinding", { name }).catch(() => {});
    }
  };
  try {
    await input.authorize();
    await cdp.send("Runtime.addBinding", { name, executionContextId: openingContextId });
    await input.authorize();
    const result = await cdp.send("Runtime.evaluate", {
      contextId: openingContextId,
      returnByValue: false,
      expression: `(() => {
        const binding = globalThis[${JSON.stringify(name)}];
        delete globalThis[${JSON.stringify(name)}];
        const stringify = JSON.stringify;
        let stopped = false;
        let count = 0;
        let bytes = 0;
        const packet = value => binding(stringify({watchId:${JSON.stringify(watchId)},token:${JSON.stringify(token)},...value}));
        const validate = (value, seen = new Set(), depth = 0) => {
          if (depth > 100) throw new Error('depth');
          if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
          if (typeof value === 'number' && Number.isFinite(value)) return;
          if (typeof value !== 'object' || seen.has(value)) throw new Error('json');
          if (typeof value.toJSON === 'function') throw new Error('toJSON');
          const proto = Object.getPrototypeOf(value);
          if (!Array.isArray(value) && proto !== Object.prototype && proto !== null) throw new Error('prototype');
          seen.add(value);
          if (Array.isArray(value)) {
            if (Reflect.ownKeys(value).length !== value.length + 1) throw new Error('array keys');
            for (let index = 0; index < value.length; index++) {
              const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
              if (!descriptor || !descriptor.enumerable || !('value' in descriptor)) throw new Error('array entry');
              validate(descriptor.value, seen, depth + 1);
            }
          } else {
            if (Reflect.ownKeys(value).some(key => typeof key !== 'string')) throw new Error('key');
            for (const descriptor of Object.values(Object.getOwnPropertyDescriptors(value))) {
              if (!descriptor.enumerable || !('value' in descriptor)) throw new Error('descriptor');
              validate(descriptor.value, seen, depth + 1);
            }
          }
          seen.delete(value);
        };
        const emit = value => {
          if (stopped) return;
          try {
            validate(value);
            const json = stringify(value);
            const size = new TextEncoder().encode(json).byteLength;
            if (++count > ${PREVIEW_WATCH_MAX_EVENTS} || size > ${PREVIEW_WATCH_MAX_PAYLOAD_BYTES} || (bytes += size) > ${PREVIEW_WATCH_MAX_BYTES}) {
              stopped = true; packet({error:'overflow'}); return;
            }
            packet({json});
          }
          catch { stopped = true; packet({error:'malformed_payload'}); }
        };
        const subscribe = (${input.subscription});
        const dispose = subscribe(emit);
        if (typeof dispose !== 'function') throw new Error('Subscription must return cleanup');
        return () => { stopped = true; dispose(); };
      })()`,
    });
    objectId = result.result.objectId;
    if (result.exceptionDetails || !objectId || result.result.type !== "function")
      throw new PreviewWatchError({ reason: "subscription_failed" });
    await input.authorize();
    await store.flush(watchId);
    if (disposed)
      await cdp
        .send("Runtime.callFunctionOn", { objectId, functionDeclaration: "function() { this(); }" })
        .catch(() => {});
    return cleanup;
  } catch (cause) {
    await cleanup().catch(() => {});
    throw cause;
  }
}
