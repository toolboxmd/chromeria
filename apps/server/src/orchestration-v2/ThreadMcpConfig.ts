import { ThreadMcpServers, ThreadTools, type ThreadToolsRequest } from "@t3tools/contracts";
import { ToolPolicyError } from "@t3tools/provider-core/server/toolPolicy";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";

export class ThreadMcpConfig extends Context.Service<
  ThreadMcpConfig,
  {
    readonly prepare: (request: ThreadToolsRequest) => Effect.Effect<ThreadTools, ToolPolicyError>;
    readonly read: (id: string) => Effect.Effect<ThreadMcpServers, ToolPolicyError>;
  }
>()("t3/orchestration-v2/ThreadMcpConfig") {}

const make = Effect.gen(function* () {
  const secrets = yield* ServerSecretStore.ServerSecretStore;
  const crypto = yield* Crypto.Crypto;
  const codec = Schema.fromJsonString(ThreadMcpServers);
  const key = (id: string) => `thread-mcp-${id}`;
  const failure = () => new ToolPolicyError({ reason: "Thread MCP configuration is unavailable." });
  const replace = Effect.fn("ThreadMcpConfig.replace")(function* (
    id: string,
    servers: ThreadMcpServers,
  ) {
    const json = yield* Schema.encodeEffect(codec)(servers);
    yield* secrets.set(key(id), new TextEncoder().encode(json));
  }, Effect.mapError(failure));
  return ThreadMcpConfig.of({
    prepare: Effect.fn("ThreadMcpConfig.prepare")(function* (request) {
      if (request.mcpServers === undefined) return { allow: request.allow };
      const id = yield* crypto.randomUUIDv4().pipe(Effect.mapError(failure));
      yield* replace(id, request.mcpServers);
      return { allow: request.allow, mcpConfigId: id };
    }),
    read: Effect.fn("ThreadMcpConfig.read")(function* (id) {
      const value = yield* secrets.get(key(id)).pipe(Effect.mapError(failure));
      if (Option.isNone(value)) return yield* failure();
      return yield* Schema.decodeEffect(codec)(new TextDecoder().decode(value.value)).pipe(
        Effect.mapError(failure),
      );
    }),
  });
});
export const layer = Layer.effect(ThreadMcpConfig, make);
