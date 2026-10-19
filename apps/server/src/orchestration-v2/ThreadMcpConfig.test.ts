import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { ThreadTools } from "@t3tools/contracts";
import * as Config from "../config.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as ThreadMcpConfig from "./ThreadMcpConfig.ts";

const encodeTools = Schema.encodeEffect(Schema.fromJsonString(ThreadTools));

it.effect(
  "restores independent authenticated MCP configurations from the private store after reconstruction",
  () => {
    const config = Config.layerTest(process.cwd(), { prefix: "t3-thread-mcp-" });
    const store = Secrets.layer.pipe(Layer.provide(config));
    const make = () =>
      ThreadMcpConfig.ThreadMcpConfig.pipe(Effect.provide(Layer.fresh(ThreadMcpConfig.layer)));
    return Effect.gen(function* () {
      const first = yield* make();
      const a = yield* first.prepare({
        allow: ["mcp:outreach:read_venue"],
        mcpServers: {
          outreach: {
            type: "http",
            url: "http://localhost/mcp",
            headers: { Authorization: "venue-a-generation-1" },
          },
        },
      });
      const b = yield* first.prepare({
        allow: ["mcp:outreach:read_venue"],
        mcpServers: {
          outreach: {
            type: "http",
            url: "http://localhost/mcp",
            headers: { Authorization: "venue-b-generation-1" },
          },
        },
      });
      expect(a.mcpConfigId).not.toEqual(b.mcpConfigId);
      expect(yield* encodeTools(a)).not.toContain("venue-a");
      const restarted = yield* make();
      expect(yield* restarted.read(a.mcpConfigId!)).toMatchObject({
        outreach: { headers: { Authorization: "venue-a-generation-1" } },
      });
      expect(yield* restarted.read(b.mcpConfigId!)).toMatchObject({
        outreach: { headers: { Authorization: "venue-b-generation-1" } },
      });
      const error = yield* restarted.read("00000000-0000-0000-0000-000000000000").pipe(Effect.flip);
      expect(error.message).toBe("Thread MCP configuration is unavailable.");
    }).pipe(Effect.provide(store.pipe(Layer.provideMerge(NodeServices.layer))));
  },
);
