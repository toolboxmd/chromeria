import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ServerSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as Config from "../config.ts";
import * as Persistence from "../persistence/Sqlite.ts";
import * as Settings from "../serverSettings.ts";

const settingsLayer = Settings.layer.pipe(
  Layer.provide(Secrets.layer),
  Layer.provideMerge(Persistence.layerMemory),
  Layer.provideMerge(Config.layerTest(process.cwd(), { prefix: "prism-settings-" })),
  Layer.provideMerge(NodeServices.layer),
);
it.effect("Prism settings writes preserve a saved reset-recovery opt-out", () =>
  Effect.gen(function* () {
    const config = yield* Config.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const settings = yield* Settings.ServerSettingsService;
    yield* fs.writeFileString(config.settingsPath, '{"autoResumeLimitedThreads":false}');
    assert.strictEqual((yield* settings.getSettings).autoResumeLimitedThreads, false);
    yield* settings.updateSettings({
      prismRoles: { planner: { instructions: "Use the project plan." } },
    });
    const persisted = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ServerSettings))(
      yield* fs.readFileString(config.settingsPath),
    );
    assert.strictEqual(persisted.autoResumeLimitedThreads, false);
    assert.strictEqual(persisted.prismRoles.planner.instructions, "Use the project plan.");
  }).pipe(Effect.provide(settingsLayer)),
);
