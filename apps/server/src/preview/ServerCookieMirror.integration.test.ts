// @effect-diagnostics nodeBuiltinImport:off - Runs an isolated real Chromium profile against synthetic cookies.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeFS from "node:fs";
import { cdpCookieMirrorStore } from "@t3tools/shared/browserCookieMirror";
import { EnvironmentId, type CookieMirrorBatch, type MirrorCookie } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";

import { ServerBrowserContexts } from "./ServerBrowserContexts.ts";
import { ServerCookieMirror } from "./ServerCookieMirror.ts";
import { ServerCookieMirrorProfiles } from "./ServerCookieMirrorProfiles.ts";

const executable =
  process.env.CHROMERIA_TEST_CHROMIUM ??
  ["/usr/bin/chromium", "/usr/bin/chromium-browser"].find(NodeFS.existsSync);
const environmentId = EnvironmentId.make("synthetic-environment");
const cookie: MirrorCookie = {
  name: "__Host-login",
  value: "synthetic-session",
  domain: "login.example.test",
  path: "/",
  secure: true,
  httpOnly: true,
  sameSite: "unspecified",
};
describe.skipIf(!executable)("real Playwright Chromium mirror adapter", () => {
  it.live(
    "preserves attributes and exact partition keys, isolates profiles, and persists the profile catalog",
    () =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        const persistent: MirrorCookie = {
          name: "persistent",
          value: "synthetic-persistent",
          domain: ".example.test",
          path: "/account",
          secure: true,
          httpOnly: true,
          sameSite: "strict",
          expires: Math.floor(now / 1000) + 86400,
        };
        const partitioned: MirrorCookie = {
          ...persistent,
          name: "partitioned",
          sameSite: "no_restriction",
          partitionKey: { topLevelSite: "https://top.test", hasCrossSiteAncestor: false },
        };
        const sibling: MirrorCookie = {
          ...partitioned,
          value: "synthetic-sibling",
          partitionKey: { topLevelSite: "https://top.test", hasCrossSiteAncestor: true },
        };

        return yield* Effect.promise(async () => {
          const dir = await NodeFSP.mkdtemp(
            NodePath.join(NodeOS.tmpdir(), "chromeria-server-mirror-adapter-"),
          );
          const contexts = new ServerBrowserContexts({
            profilesDir: NodePath.join(dir, "profiles"),
            executable: async () => executable!,
            env: { ...process.env, T3CODE_SERVER_BROWSER_SANDBOX: "0" },
          });
          const mirror = new ServerCookieMirror(contexts);
          try {
            const profile = { id: "mirrored-work", name: "Work", kind: "persistent" } as const;
            const batch: CookieMirrorBatch = {
              environmentId,
              sequence: 0,
              profile,
              mutations: [cookie, persistent, partitioned, sibling].map((cookie) => ({
                type: "set",
                cookie,
              })),
            };
            const context = await mirror.apply(batch);
            expect(context).toBe(await contexts.contextFor(profile.id));
            const page = await context.newPage();
            const cdp = await context.newCDPSession(page);
            const store = cdpCookieMirrorStore(cdp);
            expect(
              [...(await store.read())].sort(
                (a, b) =>
                  a.name.localeCompare(b.name) ||
                  Number(a.partitionKey?.hasCrossSiteAncestor) -
                    Number(b.partitionKey?.hasCrossSiteAncestor),
              ),
            ).toEqual([cookie, partitioned, sibling, persistent]);
            const other = await contexts.contextFor("different-profile");
            expect(await other.cookies()).toEqual([]);
            await mirror.apply({
              ...batch,
              sequence: 1,
              mutations: [
                {
                  type: "remove",
                  key: {
                    name: partitioned.name,
                    domain: partitioned.domain,
                    path: partitioned.path,
                    partitionKey: partitioned.partitionKey!,
                  },
                },
              ],
            });
            expect((await store.read()).filter((cookie) => cookie.name === "partitioned")).toEqual([
              sibling,
            ]);
            expect((await store.read()).find((item) => item.name === "__Host-login")).toEqual(
              cookie,
            );
            await cdp.detach();
            await page.close();
            const catalogPath = NodePath.join(dir, "mirrored-profiles.json");
            const catalog = new ServerCookieMirrorProfiles(catalogPath);
            expect(await catalog.read()).toEqual([]);
            await catalog.put(profile);
            expect(await new ServerCookieMirrorProfiles(catalogPath).read()).toEqual([profile]);
            expect(await NodeFSP.readFile(catalogPath, "utf8")).not.toContain("synthetic-session");
          } finally {
            await contexts.close();
            await NodeFSP.rm(dir, { recursive: true, force: true });
          }
        });
      }),
  );
});
