// @effect-diagnostics nodeBuiltinImport:off - Exercises the real metadata file in a disposable directory.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { describe, expect, it } from "vite-plus/test";

import { CookieMirror } from "./CookieMirror.ts";
import { CookieMirrorMetadataFile } from "./CookieMirrorMetadata.ts";

describe("durable value-free mirror metadata", () => {
  it("round-trips managed identities, profile mappings and pending removals without values", async () => {
    const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "chromeria-mirror-metadata-"));
    const path = NodePath.join(dir, "metadata.json");
    try {
      const file = new CookieMirrorMetadataFile(path);
      const mirror = new CookieMirror({
        metadata: await file.read(),
        persist: (metadata) => file.write(metadata),
      });
      await mirror.mapChromeProfile({
        root: "/synthetic/chrome",
        directory: "Profile 1",
        name: "Work",
        proposedId: "chrome-work",
        profiles: [],
      });
      const profile = { id: "chrome-work", name: "Work", kind: "persistent" } as const;
      await mirror.addPartition("partition", "env", profile, {
        read: async () => [
          {
            name: "login",
            value: "synthetic-private-value",
            domain: "example.test",
            path: "/",
            httpOnly: true,
            secure: true,
            sameSite: "unspecified",
            expires: 2_000_000_000,
          },
        ],
        apply: async () => {},
      });
      const batch = mirror.snapshot("env")[0]!;
      await mirror.prepareDelivery("env", profile.id, batch.mutations);
      const raw = await NodeFSP.readFile(path, "utf8");
      expect(raw).not.toContain("synthetic-private-value");
      expect(raw).not.toContain('"value"');
      expect(raw).toContain("login");
      const restored = new CookieMirrorMetadataFile(path);
      expect(await restored.read()).toEqual(mirror.metadata());
      expect(await NodeFSP.readdir(dir)).toEqual(["metadata.json"]);
    } finally {
      await NodeFSP.rm(dir, { recursive: true, force: true });
    }
  });

  it("fails closed on corrupt state without replacing it with an empty baseline", async () => {
    const dir = await NodeFSP.mkdtemp(
      NodePath.join(NodeOS.tmpdir(), "chromeria-mirror-metadata-bad-"),
    );
    try {
      const path = NodePath.join(dir, "metadata.json");
      await NodeFSP.writeFile(path, "synthetic-invalid-metadata");
      await expect(new CookieMirrorMetadataFile(path).read()).rejects.toThrow(
        "Could not read browser mirror metadata.",
      );
      expect(await NodeFSP.readFile(path, "utf8")).toBe("synthetic-invalid-metadata");
    } finally {
      await NodeFSP.rm(dir, { recursive: true, force: true });
    }
  });
});
