// @effect-diagnostics nodeBuiltinImport:off - Tests write a scratch dist-electron with Node's filesystem.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, describe, it } from "@effect/vitest";
import { afterEach } from "vite-plus/test";

import {
  DESKTOP_IDENTITY_MARKER,
  desktopIdentityMarkerMismatch,
  desktopIdentityMarkerPlugin,
} from "./desktop-identity-marker.ts";

const dirs: string[] = [];
const distDir = () => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "chromeria-identity-marker-"));
  dirs.push(dir);
  return dir;
};
const writeMain = (dir: string, source: string) =>
  NodeFS.writeFileSync(NodePath.join(dir, "main.cjs"), source);
const packMain = (dir: string, variant: string) =>
  desktopIdentityMarkerPlugin(variant).writeBundle({ dir });

afterEach(() => {
  for (const dir of dirs.splice(0)) NodeFS.rmSync(dir, { recursive: true, force: true });
});

describe("desktop identity marker", () => {
  it("accepts the main bundle its own pack wrote", () => {
    const dir = distDir();
    writeMain(dir, 'const variant = "v2";');
    packMain(dir, "v2");

    assert.isUndefined(desktopIdentityMarkerMismatch(dir, "v2"));
  });

  it("rejects a bundle built for another variant", () => {
    const dir = distDir();
    writeMain(dir, 'const variant = "default";');
    packMain(dir, "default");

    assert.include(desktopIdentityMarkerMismatch(dir, "v2"), 'built for variant "default"');
  });

  it("rejects a bundle without a marker, as left by a failed or pre-identity pack", () => {
    const dir = distDir();
    writeMain(dir, 'const variant = "v2";');

    assert.include(desktopIdentityMarkerMismatch(dir, "v2"), "missing or unreadable");

    const failed = distDir();
    assert.throws(() => packMain(failed, "v2"));
    assert.isFalse(NodeFS.existsSync(NodePath.join(failed, DESKTOP_IDENTITY_MARKER)));
  });

  it("rejects a main bundle replaced or removed after its marker was written", () => {
    const dir = distDir();
    writeMain(dir, 'const variant = "v2";');
    packMain(dir, "v2");

    writeMain(dir, 'const variant = "default";');
    assert.include(desktopIdentityMarkerMismatch(dir, "v2"), "does not match");

    NodeFS.rmSync(NodePath.join(dir, "main.cjs"));
    assert.include(desktopIdentityMarkerMismatch(dir, "v2"), "main.cjs is missing");
  });

  it("rejects a marker edited to claim another variant", () => {
    const dir = distDir();
    writeMain(dir, 'const variant = "default";');
    packMain(dir, "default");
    const markerPath = NodePath.join(dir, DESKTOP_IDENTITY_MARKER);
    const marker = JSON.parse(NodeFS.readFileSync(markerPath, "utf8"));
    NodeFS.writeFileSync(markerPath, JSON.stringify({ ...marker, variant: "v2", mainSha256: "0" }));

    assert.include(desktopIdentityMarkerMismatch(dir, "v2"), "does not match");
  });
});
