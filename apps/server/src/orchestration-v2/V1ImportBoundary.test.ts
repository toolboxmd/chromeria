// @effect-diagnostics nodeBuiltinImport:off - Static architecture test scans source files.
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

import { assert, it } from "@effect/vitest";

const sourceRoot = NodePath.resolve(import.meta.dirname, "..");

function productionTypeScriptFiles(directory: string): ReadonlyArray<string> {
  return NodeFS.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = NodePath.join(directory, entry.name);
    if (entry.isDirectory()) return productionTypeScriptFiles(path);
    return entry.isFile() &&
      entry.name.endsWith(".ts") &&
      !entry.name.includes(".test.") &&
      !entry.name.endsWith(".testFixtures.ts")
      ? [path]
      : [];
  });
}

const relativeSources = productionTypeScriptFiles(sourceRoot).map((path) => ({
  path: NodePath.relative(sourceRoot, path).split(NodePath.sep).join("/"),
  source: NodeFS.readFileSync(path, "utf8"),
}));

it("keeps test fixture helpers out of production imports", () => {
  const fixtureImports = relativeSources
    .filter(({ source }) =>
      /(?:from\s*|import\s*\()["'][^"']*\.testFixtures(?:\.ts)?["']/.test(source),
    )
    .map(({ path }) => path);
  assert.deepEqual(fixtureImports, []);
});
