// @effect-diagnostics nodeBuiltinImport:off - runs a failing server fixture in an isolated directory.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import { expect, it, vi } from "vite-plus/test";

const root = await vi.hoisted(async () => {
  const fs = await import("node:fs/promises");
  const os = await import("node:os");
  const path = await import("node:path");
  return fs.mkdtemp(path.join(os.tmpdir(), "second-server-exit-"));
});

vi.mock("../e2e.config.ts", () => ({
  APP_DIR: root,
  T3_HOME: NodePath.join(root, "home"),
}));

import { startSecondServer } from "./second-server.ts";

it("rejects readiness when the second server exits before serving its environment", async () => {
  await NodeFSP.mkdir(NodePath.join(root, "apps/server/src"), { recursive: true });
  try {
    for (const startup of ["", 'console.log("Token: test-token");\n']) {
      await NodeFSP.writeFile(
        NodePath.join(root, "apps/server/src/bin.ts"),
        startup + "process.exit(1);\n",
      );
      const server = startSecondServer("http://127.0.0.1:5733");
      try {
        await expect(server.ready).rejects.toMatchObject({
          message: "The second server exited with code 1 at startup.",
        });
      } finally {
        await server.stop();
      }
    }
  } finally {
    await NodeFSP.rm(root, { recursive: true, force: true });
  }
});
