// @effect-diagnostics nodeBuiltinImport:off -- real git is the external boundary under test.
import * as NodeChildProcess from "node:child_process";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { ProjectId, type Project } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as ProjectService from "../project/ProjectService.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as ServerConfig from "../config.ts";
import * as PromachosHome from "./PromachosHome.ts";

const base = Layer.mergeAll(
  NodeServices.layer,
  WorkspacePaths.layer.pipe(
    Layer.provide(NodeServices.layer),
    Layer.provide(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3-promachos-home-" }).pipe(
        Layer.provide(NodeServices.layer),
      ),
    ),
  ),
  VcsDriverRegistry.layer.pipe(Layer.provide(VcsProcess.layer), Layer.provide(NodeServices.layer)),
);
const temporary = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  return yield* fs.makeTempDirectoryScoped({ prefix: "175-promachos-" });
});
function projects(existing?: ProjectId) {
  const created: string[] = [];
  let id = existing;
  const layer = Layer.mock(ProjectService.ProjectService)({
    getByWorkspaceRoot: () => Effect.succeed(id ? Option.some({ id } as Project) : Option.none()),
    create: (input) =>
      Effect.sync(() => {
        created.push(input.workspaceRoot);
        id = input.projectId;
        return { id } as Project;
      }),
  });
  return { created, layer };
}
const create = (path: string, layer: Layer.Layer<ProjectService.ProjectService>) =>
  PromachosHome.PromachosHome.pipe(
    Effect.flatMap((home) => home.create({ path })),
    Effect.provide(PromachosHome.layer.pipe(Layer.provide(layer))),
  );

it.layer(base, { excludeTestServices: true })("Promachos home", (it) => {
  it.effect("creates a git home and registers it through ProjectService once", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = path.join(yield* temporary, "promachos");
      const service = projects();
      const first = yield* create(root, service.layer);
      const second = yield* create(root, service.layer);
      expect(second).toEqual(first);
      expect(service.created).toEqual([root]);
      expect(
        NodeChildProcess.execFileSync("git", ["rev-parse", "--is-inside-work-tree"], {
          cwd: root,
          encoding: "utf8",
        }).trim(),
      ).toBe("true");
      expect(yield* fs.readFileString(path.join(root, "AGENTS.md"))).toContain("## Who you are");
      expect(yield* fs.readFileString(path.join(root, "CLAUDE.md"))).toBe("@AGENTS.md\n");
    }),
  );
  it.effect("keeps an existing persona, repository and registered project", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* temporary;
      NodeChildProcess.execFileSync("git", ["init", "--quiet"], { cwd: root });
      yield* fs.writeFileString(path.join(root, "AGENTS.md"), "# My persona\n");
      yield* fs.writeFileString(path.join(root, "CLAUDE.md"), "my instructions\n");
      const service = projects(ProjectId.make("existing"));
      expect((yield* create(root, service.layer)).projectId).toBe("existing");
      expect(service.created).toEqual([]);
      expect(yield* fs.readFileString(path.join(root, "AGENTS.md"))).toBe("# My persona\n");
      expect(yield* fs.readFileString(path.join(root, "CLAUDE.md"))).toBe("my instructions\n");
    }),
  );
  it.effect("does not nest a git repository inside an existing checkout", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const parent = yield* temporary;
      NodeChildProcess.execFileSync("git", ["init", "--quiet"], { cwd: parent });
      const root = path.join(parent, "home");
      yield* create(root, projects().layer);
      expect(yield* fs.exists(path.join(root, ".git"))).toBe(false);
      expect(yield* fs.exists(path.join(root, "AGENTS.md"))).toBe(true);
    }),
  );
  it.effect("reuses a project that another client registers during creation", () =>
    Effect.gen(function* () {
      const root = yield* temporary;
      let added = false;
      const id = ProjectId.make("concurrent-home");
      const layer = Layer.mock(ProjectService.ProjectService)({
        getByWorkspaceRoot: () =>
          Effect.sync(() => (added ? Option.some({ id } as Project) : Option.none())),
        create: () =>
          Effect.sync(() => {
            added = true;
          }).pipe(
            Effect.andThen(
              Effect.fail(
                new ProjectService.ProjectConflictError({
                  projectId: ProjectId.make("ours"),
                  workspaceRoot: root,
                  conflictingProjectId: id,
                }),
              ),
            ),
          ),
      });
      expect((yield* create(root, layer)).projectId).toBe(id);
    }),
  );
});
