// @effect-diagnostics nodeBuiltinImport:off - the tests read git state with the real git binary.
import * as NodeChildProcess from "node:child_process";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  OrchestrationDispatchCommandError,
  ProjectId,
  VcsProcessSpawnError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";

import * as ServerConfig from "../config.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import {
  createPromachosHome,
  type PromachosHomeProjects,
  STARTER_AGENTS_MD,
} from "./PromachosHome.ts";

/** Real git, recording each `git init` and able to act as if git were not installed. */
function makeGit() {
  const state = { missing: false, inits: [] as string[] };
  const layer = Layer.effect(
    VcsDriverRegistry.VcsDriverRegistry,
    Effect.gen(function* () {
      const real = yield* VcsDriverRegistry.VcsDriverRegistry;
      return VcsDriverRegistry.VcsDriverRegistry.of({
        ...real,
        get: (kind) =>
          real.get(kind).pipe(
            Effect.map((driver) => ({
              ...driver,
              isInsideWorkTree: (cwd: string) =>
                state.missing
                  ? Effect.fail(
                      new VcsProcessSpawnError({
                        operation: "GitVcsDriver.isInsideWorkTree",
                        command: "git",
                        cwd,
                        cause: new Error("spawn git ENOENT"),
                      }),
                    )
                  : driver.isInsideWorkTree(cwd),
              initRepository: (input: Parameters<typeof driver.initRepository>[0]) => {
                state.inits.push(input.cwd);
                return driver.initRepository(input);
              },
            })),
          ),
      });
    }),
  ).pipe(Layer.provide(VcsDriverRegistry.layer.pipe(Layer.provide(VcsProcess.layer))));
  return { state, layer };
}

/** An in-memory project list standing in for the orchestration read model. */
function makeProjects(initial: ReadonlyArray<readonly [string, ProjectId]> = []) {
  const byRoot = new Map(initial);
  const created: string[] = [];
  const projects: PromachosHomeProjects = {
    find: (workspaceRoot) => Effect.succeed(byRoot.get(workspaceRoot) ?? null),
    create: ({ workspaceRoot, title }) =>
      Effect.sync(() => {
        const projectId = ProjectId.make(`project-${title}-${created.length + 1}`);
        created.push(workspaceRoot);
        byRoot.set(workspaceRoot, projectId);
        return projectId;
      }),
  };
  return { projects, created };
}

const BaseLayer = Layer.empty.pipe(
  Layer.provideMerge(WorkspacePaths.layer),
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-promachos-home-test-" })),
  Layer.provideMerge(NodeServices.layer),
);

const tempDir = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  return yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-promachos-home-" });
});

const git = (cwd: string, args: ReadonlyArray<string>) =>
  NodeChildProcess.execFileSync("git", args, { cwd, encoding: "utf8" }).trim();

it.layer(BaseLayer, { excludeTestServices: true })("createPromachosHome", (it) => {
  describe("a new folder", () => {
    it.effect("creates the folder, a git repository, the starter files and one project", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = path.join(yield* tempDir, "nested", "promachos");
        const vcs = makeGit();
        const { projects, created } = makeProjects();

        const result = yield* createPromachosHome({ path: home }, projects).pipe(
          Effect.provide(vcs.layer),
        );

        expect(result).toEqual({ projectId: "project-promachos-1", workspaceRoot: home });
        expect(created).toEqual([home]);
        expect(git(home, ["rev-parse", "--show-toplevel"])).toBe(
          NodeChildProcess.execFileSync("realpath", [home], { encoding: "utf8" }).trim(),
        );
        const agents = yield* fileSystem.readFileString(path.join(home, "AGENTS.md"));
        expect(agents).toContain("## Who you are");
        expect(yield* fileSystem.readFileString(path.join(home, "CLAUDE.md"))).toBe("@AGENTS.md\n");
      }),
    );
  });

  describe("never overwrites", () => {
    it.effect("keeps existing files, repository and project", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = yield* tempDir;
        git(home, ["init", "--quiet"]);
        yield* fileSystem.writeFileString(path.join(home, "AGENTS.md"), "# Mine\n");
        yield* fileSystem.writeFileString(path.join(home, "CLAUDE.md"), "my claude\n");
        const vcs = makeGit();
        const existing = ProjectId.make("existing-project");
        const { projects, created } = makeProjects([[home, existing]]);

        const result = yield* createPromachosHome({ path: home }, projects).pipe(
          Effect.provide(vcs.layer),
        );

        expect(result.projectId).toBe(existing);
        expect(created).toEqual([]);
        expect(vcs.state.inits).toEqual([]);
        expect(yield* fileSystem.readFileString(path.join(home, "AGENTS.md"))).toBe("# Mine\n");
        expect(yield* fileSystem.readFileString(path.join(home, "CLAUDE.md"))).toBe("my claude\n");
      }),
    );

    it.effect("does not start a repository inside an existing one", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const parent = yield* tempDir;
        git(parent, ["init", "--quiet"]);
        const home = path.join(parent, "promachos");
        const vcs = makeGit();

        yield* createPromachosHome({ path: home }, makeProjects().projects).pipe(
          Effect.provide(vcs.layer),
        );

        expect(vcs.state.inits).toEqual([]);
        expect(yield* fileSystem.exists(path.join(home, ".git"))).toBe(false);
        expect(yield* fileSystem.readFileString(path.join(home, "AGENTS.md"))).toBe(
          STARTER_AGENTS_MD,
        );
      }),
    );
  });

  describe("failures leave a retry possible", () => {
    it.effect("reports missing git before writing anything, then succeeds once git runs", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = path.join(yield* tempDir, "promachos");
        const vcs = makeGit();
        vcs.state.missing = true;
        const { projects, created } = makeProjects();

        const failure = yield* createPromachosHome({ path: home }, projects).pipe(
          Effect.provide(vcs.layer),
          Effect.flip,
        );

        expect(failure.message).toBe(
          "Git could not run on this machine. Install git and try again.",
        );
        expect(yield* fileSystem.readDirectory(home)).toEqual([]);
        expect(created).toEqual([]);

        vcs.state.missing = false;
        yield* createPromachosHome({ path: home }, projects).pipe(Effect.provide(vcs.layer));

        expect(vcs.state.inits).toEqual([home]);
        expect((yield* fileSystem.readDirectory(home)).toSorted()).toEqual([
          ".git",
          "AGENTS.md",
          "CLAUDE.md",
        ]);
        expect(created).toEqual([home]);
      }),
    );

    it.effect.skipIf(process.getuid?.() === 0)(
      "reports a folder that cannot be created, then succeeds once it can",
      () =>
        Effect.gen(function* () {
          const fileSystem = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const locked = yield* tempDir;
          const home = path.join(locked, "promachos");
          yield* fileSystem.chmod(locked, 0o500);
          const vcs = makeGit();
          const { projects, created } = makeProjects();

          const failure = yield* createPromachosHome({ path: home }, projects).pipe(
            Effect.provide(vcs.layer),
            Effect.flip,
          );
          yield* fileSystem.chmod(locked, 0o700);

          expect(failure.message).toContain(`Could not create ${home}`);
          expect(created).toEqual([]);

          const result = yield* createPromachosHome({ path: home }, projects).pipe(
            Effect.provide(vcs.layer),
          );
          expect(result.workspaceRoot).toBe(home);
          expect(yield* fileSystem.exists(path.join(home, "AGENTS.md"))).toBe(true);
        }),
    );

    it.effect("keeps a file another process puts in place and leaves no staging behind", () =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const home = path.join(yield* tempDir, "promachos");
        let failInstall = true;
        // The first install fails right after another process writes its own AGENTS.md.
        const racing = FileSystem.FileSystem.of({
          ...fileSystem,
          link: (from, to) =>
            failInstall && to.endsWith("AGENTS.md")
              ? fileSystem.writeFileString(to, "# Theirs\n").pipe(
                  Effect.andThen(
                    Effect.fail(
                      PlatformError.systemError({
                        _tag: "PermissionDenied",
                        module: "FileSystem",
                        method: "link",
                        pathOrDescriptor: to,
                      }),
                    ),
                  ),
                )
              : fileSystem.link(from, to),
        });
        const vcs = makeGit();
        const { projects, created } = makeProjects();

        const failure = yield* createPromachosHome({ path: home }, projects).pipe(
          Effect.provide(vcs.layer),
          Effect.provideService(FileSystem.FileSystem, racing),
          Effect.flip,
        );

        expect(failure.message).toContain(`Could not write ${path.join(home, "AGENTS.md")}`);
        expect(yield* fileSystem.readFileString(path.join(home, "AGENTS.md"))).toBe("# Theirs\n");
        expect((yield* fileSystem.readDirectory(home)).toSorted()).toEqual([".git", "AGENTS.md"]);
        expect(created).toEqual([]);

        failInstall = false;
        yield* createPromachosHome({ path: home }, projects).pipe(
          Effect.provide(vcs.layer),
          Effect.provideService(FileSystem.FileSystem, racing),
        );

        expect(yield* fileSystem.readFileString(path.join(home, "AGENTS.md"))).toBe("# Theirs\n");
        expect((yield* fileSystem.readDirectory(home)).toSorted()).toEqual([
          ".git",
          "AGENTS.md",
          "CLAUDE.md",
        ]);
        expect(created).toEqual([home]);
      }),
    );

    it.effect("reuses the project another client added first", () =>
      Effect.gen(function* () {
        const home = yield* tempDir;
        const other = ProjectId.make("added-by-another-client");
        let added = false;
        const projects: PromachosHomeProjects = {
          find: () => Effect.succeed(added ? other : null),
          create: () =>
            Effect.sync(() => {
              added = true;
            }).pipe(
              Effect.andThen(
                Effect.fail(
                  new OrchestrationDispatchCommandError({
                    message: "Active project already exists",
                  }),
                ),
              ),
            ),
        };

        const result = yield* createPromachosHome({ path: home }, projects).pipe(
          Effect.provide(makeGit().layer),
        );

        expect(result.projectId).toBe(other);
      }),
    );
  });
});
