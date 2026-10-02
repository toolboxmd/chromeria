/**
 * Creates a starter Promachos home (toolboxmd/chromeria#139) on this server's machine, so it
 * works the same for local and remote environments.
 *
 * Every step keeps what is already there: an existing folder, git repository (including a
 * parent repository or a worktree), `AGENTS.md`, `CLAUDE.md` or project is reused. A failed
 * attempt therefore leaves nothing that blocks a retry.
 */
import {
  type EnvironmentAuthorizationError,
  PROMACHOS_HOME_WS_METHODS,
  type ProjectId,
  PromachosHomeError,
  type PromachosHomeCreateInput,
  type PromachosHomeCreateResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { VcsDriverRegistry } from "../vcs/VcsDriverRegistry.ts";
import { WorkspacePaths } from "../workspace/WorkspacePaths.ts";

export const STARTER_AGENTS_MD = `# Promachos home

This folder is the Promachos home. Every Promachos conversation runs here and reads this file.

## Who you are

<!-- Write the Promachos persona here: his name, how he speaks, and how he works with you. -->
`;

export const STARTER_CLAUDE_MD = "@AGENTS.md\n";

/** How the home finds or registers its project; the WebSocket layer dispatches the command. */
export interface PromachosHomeProjects {
  readonly find: (workspaceRoot: string) => Effect.Effect<ProjectId | null, Error>;
  readonly create: (project: {
    readonly workspaceRoot: string;
    readonly title: string;
  }) => Effect.Effect<ProjectId, Error>;
}

const causeMessage = (cause: unknown) => (cause instanceof Error ? cause.message : String(cause));

const fail = (detail: string) => Effect.fail(new PromachosHomeError({ detail }));

export const createPromachosHome = Effect.fn("PromachosHome.create")(function* (
  input: PromachosHomeCreateInput,
  projects: PromachosHomeProjects,
) {
  const workspacePaths = yield* WorkspacePaths;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const vcs = yield* VcsDriverRegistry;

  const workspaceRoot = yield* workspacePaths
    .normalizeWorkspaceRoot(input.path, { createIfMissing: true })
    .pipe(
      Effect.catch((error) =>
        fail(
          error._tag === "WorkspaceRootCreateFailedError"
            ? `Could not create ${error.normalizedWorkspaceRoot}: ${causeMessage(error.cause)}`
            : error.message,
        ),
      ),
    );

  // Git runs before any file is written, so a missing git leaves only an empty folder.
  yield* Effect.gen(function* () {
    const git = yield* vcs.get("git");
    if (!(yield* git.isInsideWorkTree(workspaceRoot))) {
      yield* git.initRepository({ cwd: workspaceRoot });
    }
  }).pipe(
    Effect.catch((error) =>
      fail(
        error._tag === "VcsProcessSpawnError"
          ? "Git could not run on this machine. Install git and try again."
          : `Could not create a git repository in ${workspaceRoot}: ${error.message}`,
      ),
    ),
  );

  for (const [name, contents] of [
    ["AGENTS.md", STARTER_AGENTS_MD],
    ["CLAUDE.md", STARTER_CLAUDE_MD],
  ] as const) {
    const file = path.join(workspaceRoot, name);
    // The file is written in full inside a private folder, then hard-linked into place. The
    // link never replaces an existing file, and cleanup removes only the private folder, so a
    // failure leaves nothing behind that blocks a retry.
    yield* Effect.scoped(
      Effect.gen(function* () {
        const staging = yield* fileSystem.makeTempDirectoryScoped({
          directory: workspaceRoot,
          prefix: ".promachos-home-",
        });
        const staged = path.join(staging, name);
        yield* fileSystem.writeFileString(staged, contents);
        yield* fileSystem.link(staged, file).pipe(
          Effect.catchIf(
            (error) => error.reason._tag === "AlreadyExists",
            () => Effect.void,
          ),
        );
      }),
    ).pipe(Effect.catch((error) => fail(`Could not write ${file}: ${causeMessage(error)}`)));
  }

  const findOrCreate = projects
    .find(workspaceRoot)
    .pipe(
      Effect.flatMap((existing) =>
        existing === null
          ? projects.create({ workspaceRoot, title: path.basename(workspaceRoot) })
          : Effect.succeed(existing),
      ),
    );
  // Another client may add the same folder first; the project it created is reused.
  const projectId = yield* findOrCreate.pipe(
    Effect.catch((error) =>
      projects
        .find(workspaceRoot)
        .pipe(
          Effect.flatMap((existing) =>
            existing === null ? Effect.fail(error) : Effect.succeed(existing),
          ),
        ),
    ),
    Effect.catch((error) => fail(`Could not add ${workspaceRoot} as a project: ${error.message}`)),
  );
  return { projectId, workspaceRoot } satisfies PromachosHomeCreateResult;
});

type ObserveRpcEffect = <A, E, R>(
  method: string,
  effect: Effect.Effect<A, E, R>,
  traceAttributes?: Readonly<Record<string, unknown>>,
) => Effect.Effect<A, E | EnvironmentAuthorizationError, R>;

/** The Promachos home RPC, spread into the WebSocket handler group behind its scope check. */
export const makePromachosHomeRpcHandlers = Effect.fn("PromachosHome.rpcHandlers")(function* (
  observe: ObserveRpcEffect,
  projects: PromachosHomeProjects,
) {
  const context = yield* Effect.context<
    WorkspacePaths | FileSystem.FileSystem | Path.Path | VcsDriverRegistry
  >();
  return {
    [PROMACHOS_HOME_WS_METHODS.create]: (input: PromachosHomeCreateInput) =>
      observe(
        PROMACHOS_HOME_WS_METHODS.create,
        createPromachosHome(input, projects).pipe(Effect.provideContext(context)),
        { "rpc.aggregate": "promachos" },
      ),
  };
});
