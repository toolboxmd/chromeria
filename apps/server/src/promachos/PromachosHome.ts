/**
 * Creates a starter Promachos home (toolboxmd/chromeria#139) on this server's machine, so it
 * works the same for local and remote environments.
 *
 * Every step keeps what is already there: an existing folder, git repository (including a
 * parent repository or a worktree), `AGENTS.md`, `CLAUDE.md` or project is reused. A failed
 * attempt therefore leaves nothing that blocks a retry.
 */
import {
  CommandId,
  ProjectId,
  PromachosHomeError,
  type PromachosHomeCreateInput,
  type PromachosHomeCreateResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as ProjectService from "../project/ProjectService.ts";
import { randomUuidV4 } from "../orchestration-v2/RandomUuid.ts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { VcsDriverRegistry } from "../vcs/VcsDriverRegistry.ts";
import { WorkspacePaths } from "../workspace/WorkspacePaths.ts";

const STARTER_AGENTS_MD = `# Promachos home

This folder is the Promachos home. Every Promachos conversation runs here and reads this file.

## Who you are

<!-- Write the Promachos persona here: his name, how he speaks, and how he works with you. -->
`;

const STARTER_CLAUDE_MD = "@AGENTS.md\n";

export class PromachosHome extends Context.Service<
  PromachosHome,
  {
    readonly create: (
      input: PromachosHomeCreateInput,
    ) => Effect.Effect<PromachosHomeCreateResult, PromachosHomeError>;
  }
>()("t3/promachos/PromachosHome") {}

const make = Effect.gen(function* () {
  const projects = yield* ProjectService.ProjectService;
  const workspacePaths = yield* WorkspacePaths;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const vcs = yield* VcsDriverRegistry;
  const create = Effect.fn("PromachosHome.create")(function* (input: PromachosHomeCreateInput) {
    const causeMessage = (cause: unknown) =>
      cause instanceof Error ? cause.message : String(cause);

    const fail = (detail: string) => Effect.fail(new PromachosHomeError({ detail }));

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

    const find = projects.getByWorkspaceRoot(workspaceRoot).pipe(Effect.map(Option.getOrUndefined));
    const findOrCreate = Effect.gen(function* () {
      const existing = yield* find;
      if (existing !== undefined) return existing.id;
      const id = yield* randomUuidV4;
      return (yield* projects.create({
        commandId: CommandId.make(`promachos-home:${id}`),
        projectId: ProjectId.make(id),
        workspaceRoot,
        title: path.basename(workspaceRoot),
      })).id;
    });
    const projectId = yield* findOrCreate.pipe(
      Effect.catch((error) =>
        find.pipe(
          Effect.flatMap((existing) =>
            existing === undefined ? Effect.fail(error) : Effect.succeed(existing.id),
          ),
        ),
      ),
      Effect.catch((error) =>
        fail(`Could not add ${workspaceRoot} as a project: ${error.message}`),
      ),
    );
    return { projectId, workspaceRoot } satisfies PromachosHomeCreateResult;
  });

  return PromachosHome.of({ create });
});
export const layer = Layer.effect(PromachosHome, make);
