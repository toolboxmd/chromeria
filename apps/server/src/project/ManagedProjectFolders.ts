import {
  CommandId,
  OrchestratorMcpFailure,
  type GitManagerServiceError,
  ProjectId,
  type ThreadId,
  VcsProcessSpawnError,
} from "@t3tools/contracts";
import { newProjectFolderName } from "@t3tools/shared/path";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as CommandReceipts from "../orchestration-v2/CommandReceiptStore.ts";
import { requestDigest, type RequestIdentity } from "../mcp/McpRequestReceipts.ts";

import * as ServerConfig from "../config.ts";
import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as ProjectService from "./ProjectService.ts";

export class ScratchUnavailableError extends Schema.TaggedError<ScratchUnavailableError>()(
  "ScratchUnavailableError",
  {},
) {
  override get message(): string {
    return "Threads without a project are not available on this environment.";
  }
}

export class ScratchFolderError extends Schema.TaggedError<ScratchFolderError>()(
  "ScratchFolderError",
  {
    folder: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Failed to create the folder for threads without a project.";
  }
}

export class ScratchProjectError extends Schema.TaggedError<ScratchProjectError>()(
  "ScratchProjectError",
  {
    workspaceRoot: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Failed to create the project for threads without a project.";
  }
}

export type ScratchError = ScratchUnavailableError | ScratchFolderError | ScratchProjectError;

export class NamedProjectFolderError extends Schema.TaggedError<NamedProjectFolderError>()(
  "NamedProjectFolderError",
  {
    folder: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Failed to create the project folder.";
  }
}

export class NamedProjectCreateError extends Schema.TaggedError<NamedProjectCreateError>()(
  "NamedProjectCreateError",
  {
    workspaceRoot: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return "Failed to create the project.";
  }
}

export type NamedProjectError = NamedProjectFolderError | NamedProjectCreateError;

export class ManagedProjectFolders extends Context.Service<
  ManagedProjectFolders,
  {
    readonly scratchRoot: Effect.Effect<Option.Option<string>>;
    readonly ensureScratchProject: Effect.Effect<{ readonly projectId: ProjectId }, ScratchError>;
    readonly folderForThread: (input: {
      readonly projectId: ProjectId;
      readonly threadId: ThreadId;
      readonly text: string;
    }) => Effect.Effect<Option.Option<string>, ScratchFolderError>;
    readonly namedProjectsRoot: string;
    /**
     * The folder is removed when the create fails or is cancelled before the
     * project exists, never once another project owns it.
     */
    readonly createNamedProject: (input: {
      readonly name: string;
      readonly request?: RequestIdentity;
      readonly recheck?: Effect.Effect<void, OrchestratorMcpFailure>;
    }) => Effect.Effect<
      {
        readonly projectId: ProjectId;
        readonly workspaceRoot: string;
        readonly commitError?: string;
      },
      NamedProjectError | OrchestratorMcpFailure
    >;
  }
>()("t3/project/ManagedProjectFolders") {}

const folderWords = (text: string) =>
  text
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(Boolean)
    .slice(0, 5)
    .join("-")
    .slice(0, 48)
    .replace(/-+$/, "");

const ICON_BACKGROUNDS = [
  "#dc2626",
  "#ea580c",
  "#d97706",
  "#16a34a",
  "#059669",
  "#0d9488",
  "#0891b2",
  "#0284c7",
  "#2563eb",
  "#4f46e5",
  "#7c3aed",
  "#9333ea",
  "#c026d3",
  "#db2777",
  "#e11d48",
] as const;

const MAX_NAMED_FOLDER_ATTEMPTS = 100;

function escapeXml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

/**
 * It lives at `assets/icon.svg`, a path ProjectFaviconResolver already checks,
 * so every machine that clones the project shows the same icon.
 */
function namedProjectIconSvg(name: string): string {
  const words = name.split(/[^\p{L}\p{N}]+/u).filter(Boolean);
  const initials =
    words
      .slice(0, 2)
      .map((word) => Array.from(word)[0] ?? "")
      .join("")
      .toUpperCase() ||
    Array.from(name.trim())[0] ||
    "?";
  let hash = 0;
  for (const char of name) hash = (hash * 31 + (char.codePointAt(0) ?? 0)) >>> 0;
  const background = ICON_BACKGROUNDS[hash % ICON_BACKGROUNDS.length];
  return [
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">`,
    `  <rect width="64" height="64" rx="14" fill="${background}"/>`,
    `  <text x="32" y="32" dy="0.35em" text-anchor="middle" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="${initials.length > 1 ? 26 : 32}" font-weight="600" fill="#ffffff">${escapeXml(initials)}</text>`,
    `</svg>`,
    "",
  ].join("\n");
}

function namedProjectReadme(name: string): string {
  return [
    `<img src="assets/icon.svg" width="64" height="64" alt="">`,
    "",
    `# ${name}`,
    "",
    "Created in [T3 Code](https://t3.codes).",
    "",
  ].join("\n");
}

function describeCommitFailure(stderr: string): string {
  if (/identity unknown|tell me who you are|no (name|email) was given/i.test(stderr)) {
    return "Git has no name or email on this machine. Set user.name and user.email, then commit.";
  }
  const lines = stderr
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  return lines.at(-1) ?? "Git could not make the first commit.";
}

const isVcsProcessSpawnError = Schema.is(VcsProcessSpawnError);

// Spawning a `git` that is not on PATH fails with NotFound, on Windows too.
const isGitMissing = (error: GitManagerServiceError) =>
  error._tag === "GitManagerError" &&
  isVcsProcessSpawnError(error.cause) &&
  PlatformError.isPlatformError(error.cause.cause) &&
  error.cause.cause.reason._tag === "NotFound";

const make = Effect.gen(function* () {
  const config = yield* ServerConfig.ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const gitWorkflow = yield* GitWorkflow.GitWorkflowService;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const projects = yield* ProjectService.ProjectService;
  const crypto = yield* Crypto.Crypto;

  /**
   * Each folder is created without `recursive`, so creating it is the claim: of
   * two racers for one name, one gets AlreadyExists and moves on to the next.
   */
  const claimFreeFolder = Effect.fnUntraced(function* <E>(
    folderFor: (attempt: number) => Effect.Effect<Option.Option<string>, E>,
    onError: (folder: string, cause: PlatformError.PlatformError) => E,
  ) {
    for (let attempt = 1; ; attempt++) {
      const folder = yield* folderFor(attempt);
      if (Option.isNone(folder)) return folder;
      const claimed = yield* fileSystem.makeDirectory(folder.value).pipe(
        Effect.as(true),
        Effect.catchIf(
          (error) => error.reason._tag === "AlreadyExists",
          () => Effect.succeed(false),
        ),
        Effect.mapError((cause) => onError(folder.value, cause)),
      );
      if (claimed) return folder;
    }
  });

  // Inside a checkout (a dev worktree's .t3, a dotfiles home) the folder would
  // inherit the repo's git status and checkpoints, so Scratch is offered only
  // when the data dir is outside any work tree.
  const [probe, invalidate] = yield* Effect.cachedInvalidateWithTTL(
    gitWorkflow.isRepository(config.baseDir).pipe(
      Effect.catchIf(isGitMissing, () => Effect.succeed(false)),
      Effect.map((isRepository) =>
        isRepository ? Option.none<string>() : Option.some(path.resolve(config.baseDir, "scratch")),
      ),
      Effect.catchCause((cause) =>
        Cause.hasInterrupts(cause) ? Effect.interrupt : Effect.succeed(Option.none<string>()),
      ),
    ),
    Duration.infinity,
  );
  const scratchRoot = probe.pipe(Effect.onInterrupt(() => invalidate));

  const makeScratchFolder = (folder: string) =>
    fileSystem
      .makeDirectory(folder, { recursive: true })
      .pipe(Effect.mapError((cause) => new ScratchFolderError({ folder, cause })));

  const ensureScratchProject: ManagedProjectFolders["Service"]["ensureScratchProject"] = Effect.gen(
    function* () {
      const workspaceRoot = yield* scratchRoot.pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.fail(new ScratchUnavailableError()),
            onSome: Effect.succeed,
          }),
        ),
      );
      yield* makeScratchFolder(workspaceRoot);
      const id = yield* crypto
        .randomUUIDv4()
        .pipe(Effect.mapError((cause) => new ScratchProjectError({ workspaceRoot, cause })));
      const bootstrapped = yield* projects
        .bootstrap({
          commandId: CommandId.make(`scratch-project:${id}`),
          projectId: ProjectId.make(id),
          title: "No project",
          workspaceRoot,
        })
        .pipe(
          // bootstrap looks the root up before taking the workspace lock, so a
          // racing create loses with a conflict that names the winner.
          Effect.catchTags({
            ProjectConflictError: (conflict) =>
              Effect.succeed({
                project: { id: conflict.conflictingProjectId },
                created: false,
              }),
          }),
          Effect.mapError((cause) => new ScratchProjectError({ workspaceRoot, cause })),
        );
      if (bootstrapped.created) {
        // Set once at create, so a user's own icon choice is never overwritten.
        yield* projects
          .update({
            commandId: CommandId.make(`scratch-project-icon:${id}`),
            projectId: bootstrapped.project.id,
            projectIcon: { kind: "lucide", name: "message-square-dashed", color: "gray" },
          })
          .pipe(Effect.mapError((cause) => new ScratchProjectError({ workspaceRoot, cause })));
      }
      return { projectId: bootstrapped.project.id };
    },
  );

  const isScratchProject = (projectId: ProjectId, root: string) =>
    projects.getById(projectId).pipe(
      Effect.map(
        Option.exists((project) => path.resolve(project.workspaceRoot) === path.resolve(root)),
      ),
      Effect.orElseSucceed(() => false),
    );

  const folderForThread: ManagedProjectFolders["Service"]["folderForThread"] = Effect.fn(
    "ManagedProjectFolders.folderForThread",
  )(function* (input) {
    const root = yield* scratchRoot;
    if (Option.isNone(root)) return Option.none();
    if (!(yield* isScratchProject(input.projectId, root.value))) return Option.none();
    const date = DateTime.formatIso(yield* DateTime.now).slice(0, 10);
    const words = folderWords(input.text);
    const id = input.threadId.toLowerCase().replace(/[^a-z0-9]/g, "");
    const folderFor = (idPart: string) =>
      path.join(root.value, [date, words, idPart].filter(Boolean).join("-"));
    yield* makeScratchFolder(root.value);
    const fullFolder = folderFor(id);
    return yield* claimFreeFolder(
      (attempt) =>
        attempt === 1
          ? Effect.succeed(Option.some(folderFor(id.slice(-8))))
          : attempt === 2
            ? Effect.succeed(Option.some(fullFolder))
            : crypto.randomUUIDv4().pipe(
                Effect.map((uuid) => Option.some(`${fullFolder}-${uuid.slice(0, 8)}`)),
                Effect.mapError((cause) => new ScratchFolderError({ folder: fullFolder, cause })),
              ),
      (folder, cause) => new ScratchFolderError({ folder, cause }),
    );
  });

  const namedProjectsRoot = path.resolve(config.baseDir, "projects");

  const claimNamedFolder = Effect.fn("ManagedProjectFolders.claimNamedFolder")(function* (
    name: string,
  ) {
    yield* fileSystem
      .makeDirectory(namedProjectsRoot, { recursive: true })
      .pipe(
        Effect.mapError(
          (cause) => new NamedProjectFolderError({ folder: namedProjectsRoot, cause }),
        ),
      );
    const folderName = newProjectFolderName(name);
    const claimed = yield* claimFreeFolder(
      (attempt) =>
        Effect.succeed(
          attempt > MAX_NAMED_FOLDER_ATTEMPTS
            ? Option.none()
            : Option.some(
                path.join(
                  namedProjectsRoot,
                  attempt === 1 ? folderName : `${folderName}-${attempt}`,
                ),
              ),
        ),
      (folder, cause) => new NamedProjectFolderError({ folder, cause }),
    );
    if (Option.isSome(claimed)) return claimed.value;
    return yield* new NamedProjectFolderError({
      folder: path.join(namedProjectsRoot, folderName),
      cause: `Every folder name for "${folderName}" is taken.`,
    });
  });

  const scaffoldRepository = Effect.fn("ManagedProjectFolders.scaffoldRepository")(function* (
    cwd: string,
    name: string,
  ) {
    const branch =
      (yield* git
        .readConfigValue(cwd, "init.defaultBranch")
        .pipe(Effect.orElseSucceed(() => null))) ?? "main";
    yield* git.execute({
      operation: "ManagedProjectFolders.init",
      cwd,
      args: ["init", `--initial-branch=${branch}`],
      timeoutMs: 10_000,
    });
    yield* fileSystem.writeFileString(path.join(cwd, "README.md"), namedProjectReadme(name));
    yield* fileSystem.makeDirectory(path.join(cwd, "assets"));
    yield* fileSystem.writeFileString(
      path.join(cwd, "assets", "icon.svg"),
      namedProjectIconSvg(name),
    );
    // Named and forced so a global ignore rule (say `*.svg`) cannot drop one.
    yield* git.execute({
      operation: "ManagedProjectFolders.add",
      cwd,
      args: ["add", "--force", "--", "README.md", "assets/icon.svg"],
      timeoutMs: 10_000,
    });
    return yield* git
      .execute({
        operation: "ManagedProjectFolders.commit",
        cwd,
        args: ["commit", "--message", "Initial commit"],
        allowNonZeroExit: true,
        timeoutMs: 30_000,
      })
      .pipe(
        Effect.map((result) =>
          result.exitCode === 0 ? undefined : describeCommitFailure(result.stderr),
        ),
        Effect.catch((error) => Effect.succeed(error.message)),
      );
  });

  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  const receipts = yield* Effect.serviceOption(CommandReceipts.CommandReceiptStoreV2);
  if (Option.isSome(sql))
    yield* sql.value`CREATE TABLE IF NOT EXISTS managed_project_requests (
    request_id TEXT PRIMARY KEY, generation TEXT NOT NULL, input_digest TEXT NOT NULL, name_digest TEXT NOT NULL,
    project_id TEXT NOT NULL, command_id TEXT NOT NULL, workspace_root TEXT NOT NULL,
    stage TEXT NOT NULL, commit_error TEXT)`.pipe(Effect.orDie);
  const keyedNamedProject = (input: {
    readonly name: string;
    readonly request: RequestIdentity;
    readonly recheck?: Effect.Effect<void, OrchestratorMcpFailure>;
  }) =>
    Effect.gen(function* () {
      const request = input.request;
      const unknown = () =>
        new OrchestratorMcpFailure({
          code: "outcome_unknown",
          requestId: request.clientRequestId,
          lastConfirmedStage: "dispatch_started",
          message:
            "The named project filesystem work may have happened. Inspect its selected folder before choosing another request key.",
        });
      if (Option.isNone(sql) || Option.isNone(receipts))
        return yield* new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "The native project request owner is unavailable; no folder was allocated.",
        });
      const db = sql.value;
      const projectId = ProjectId.make(`project:mcp:${request.id}`);
      const commandId = CommandId.make(`named-project:${projectId}`);
      const slug = newProjectFolderName(input.name);
      yield* db`INSERT INTO managed_project_requests (request_id,generation,input_digest,name_digest,project_id,command_id,workspace_root,stage)
      VALUES (${request.id},${request.generation},${request.inputDigest},${requestDigest(input.name)},${projectId},${commandId},${path.join(namedProjectsRoot, slug)},'selected') ON CONFLICT DO NOTHING`;
      type NamedRequest = {
        generation: string;
        input_digest: string;
        name_digest: string;
        project_id: string;
        command_id: string;
        workspace_root: string;
        stage: string;
        commit_error: string | null;
      };
      const read = Effect.gen(function* () {
        const row =
          (yield* db<NamedRequest>`SELECT * FROM managed_project_requests WHERE request_id=${request.id}`)[0];
        if (row !== undefined && row.generation !== request.generation)
          return yield* new OrchestratorMcpFailure({
            code: "capability_denied",
            message: "This named-project request belongs to its original grant generation.",
          });
        if (
          !row ||
          row.input_digest !== request.inputDigest ||
          row.name_digest !== requestDigest(input.name) ||
          row.project_id !== projectId ||
          row.command_id !== commandId ||
          path.dirname(row.workspace_root) !== namedProjectsRoot
        )
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message:
              "The named-project request belongs to different input, resource or grant generation.",
          });
        return row;
      });
      let row = yield* read;
      const original = yield* receipts.value
        .getProjectByCommandId(commandId)
        .pipe(Effect.mapError(() => unknown()));
      if (Option.isSome(original)) {
        if (
          original.value.status !== "accepted" ||
          original.value.commandType !== "project.create" ||
          original.value.projectId !== projectId
        )
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: "The original native project command was not accepted for this project.",
          });
        yield* db`UPDATE managed_project_requests SET stage='confirmed' WHERE request_id=${request.id}`;
        return {
          projectId,
          workspaceRoot: row.workspace_root,
          ...(row.commit_error === null ? {} : { commitError: row.commit_error }),
        };
      }
      if (row.stage === "selected") {
        yield* input.recheck ?? Effect.void;
        // Acceptance precedes mkdir. A crash from here cannot allocate another
        // folder on retry; only exact native command acceptance can reconcile it.
        const acquired = yield* db`UPDATE managed_project_requests SET stage='scaffolding'
        WHERE request_id=${request.id} AND stage='selected' RETURNING request_id`;
        if (acquired.length !== 1) return yield* unknown();
        yield* fileSystem
          .makeDirectory(namedProjectsRoot, { recursive: true })
          .pipe(Effect.mapError(() => unknown()));
        let claimed = false;
        for (let attempt = 1; attempt <= MAX_NAMED_FOLDER_ATTEMPTS; attempt++) {
          const workspaceRoot = path.join(
            namedProjectsRoot,
            attempt === 1 ? slug : `${slug}-${attempt}`,
          );
          // Bind the exact candidate before the filesystem claim. Known
          // AlreadyExists is a refusal, so the existing owner alone selects next.
          yield* db`UPDATE managed_project_requests SET workspace_root=${workspaceRoot} WHERE request_id=${request.id}`;
          claimed = yield* fileSystem.makeDirectory(workspaceRoot).pipe(
            Effect.as(true),
            Effect.catchIf(
              (e) => e.reason._tag === "AlreadyExists",
              () => Effect.succeed(false),
            ),
            Effect.mapError(() => unknown()),
          );
          if (!claimed) continue;
          const commitError = yield* scaffoldRepository(workspaceRoot, input.name).pipe(
            Effect.catchCause(() => Effect.fail(unknown())),
          );
          yield* db`UPDATE managed_project_requests SET stage='prepared',commit_error=${commitError ?? null} WHERE request_id=${request.id}`;
          break;
        }
        if (!claimed) return yield* unknown();
        row = yield* read;
      }
      if (row.stage !== "prepared" && row.stage !== "create_started") return yield* unknown();
      // Only the native create may resume. Never rerun scaffolding or remove a
      // possibly accepted folder after an interrupted or lost create response.
      yield* input.recheck ?? Effect.void;
      yield* db`UPDATE managed_project_requests SET stage='create_started' WHERE request_id=${request.id}`;
      yield* projects
        .create({ commandId, projectId, title: input.name, workspaceRoot: row.workspace_root })
        .pipe(
          Effect.mapError(
            (cause) => new NamedProjectCreateError({ workspaceRoot: row.workspace_root, cause }),
          ),
        );
      yield* db`UPDATE managed_project_requests SET stage='confirmed' WHERE request_id=${request.id}`;
      return {
        projectId,
        workspaceRoot: row.workspace_root,
        ...(row.commit_error === null ? {} : { commitError: row.commit_error }),
      };
    }).pipe(
      Effect.mapError((error) =>
        error._tag === "SqlError"
          ? new OrchestratorMcpFailure({
              code: "orchestration_error",
              message:
                "The durable named-project binding could not be confirmed; inspect its original request before proceeding.",
            })
          : error,
      ),
    );

  const createNamedProject: ManagedProjectFolders["Service"]["createNamedProject"] = Effect.fn(
    "ManagedProjectFolders.createNamedProject",
  )(function* (input) {
    if (input.request !== undefined)
      return yield* keyedNamedProject({ ...input, request: input.request });
    const workspaceRoot = yield* claimNamedFolder(input.name);
    const removeFolder = fileSystem.remove(workspaceRoot, { recursive: true }).pipe(Effect.ignore);
    const prepared = yield* Effect.all([
      scaffoldRepository(workspaceRoot, input.name).pipe(
        Effect.mapError((cause) => new NamedProjectFolderError({ folder: workspaceRoot, cause })),
      ),
      crypto
        .randomUUIDv4()
        .pipe(Effect.mapError((cause) => new NamedProjectCreateError({ workspaceRoot, cause }))),
    ]).pipe(Effect.onExit((exit) => (Exit.isSuccess(exit) ? Effect.void : removeFolder)));
    const [commitError, id] = prepared;
    const project = yield* projects
      .create({
        commandId: CommandId.make(`named-project:${id}`),
        projectId: ProjectId.make(id),
        title: input.name,
        workspaceRoot,
      })
      .pipe(
        // A rejected create leaves the folder unused, so remove it. An
        // interrupt can land after the create is committed, and a conflict
        // means another project owns the folder, so it stays then; the owner
        // is checked again first, since deleting another project's files is
        // never acceptable.
        Effect.tapError((error) =>
          error._tag === "ProjectConflictError"
            ? Effect.void
            : projects.getByWorkspaceRoot(workspaceRoot).pipe(
                Effect.flatMap((owner) => (Option.isNone(owner) ? removeFolder : Effect.void)),
                Effect.ignore,
              ),
        ),
        Effect.mapError((cause) => new NamedProjectCreateError({ workspaceRoot, cause })),
      );
    return {
      projectId: project.id,
      workspaceRoot,
      ...(commitError === undefined ? {} : { commitError }),
    };
  });

  return ManagedProjectFolders.of({
    scratchRoot,
    ensureScratchProject,
    folderForThread,
    namedProjectsRoot,
    createNamedProject,
  });
});

export const layer = Layer.effect(ManagedProjectFolders, make);
