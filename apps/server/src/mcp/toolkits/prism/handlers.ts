import * as NodeOS from "node:os";

import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  type PrismLane,
  ProjectId,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";

import * as EnvironmentAuth from "../../../auth/EnvironmentAuth.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProcessRunner from "../../../processRunner.ts";
import { ServerSettingsService } from "../../../serverSettings.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { threadToolScopeOf } from "../threads/roles.ts";
import { type PrismDispatcher, PrismToolError, PrismToolkit } from "./tools.ts";

/** Where the Model Router plugin installs its versions, newest wins. */
const ROUTER_PLUGIN_DIR = [".codex", "plugins", "cache", "toolboxmd", "model-router"];
const ROUTER_TIMEOUT = "120 seconds";
/** Jobs run for hours; the token must outlive the job, not the tool call. */
const ROUTER_TOKEN_TTL = Duration.days(30);
/** Cancel and recover finish within the call; the token only needs to outlive them. */
const ROUTER_CANCEL_TOKEN_TTL = Duration.hours(1);

const fail = (reason: string) => Effect.fail(new PrismToolError({ reason }));

function compareVersions(left: string, right: string): number {
  const a = left.split(".").map((part) => Number.parseInt(part, 10) || 0);
  const b = right.split(".").map((part) => Number.parseInt(part, 10) || 0);
  for (let index = 0; index < Math.max(a.length, b.length); index += 1) {
    const diff = (a[index] ?? 0) - (b[index] ?? 0);
    if (diff !== 0) return diff;
  }
  return 0;
}

/**
 * The installed router CLI: `PRISM_ROUTER_BIN` when set, else the newest
 * version under the Model Router plugin cache that ships `bin/model-router`.
 */
export const findRouterBin = Effect.fn("Prism.findRouterBin")(function* (
  env: NodeJS.ProcessEnv,
  home: string,
) {
  const explicit = env.PRISM_ROUTER_BIN?.trim();
  if (explicit) return explicit;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = path.join(home, ...ROUTER_PLUGIN_DIR);
  const versions = yield* fs.readDirectory(root).pipe(Effect.orElseSucceed(() => []));
  for (const version of versions.toSorted(compareVersions).toReversed()) {
    const bin = path.join(root, version, "bin", "model-router");
    if (yield* fs.exists(bin).pipe(Effect.orElseSucceed(() => false))) return bin;
  }
  return null;
});

/** The router's private state directory, as the router itself defaults it. */
export function routerStateDir(env: NodeJS.ProcessEnv, home: string): string {
  return env.DURABLE_RUNNER_STATE_DIR?.trim() || `${home}/.local/share/durable-runner`;
}

/**
 * The installed router (0.34) names its lanes default, small and hard;
 * Prism lanes map onto them until the router accepts easy/medium/hard.
 */
const ROUTER_LANES: Record<PrismLane, string> = {
  easy: "small",
  medium: "default",
  hard: "hard",
};

export interface PrismSubmitArgs {
  readonly requestId: string;
  readonly task: string;
  readonly workspace: string;
  readonly plannerThreadId: string;
  readonly serverUrl: string;
  readonly lane?: PrismLane | undefined;
  readonly dispatcher?: PrismDispatcher | undefined;
  readonly handoffSummary?: string | undefined;
}

/** `submit` argv: the calling thread is the planner, on the T3 execution path. */
export function submitArgs(input: PrismSubmitArgs): string[] {
  return [
    "submit",
    "--request-id",
    input.requestId,
    "--task",
    input.task,
    "--workspace",
    input.workspace,
    "--planner-session",
    input.plannerThreadId,
    "--planner-harness",
    "t3",
    "--planner-t3-thread",
    input.plannerThreadId,
    "--t3-server-url",
    input.serverUrl,
    ...(input.lane ? ["--lane", ROUTER_LANES[input.lane]] : []),
    ...(input.dispatcher ? ["--dispatcher", input.dispatcher] : []),
    ...(input.handoffSummary ? ["--handoff-summary", input.handoffSummary] : []),
    "--start",
  ];
}

/** Router output is JSON on stdout, errors included (`{"error": ...}`). */
export function parseRouterOutput(
  stdout: string,
  stderr: string,
  code: number | null,
): { readonly ok: unknown } | { readonly error: string } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(stdout);
  } catch {
    const detail = (stderr || stdout).trim().slice(0, 1_000);
    return { error: `Prism router exited ${code ?? "without a code"}: ${detail || "no output"}` };
  }
  if (code !== 0) {
    const reason =
      parsed !== null && typeof parsed === "object" && "error" in parsed
        ? String((parsed as { error: unknown }).error)
        : stdout.trim().slice(0, 1_000);
    return { error: `Prism router refused: ${reason}` };
  }
  return { ok: parsed };
}

/** The T3 planner thread a router `status` reply names as the job's owner. */
function plannerThreadOf(status: unknown): string | null {
  const job =
    status !== null && typeof status === "object" && "job" in status
      ? (status as { job: unknown }).job
      : null;
  const thread =
    job !== null && typeof job === "object" && "planner_t3_thread" in job
      ? (job as { planner_t3_thread: unknown }).planner_t3_thread
      : null;
  return typeof thread === "string" && thread.length > 0 ? thread : null;
}

/** The job's status from a router `status` reply. */
function jobStatusOf(status: unknown): string | null {
  const job =
    status !== null && typeof status === "object" && "job" in status
      ? (status as { job: unknown }).job
      : null;
  const value =
    job !== null && typeof job === "object" && "status" in job
      ? (job as { status: unknown }).status
      : null;
  return typeof value === "string" ? value : null;
}

/**
 * Router `cancel` then `recover` for a job the caller started, then its final
 * status. `cancel` persists the intent; `recover` stops the job's processes
 * and T3 child threads and finalizes it, or leaves it `cancelling` when
 * something is still alive.
 */
export const cancelOwnedJob = <E, R>(input: {
  readonly requestId: string;
  readonly callerThreadId: string;
  readonly runRouter: (
    args: ReadonlyArray<string>,
    env?: NodeJS.ProcessEnv,
  ) => Effect.Effect<unknown, PrismToolError | E, R>;
  readonly issueToken: Effect.Effect<NodeJS.ProcessEnv, PrismToolError | E, R>;
}) =>
  Effect.gen(function* () {
    const { requestId, runRouter } = input;
    const owner = plannerThreadOf(yield* runRouter(["status", "--request-id", requestId]));
    if (owner !== input.callerThreadId) {
      return yield* fail(`Prism job ${requestId} was not started by this thread.`);
    }
    const env = yield* input.issueToken;
    const cancel = yield* runRouter(["cancel", "--request-id", requestId], env);
    const recover = yield* runRouter(["recover", "--request-id", requestId], env);
    const status = jobStatusOf(yield* runRouter(["status", "--request-id", requestId]));
    return { requestId, router: { status, cancel, recover } };
  });

/** A wildcard bind is reachable on loopback, where the router runs. */
function serverBaseUrl(address: HttpServer.HttpServer["Service"]["address"]): string {
  if (!NetAddress.isInetAddress(address)) return "http://127.0.0.1:3773";
  const host = NetAddress.isUnspecified(address.address)
    ? "127.0.0.1"
    : NetAddress.formatUrlHostString(NetAddress.formatIp(address.address));
  return `http://${host}:${address.port}`;
}

const make = Effect.gen(function* () {
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const serverSettings = yield* ServerSettingsService;
  const auth = yield* EnvironmentAuth.EnvironmentAuth;
  const runner = yield* ProcessRunner.ProcessRunner;
  const crypto = yield* Crypto.Crypto;
  const httpServer = yield* HttpServer.HttpServer;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const serverUrl = serverBaseUrl(httpServer.address);

  /** The calling thread, once its role carries the planner's tools. */
  const plannerCaller = Effect.gen(function* () {
    const invocation = yield* McpInvocationContext.McpInvocationContext;
    const caller = yield* snapshots.getThreadShellById(invocation.threadId).pipe(
      Effect.map(Option.getOrUndefined),
      Effect.catchCause(() => Effect.void),
    );
    if (!caller) return yield* fail(`Thread ${invocation.threadId} was not found.`);
    const kits = yield* serverSettings.getSettings.pipe(
      Effect.map((settings) => resolveProjectSettings(settings, caller.projectId).settings),
      Effect.map((settings) => settings.prismRoles),
      Effect.catchCause(() => fail("Could not read Prism role settings.")),
    );
    if (threadToolScopeOf(caller.id, kits) !== "planner") {
      return yield* fail("Only a planner thread may use the Prism tools.");
    }
    return caller;
  });

  const runRouter = (args: ReadonlyArray<string>, env?: NodeJS.ProcessEnv) =>
    Effect.gen(function* () {
      const home = NodeOS.homedir();
      const bin = yield* findRouterBin(process.env, home).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
      );
      if (!bin) {
        return yield* fail(
          `Prism router is not installed: no bin/model-router under ~/${ROUTER_PLUGIN_DIR.join("/")}.`,
        );
      }
      const output = yield* runner
        .run({
          command: bin,
          args: ["--state-dir", routerStateDir(process.env, home), ...args],
          timeout: ROUTER_TIMEOUT,
          env: { ...process.env, ...env },
        })
        .pipe(Effect.catchCause(() => fail(`Could not run the Prism router at ${bin}.`)));
      const parsed = parseRouterOutput(output.stdout, output.stderr, output.code);
      if ("error" in parsed) return yield* fail(parsed.error);
      return parsed.ok;
    });

  return PrismToolkit.of({
    prism_submit: (input) =>
      Effect.gen(function* () {
        const caller = yield* plannerCaller;
        const project = yield* snapshots.getProjectShellById(ProjectId.make(caller.projectId)).pipe(
          Effect.map(Option.getOrUndefined),
          Effect.catchCause(() => Effect.void),
        );
        const workspace = input.workspace ?? caller.worktreePath ?? project?.workspaceRoot;
        if (!workspace) return yield* fail("Pass workspace: this thread has no checkout.");
        const random = (yield* crypto.randomUUIDv4.pipe(Effect.orDie)).slice(0, 8);
        const nowMs = yield* Clock.currentTimeMillis;
        const requestId = input.requestId ?? `prism-${nowMs.toString(36)}-${random}`;
        // The router talks back to this server (child threads, planner
        // reports) with a token scoped to orchestration, never admin.
        const issued = yield* auth
          .issueSession({
            scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
            label: `Prism router ${requestId}`,
            ttl: ROUTER_TOKEN_TTL,
          })
          .pipe(Effect.catchCause(() => fail("Could not issue a server token for Prism.")));
        const router = yield* runRouter(
          submitArgs({
            requestId,
            task: input.task,
            workspace,
            plannerThreadId: caller.id,
            serverUrl,
            lane: input.lane,
            dispatcher: input.dispatcher,
            handoffSummary: input.handoffSummary,
          }),
          { T3_SERVER_URL: serverUrl, T3_SERVER_TOKEN: issued.token },
        );
        return { requestId, router };
      }),
    prism_status: ({ requestId }) =>
      plannerCaller.pipe(
        Effect.andThen(runRouter(["status", "--request-id", requestId])),
        Effect.map((router) => ({ requestId, router })),
      ),
    prism_questions: ({ requestId, includeAnswered }) =>
      plannerCaller.pipe(
        Effect.andThen(
          runRouter([
            "questions",
            "--request-id",
            requestId,
            ...(includeAnswered ? ["--all"] : []),
          ]),
        ),
        Effect.map((router) => ({ requestId, router })),
      ),
    prism_answer: ({ requestId, qid, answer }) =>
      plannerCaller.pipe(
        Effect.andThen(
          runRouter(["answer", "--request-id", requestId, "--qid", qid, "--answer", answer]),
        ),
        Effect.map((router) => ({ requestId, router })),
      ),
    prism_cancel: ({ requestId }) =>
      Effect.gen(function* () {
        const caller = yield* plannerCaller;
        // Cancelling interrupts the job's T3 child threads, which needs a
        // server token like submit's.
        const issueToken = auth
          .issueSession({
            scopes: [AuthOrchestrationReadScope, AuthOrchestrationOperateScope],
            label: `Prism router cancel ${requestId}`,
            ttl: ROUTER_CANCEL_TOKEN_TTL,
          })
          .pipe(
            Effect.map((issued) => ({ T3_SERVER_URL: serverUrl, T3_SERVER_TOKEN: issued.token })),
            Effect.catchCause(() => fail("Could not issue a server token for Prism.")),
          );
        return yield* cancelOwnedJob({
          requestId,
          callerThreadId: caller.id,
          runRouter,
          issueToken,
        });
      }),
  });
});

export const PrismToolkitHandlersLive = PrismToolkit.toLayer(make);
