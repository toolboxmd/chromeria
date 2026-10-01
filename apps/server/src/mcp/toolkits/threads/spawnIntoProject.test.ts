// @effect-diagnostics nodeBuiltinImport:off
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";
import {
  ProjectId,
  ThreadId,
  ProviderInstanceId,
  ProviderDriverKind,
  TurnId,
  GitCommandError,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderSendTurnInput,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Option from "effect/Option";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderCommandReactor } from "../../../orchestration/Services/ProviderCommandReactor.ts";
import { ProjectSetupScriptOperationError } from "../../../project/ProjectSetupScriptRunner.ts";
import { isSubagentThreadId, parentThreadIdOf } from "./subagentThreadId.ts";
import {
  assistantReply,
  callTool,
  commandId,
  createParent,
  dispatchAll,
  dispatchUntil,
  NOW,
  PARENT_ID,
  parentActivity,
  parentMessages,
  session,
  temporaryDirectory,
  withServer,
} from "./handlers.testFixtures.ts";

const TARGET = ProjectId.make("target-project");
const createTarget = (workspaceRoot: string) =>
  dispatchAll([
    {
      type: "project.create",
      commandId: commandId(),
      projectId: TARGET,
      title: "Target",
      workspaceRoot,
      createdAt: NOW,
    },
  ]);
const setCallerWorkspace = () =>
  dispatchAll([
    {
      type: "thread.meta.update",
      commandId: commandId(),
      threadId: PARENT_ID,
      branch: "caller-only",
      worktreePath: "/never-use-caller-worktree",
    },
  ]);
const detail = (id: string) =>
  Effect.gen(function* () {
    const query = yield* ProjectionSnapshotQuery;
    return Option.getOrThrow(yield* query.getThreadDetailById(ThreadId.make(id)));
  });

// The engine, projections and provider command reactor are real. Only the
// provider boundary is faked; this proves launch cwd, not a harness's AGENTS loading.
describe("spawn into another project", () => {
  for (const mode of ["child", "top-level"] as const) {
    for (const environment of ["local", "worktree"] as const) {
      it.live(
        `${mode} launches in the target ${environment}, with caller Prism kit and explicit runtime`,
        () =>
          Effect.gen(function* () {
            const directory = yield* temporaryDirectory("t3-spawn-launch-");
            const targetRoot = NodePath.join(directory, "target");
            const targetWorktree = NodePath.join(directory, "target-worktree");
            yield* Effect.promise(() => NodeFSP.mkdir(targetRoot));
            yield* Effect.promise(() => NodeFSP.mkdir(targetWorktree));
            const launched: ProviderSessionStartInput[] = [];
            const sent: ProviderSendTurnInput[] = [];
            const sessions: ProviderSession[] = [];
            const sentReceipt = yield* Deferred.make<void>();
            const worktreeInputs: unknown[] = [];
            yield* withServer(
              NodePath.join(directory, "state.sqlite"),
              Effect.gen(function* () {
                yield* createParent(directory);
                yield* createTarget(targetRoot);
                yield* setCallerWorkspace();
                const reactor = yield* ProviderCommandReactor;
                yield* reactor.start();
                const { result, event: turnStart } = yield* dispatchUntil(
                  callTool("spawn_thread", {
                    task: "Do target work.",
                    projectId: TARGET,
                    mode,
                    runtimeMode: "approval-required",
                    role: "worker",
                    model: "gpt-5",
                    title: "Target probe",
                    reportBack: false,
                  }),
                  (event) =>
                    event.type === "thread.turn-start-requested" && event.aggregateId !== PARENT_ID,
                );
                expect(turnStart.payload).not.toHaveProperty("bootstrap");
                expect(turnStart.payload).not.toHaveProperty("branch");
                expect(turnStart.payload).not.toHaveProperty("worktreePath");
                yield* Deferred.await(sentReceipt);
                yield* reactor.drain;
                const thread = yield* detail(result.threadId);
                expect(thread.projectId).toBe(TARGET);
                expect(thread.branch).toBe(environment === "local" ? null : "target-only");
                expect(thread.worktreePath).toBe(environment === "local" ? null : targetWorktree);
                expect(launched).toHaveLength(1);
                expect(launched[0]).toMatchObject({
                  threadId: result.threadId,
                  cwd: environment === "local" ? targetRoot : targetWorktree,
                  runtimeMode: "approval-required",
                  modelSelection: { instanceId: "codex", model: "gpt-5" },
                });
                expect(sent).toHaveLength(1);
                expect(sent[0]?.input).toContain("CALLER KIT");
                expect(sent[0]?.input).not.toContain("TARGET KIT");
                expect(launched[0]?.cwd).not.toBe("/never-use-caller-worktree");
                expect(launched[0]).not.toHaveProperty("branch");
                expect(launched[0]).not.toHaveProperty("worktreePath");
                expect(result.parentThreadId).toBe(mode === "child" ? PARENT_ID : null);
                expect(isSubagentThreadId(result.threadId)).toBe(mode === "child");
                expect(parentThreadIdOf(result.threadId)).toBe(mode === "child" ? PARENT_ID : null);
                expect(worktreeInputs).toEqual(
                  environment === "local"
                    ? []
                    : [
                        expect.objectContaining({
                          cwd: targetRoot,
                          refName: "HEAD",
                          newRefName: expect.stringMatching(/^t3\/spawn-/),
                        }),
                      ],
                );
              }),
              undefined,
              {
                settings: {
                  defaultThreadEnvMode: environment === "local" ? "worktree" : "local",
                  projectSettingsOverrides: {
                    [TARGET]: {
                      defaultThreadEnvMode: environment,
                      prismRoles: { worker: { instructions: "TARGET KIT" } },
                    },
                    [ProjectId.make("project-threads")]: {
                      prismRoles: {
                        worker: { instructions: "CALLER KIT", skills: ["caller-skill"] },
                      },
                    },
                  },
                },
                git: {
                  isRepository: () => Effect.succeed(true),
                  hasCommit: () => Effect.succeed(true),
                  createWorktree: (input) =>
                    Effect.sync(() => {
                      worktreeInputs.push(input);
                      return { worktree: { path: targetWorktree, refName: "target-only" } };
                    }),
                },
                provider: {
                  listSessions: () => Effect.succeed(sessions),
                  getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
                  startSession: (threadId, input) =>
                    Effect.sync(() => {
                      launched.push(input);
                      const value: ProviderSession = {
                        threadId,
                        provider: ProviderDriverKind.make("codex"),
                        providerInstanceId: ProviderInstanceId.make("codex"),
                        runtimeMode: input.runtimeMode,
                        status: "ready",
                        cwd: input.cwd,
                        model: input.modelSelection?.model,
                        createdAt: NOW,
                        updatedAt: NOW,
                      };
                      sessions.push(value);
                      return value;
                    }),
                  sendTurn: (input) =>
                    Effect.sync(() => sent.push(input)).pipe(
                      Effect.andThen(Deferred.succeed(sentReceipt, undefined)),
                      Effect.as({ threadId: input.threadId, turnId: TurnId.make("launch-turn") }),
                    ),
                },
              },
            );
          }).pipe(Effect.scoped),
      );
    }
  }

  it.effect(
    "cross-project children report after restart and interrupt through children scope",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("t3-cross-child-");
        const database = NodePath.join(directory, "state.sqlite");
        const child = yield* withServer(
          database,
          Effect.gen(function* () {
            yield* createParent(directory);
            yield* createTarget(NodePath.join(directory, "target"));
            return ThreadId.make(
              (yield* dispatchUntil(
                callTool("spawn_thread", {
                  task: "Report target result.",
                  projectId: TARGET,
                }),
                (event) =>
                  event.type === "thread.activity-appended" &&
                  event.payload.activity.kind === "task.started",
              )).result.threadId,
            );
          }),
        );
        yield* withServer(
          database,
          Effect.gen(function* () {
            yield* dispatchUntil(
              dispatchAll([
                session(child, "running", "child-turn"),
                ...assistantReply(child, "target-reply", "Target done."),
                session(child, "ready", null),
              ]),
              parentActivity(child, "task.progress", "idle"),
            );
            expect(
              (yield* parentMessages).some((message) => message.text.includes("Target done.")),
            ).toBe(true);
            expect(
              (yield* callTool("list_child_threads", {})).threads.map((thread) => thread.id),
            ).toContain(child);
            yield* dispatchAll([session(child, "running", "interrupt-turn")]);
            const scope = yield* Scope.Scope;
            const { result: interrupt } = yield* dispatchUntil(
              callTool("interrupt_thread", {
                threadId: child,
                scope: "children",
              }).pipe(Effect.forkIn(scope)),
              (event) =>
                event.type === "thread.turn-interrupt-requested" && event.aggregateId === child,
            );
            yield* dispatchAll([session(child, "interrupted", null)]);
            expect((yield* Fiber.join(interrupt)).status).toBe("interrupted");
          }),
        );
      }).pipe(Effect.scoped),
  );

  it.effect(
    "top-level is visible in target project scope, stays independent, and can be read/messaged after restart",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("t3-cross-top-");
        const database = NodePath.join(directory, "state.sqlite");
        const top = yield* withServer(
          database,
          Effect.gen(function* () {
            yield* createParent(directory);
            yield* createTarget(NodePath.join(directory, "target"));
            return ThreadId.make(
              (yield* callTool("spawn_thread", {
                task: "Independent work.",
                projectId: TARGET,
                mode: "top-level",
              })).threadId,
            );
          }),
        );
        yield* withServer(
          database,
          Effect.gen(function* () {
            yield* dispatchAll([
              ...assistantReply(top, "top-reply", "Independent reply."),
              session(top, "ready", null),
            ]);
            const input = { threadId: top, scope: "project" as const, projectId: TARGET };
            expect((yield* callTool("read_thread", input)).lastAssistantMessage).toBe(
              "Independent reply.",
            );
            expect(
              (yield* callTool("list_threads", {
                scope: "project",
                projectId: TARGET,
                includeSettled: true,
              })).threads.map((t) => t.id),
            ).toContain(top);
            expect(
              (yield* callTool("list_threads", { scope: "project", includeSettled: true })).threads,
            ).toHaveLength(1);
            expect((yield* callTool("list_child_threads", {})).threads).toEqual([]);
            expect(yield* parentMessages).toEqual([]);
            expect(
              (yield* callTool("read_thread", { threadId: top, scope: "project" }).pipe(
                Effect.flip,
              )).message,
            ).toContain("outside scope");
            expect(
              (yield* callTool("message_thread", { ...input, text: "Follow up." })).delivery,
            ).toBe("new-turn");
            expect(
              (yield* detail(top)).messages.some((message) => message.text.includes("Follow up.")),
            ).toBe(true);
            expect((yield* callTool("interrupt_thread", input)).status).toBe("no_active_run");
            expect(
              (yield* callTool("read_thread", { ...input, scope: "children" }).pipe(Effect.flip))
                .message,
            ).toContain("requires scope: project");
          }),
        );
      }).pipe(Effect.scoped),
  );

  for (const reason of ["non-repository", "unborn HEAD"] as const) {
    it.effect(`worktree preference falls back to target checkout for ${reason}`, () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("t3-spawn-fallback-");
        yield* withServer(
          NodePath.join(directory, "state.sqlite"),
          Effect.gen(function* () {
            yield* createParent(directory);
            yield* createTarget(NodePath.join(directory, "target"));
            yield* setCallerWorkspace();
            const result = yield* callTool("spawn_thread", { projectId: TARGET, task: "Work." });
            expect(yield* detail(result.threadId)).toMatchObject({
              projectId: TARGET,
              branch: null,
              worktreePath: null,
            });
          }),
          undefined,
          {
            settings: {
              projectSettingsOverrides: { [TARGET]: { defaultThreadEnvMode: "worktree" } },
            },
            git: {
              isRepository: () => Effect.succeed(reason !== "non-repository"),
              hasCommit: () => Effect.succeed(false),
            },
          },
        );
      }).pipe(Effect.scoped),
    );
  }

  for (const async of [false, true]) {
    it.live(`new target worktree runs setup with async=${async} before starting work`, () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("t3-spawn-setup-");
        const targetRoot = NodePath.join(directory, "target");
        const worktree = NodePath.join(directory, "worktree");
        const setupStarted = yield* Deferred.make<string>();
        const setupFinished = yield* Deferred.make<{ exitCode: number; durationMs: number }>();
        yield* withServer(
          NodePath.join(directory, "state.sqlite"),
          Effect.gen(function* () {
            yield* createParent(directory);
            yield* createTarget(targetRoot);
            const spawn = yield* callTool("spawn_thread", {
              projectId: TARGET,
              mode: "top-level",
              task: "Work after setup.",
            }).pipe(Effect.forkScoped);
            const id = yield* Deferred.await(setupStarted);
            if (async) {
              expect((yield* Fiber.join(spawn)).threadId).toBe(id);
              expect((yield* detail(id)).messages).toHaveLength(1);
            } else {
              expect(spawn.pollUnsafe()).toBeUndefined();
              expect((yield* detail(id)).messages).toEqual([]);
            }
            yield* dispatchUntil(
              Deferred.succeed(setupFinished, { exitCode: 1, durationMs: 10 }),
              (event) =>
                event.type === "thread.activity-appended" &&
                event.aggregateId === id &&
                event.payload.activity.kind === "setup-script.failed",
            );
            expect((yield* Fiber.join(spawn)).threadId).toBe(id);
            const thread = yield* detail(id);
            expect(thread.messages.at(-1)?.text).toBe("Work after setup.");
            expect(thread.activities).toContainEqual(
              expect.objectContaining({ kind: "setup-script.failed", tone: "error" }),
            );
          }),
          undefined,
          {
            settings: {
              projectSettingsOverrides: { [TARGET]: { defaultThreadEnvMode: "worktree" } },
            },
            git: {
              isRepository: () => Effect.succeed(true),
              hasCommit: () => Effect.succeed(true),
              createWorktree: () =>
                Effect.succeed({ worktree: { path: worktree, refName: "target-setup" } }),
            },
            setup: (input) =>
              Effect.gen(function* () {
                expect(input).toMatchObject({
                  projectId: TARGET,
                  projectCwd: targetRoot,
                  worktreePath: worktree,
                  observeCompletion: {},
                });
                yield* Deferred.succeed(setupStarted, input.threadId);
                return {
                  status: "started",
                  scriptId: "setup",
                  scriptName: "Install",
                  scriptCommand: "install",
                  terminalId: "setup-terminal",
                  cwd: worktree,
                  async,
                  completion: Deferred.await(setupFinished),
                };
              }),
          },
        );
      }).pipe(Effect.scoped),
    );
  }

  it.live("setup launch failure is recorded and still starts the target thread", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("t3-setup-failed-");
      yield* withServer(
        NodePath.join(directory, "state.sqlite"),
        Effect.gen(function* () {
          yield* createParent(directory);
          yield* createTarget(NodePath.join(directory, "target"));
          const result = yield* callTool("spawn_thread", {
            projectId: TARGET,
            mode: "top-level",
            task: "Fix setup.",
          });
          const thread = yield* detail(result.threadId);
          expect(thread.messages.some((message) => message.text === "Fix setup.")).toBe(true);
          expect(thread.activities).toContainEqual(
            expect.objectContaining({ kind: "setup-script.failed", tone: "error" }),
          );
        }),
        undefined,
        {
          settings: {
            projectSettingsOverrides: { [TARGET]: { defaultThreadEnvMode: "worktree" } },
          },
          git: {
            isRepository: () => Effect.succeed(true),
            hasCommit: () => Effect.succeed(true),
            createWorktree: () =>
              Effect.succeed({
                worktree: { path: NodePath.join(directory, "new"), refName: "new" },
              }),
          },
          setup: (input) =>
            Effect.fail(
              new ProjectSetupScriptOperationError({
                threadId: input.threadId,
                worktreePath: input.worktreePath,
                operation: "openTerminal",
                cause: "test failure",
              }),
            ),
        },
      );
    }).pipe(Effect.scoped),
  );

  it.live("immediate setup completion is recorded after its requested/started activities", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("t3-setup-order-");
      yield* withServer(
        NodePath.join(directory, "state.sqlite"),
        Effect.gen(function* () {
          yield* createParent(directory);
          yield* createTarget(NodePath.join(directory, "target"));
          const result = yield* callTool("spawn_thread", {
            projectId: TARGET,
            mode: "top-level",
            task: "Ready.",
          });
          const thread = yield* detail(result.threadId);
          expect(
            thread.activities
              .filter((activity) => activity.kind.startsWith("setup-script."))
              .map((activity) => activity.kind),
          ).toEqual(["setup-script.requested", "setup-script.started", "setup-script.completed"]);
        }),
        undefined,
        {
          settings: {
            projectSettingsOverrides: { [TARGET]: { defaultThreadEnvMode: "worktree" } },
          },
          git: {
            isRepository: () => Effect.succeed(true),
            hasCommit: () => Effect.succeed(true),
            createWorktree: () =>
              Effect.succeed({
                worktree: { path: NodePath.join(directory, "new"), refName: "new" },
              }),
          },
          setup: (input) =>
            Effect.succeed({
              status: "started",
              scriptId: "setup",
              scriptName: "Install",
              scriptCommand: "install",
              terminalId: "setup-terminal",
              cwd: input.worktreePath,
              async: false,
              completion: Effect.succeed({ exitCode: 0, durationMs: 0 }),
            }),
        },
      );
    }).pipe(Effect.scoped),
  );

  it.effect("worktree creation failure is a tool error and creates no target thread", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("t3-worktree-failed-");
      yield* withServer(
        NodePath.join(directory, "state.sqlite"),
        Effect.gen(function* () {
          yield* createParent(directory);
          yield* createTarget(NodePath.join(directory, "target"));
          const error = yield* callTool("spawn_thread", { projectId: TARGET, task: "Work." }).pipe(
            Effect.flip,
          );
          expect(error.message).toContain("Could not prepare target project worktree");
          const query = yield* ProjectionSnapshotQuery;
          expect((yield* query.getShellSnapshot()).threads).toHaveLength(1);
        }),
        undefined,
        {
          settings: {
            projectSettingsOverrides: { [TARGET]: { defaultThreadEnvMode: "worktree" } },
          },
          git: {
            isRepository: () => Effect.succeed(true),
            hasCommit: () => Effect.succeed(true),
            createWorktree: (input) =>
              Effect.fail(
                new GitCommandError({
                  operation: "createWorktree",
                  command: "git worktree add",
                  cwd: input.cwd,
                  detail: "disk full",
                }),
              ),
          },
        },
      );
    }).pipe(Effect.scoped),
  );

  it.live("server shutdown cancels an unfinished async setup observer", () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory("t3-setup-scope-");
      const observed = yield* Deferred.make<void>();
      yield* withServer(
        NodePath.join(directory, "state.sqlite"),
        Effect.gen(function* () {
          yield* createParent(directory);
          yield* createTarget(NodePath.join(directory, "target"));
          yield* callTool("spawn_thread", { projectId: TARGET, mode: "top-level", task: "Work." });
        }),
        undefined,
        {
          settings: {
            projectSettingsOverrides: { [TARGET]: { defaultThreadEnvMode: "worktree" } },
          },
          git: {
            isRepository: () => Effect.succeed(true),
            hasCommit: () => Effect.succeed(true),
            createWorktree: () =>
              Effect.succeed({
                worktree: { path: NodePath.join(directory, "new"), refName: "new" },
              }),
          },
          setup: (input) =>
            Effect.succeed({
              status: "started",
              scriptId: "setup",
              scriptName: "Install",
              scriptCommand: "install",
              terminalId: "setup-terminal",
              cwd: input.worktreePath,
              async: true,
              completion: Effect.never.pipe(Effect.ensuring(Deferred.succeed(observed, undefined))),
            }),
        },
      );
      yield* Deferred.await(observed);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "absent project selector retains caller checkout; invalid project and top-level reportback fail before creation",
    () =>
      Effect.gen(function* () {
        const directory = yield* temporaryDirectory("t3-spawn-compat-");
        yield* withServer(
          NodePath.join(directory, "state.sqlite"),
          Effect.gen(function* () {
            yield* createParent(directory);
            yield* setCallerWorkspace();
            const result = yield* callTool("spawn_thread", { task: "Existing child." });
            expect(yield* detail(result.threadId)).toMatchObject({
              branch: "caller-only",
              worktreePath: "/never-use-caller-worktree",
              runtimeMode: "full-access",
            });
            expect(
              (yield* callTool("spawn_thread", {
                projectId: ProjectId.make("missing"),
                task: "Fail.",
              }).pipe(Effect.flip)).message,
            ).toContain("was not found");
            expect(
              (yield* callTool("spawn_thread", {
                task: "Fail.",
                mode: "top-level",
                reportBack: true,
              }).pipe(Effect.flip)).message,
            ).toContain("do not report back");
            const query = yield* ProjectionSnapshotQuery;
            expect((yield* query.getShellSnapshot()).threads).toHaveLength(2);
          }),
        );
      }).pipe(Effect.scoped),
  );
});
