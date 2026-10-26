import * as PeerLinkEvents from "./PeerLinkEvents.ts";
import * as PubSub from "effect/PubSub";
import * as SpectrumMcp from "../spectrum/mcpToolkit.ts";
import * as SpectrumStore from "../spectrum/store.ts";
import * as SpectrumReports from "../spectrum/ReportService.ts";
import * as SpectrumCommands from "../spectrum/commandPlan.ts";
import { ForkCommandInterceptor } from "../fork/ForkCommandInterceptor.ts";
import { ForkDispatchPlans } from "../fork/ForkDispatchPlans.ts";
import { spectrumLaunchAdmission } from "../spectrum/launchAdmission.ts";
import * as SpectrumRounds from "../spectrum/RoundService.ts";
import * as AutomationAuthority from "../scheduledTasks/DestinationAutomationAuthority.ts";
import * as SpectrumController from "../spectrum/Controller.ts";
import * as SpectrumTranscript from "../spectrum/TranscriptService.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as FileSystem from "effect/FileSystem";
import { PeerDispatchAuthority } from "./peerEffectAuthority.ts";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { CommandId, MessageId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import type * as Adapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as PeerAuthority from "./PeerAuthority.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";
import { descriptorOf, linkTo } from "./PeerLinks.testkit.ts";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import {
  activate,
  automationAdapter,
  automationReply,
  decodeAutomationAck,
  decodeAutomationScheduleReceipt,
  decodeAutomationSpectrumResult,
  delegate,
  drainAutomationRun,
  finishAutomationRun,
  json,
  makeNode,
  nativeCall,
  seedParent,
  selection,
} from "./PeerAuthority.workflow.fixture.ts";

it.live(
  "automation384: public Spectrum reopen captures fresh authority and revoked birth contributors leave its generation unchanged",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf("automation384-spectrum-source", "Spectrum source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf("automation384-spectrum-destination", "Spectrum destination", {
            peerAuthority: 1,
          }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make("automation384-spectrum-parent");
        yield* seedParent(a, parent);
        const discovered = yield* nativeCall(a, parent, "t3_project_list", {
          environmentId: b.environmentId,
        });
        expect(discovered.isError, json(discovered)).not.toBe(true);
        const incoming = (yield* b.linkedSessions)[0]!;
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
        const { childId } = yield* delegate(a, b, parent, "automation384-spectrum-child");
        yield* activate(b, childId);
        const started = yield* nativeCall(b, childId, "start_spectrum", {
          question: "Isolated fresh generation authority",
          mode: "council",
          colors: [
            { label: "Blue", selection },
            { label: "Red", selection },
          ],
          clientRequestId: "automation384-spectrum-start",
        });
        expect(started.isError, json(started)).not.toBe(true);
        const result = yield* decodeAutomationSpectrumResult(started.structuredContent);
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const read = SpectrumStore.readSpectrum(result.threadId).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
          Effect.map(Option.getOrThrow),
        );
        const commandContext = yield* SpectrumCommands.layer.pipe(
          Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const dispatch = (command: Parameters<typeof b.threads.dispatch>[0]) =>
          b.threads
            .dispatch(command)
            .pipe(
              Effect.provideService(
                ForkCommandInterceptor,
                Context.get(commandContext, ForkCommandInterceptor),
              ),
            );
        const original = yield* read;
        expect(original.automationCertificateId).toBeDefined();
        const reportsContext = yield* SpectrumReports.layer.pipe(
          Layer.provide(Layer.succeedContext(b.runtime)),
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const reports = Context.get(reportsContext, SpectrumReports.SpectrumReportService);
        const stop = (key: string) =>
          Effect.gen(function* () {
            yield* dispatch({
              type: "thread.stop",
              threadId: result.threadId,
              commandId: CommandId.make(`${key}:stop`),
            });
            const stopped = yield* read;
            if (stopped.report === null)
              return yield* Effect.die("Stop must retain its report obligation");
            yield* reports.abandon(result.threadId, stopped.report.commandId, "isolated-human-384");
            return yield* read;
          });
        const stopped = yield* stop("automation384-spectrum-first");
        yield* dispatch({
          type: "thread.unsettle",
          threadId: result.threadId,
          reason: "user",
          commandId: CommandId.make("automation384-spectrum-reopen"),
        });
        const reopened = yield* read;
        expect(reopened.status).toBe("active");
        expect(reopened.generation).toBe(stopped.generation + 1);
        expect(reopened.automationCertificateId).toBeDefined();
        expect(reopened.automationCertificateId).not.toBe(original.automationCertificateId);
        const births = yield* sql<{
          record_id: string;
        }>`SELECT record_id FROM peer_thread_authority WHERE thread_id=${result.threadId}`;
        expect(births).toHaveLength(1);
        expect((yield* Effect.result(b.authority.checkRecord(births[0]!.record_id)))._tag).toBe(
          "Success",
        );
        expect(
          (yield* Effect.result(
            b.authority.automationExecution(
              original.automationCertificateId!,
              "retired-spectrum-must-not-execute",
            ),
          ))._tag,
        ).toBe("Failure");
        const retained = yield* stop("automation384-spectrum-second");
        const footprint = yield* sql`SELECT sequence FROM orchestration_events`;
        const effects = yield* sql`SELECT effect_id,status FROM orchestration_v2_effect_outbox`;
        yield* b.auth.revokeSession(incoming.sessionId);
        expect(
          (yield* Effect.result(
            dispatch({
              type: "thread.unsettle",
              threadId: result.threadId,
              reason: "user",
              commandId: CommandId.make("automation384-spectrum-denied-reopen"),
            }),
          ))._tag,
        ).toBe("Failure");
        expect(yield* read).toEqual(retained);
        expect(yield* sql`SELECT sequence FROM orchestration_events`).toEqual(footprint);
        expect(yield* sql`SELECT effect_id,status FROM orchestration_v2_effect_outbox`).toEqual(
          effects,
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each(["council", "free"] as const)(
  "automation384: %s round and lost native acceptance replay stay local while new revoked work is refused",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf(`automation384-${mode}-source`, "Round source", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        const b = yield* makeNode(
          descriptorOf(`automation384-${mode}-destination`, "Round destination", {
            peerAuthority: 1,
          }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make(`automation384-${mode}-parent`);
        yield* seedParent(a, parent);
        expect(
          (yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId }))
            .isError,
        ).not.toBe(true);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          onward: true,
        });
        const { childId } = yield* delegate(a, b, parent, `automation384-${mode}-child`);
        yield* activate(b, childId);
        const started = yield* nativeCall(b, childId, "start_spectrum", {
          question: "Isolated local round authority",
          mode,
          colors: [
            { label: "Blue", selection },
            { label: "Red", selection },
          ],
          clientRequestId: `automation384-${mode}-start`,
        });
        expect(started.isError, json(started)).not.toBe(true);
        const result = yield* decodeAutomationSpectrumResult(started.structuredContent);
        yield* linkTo(b.links, a, "full-access");
        const enrolled = yield* nativeCall(b, childId, "t3_project_list", {
          environmentId: a.environmentId,
        });
        expect((yield* a.authority.grants).length, json(enrolled)).toBeGreaterThan(0);
        const onwardGrant = (yield* a.authority.grants)[0]!;
        yield* a.authority.setGrant({
          ...onwardGrant,
          resourceScope: "environment-resources",
          onward: true,
        });
        const online = yield* nativeCall(b, result.participants[0]!.threadId, "t3_project_list", {
          environmentId: a.environmentId,
        });
        expect(online.isError, json(online)).not.toBe(true);
        const commands = yield* SpectrumCommands.layer.pipe(
          Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const dispatch = (command: Parameters<typeof b.threads.dispatch>[0]) =>
          b.threads
            .dispatch(command)
            .pipe(
              Effect.provideService(
                ForkCommandInterceptor,
                Context.get(commands, ForkCommandInterceptor),
              ),
            );
        const reports = Context.get(
          yield* SpectrumReports.layer.pipe(
            Layer.provide(Layer.succeedContext(b.runtime)),
            Layer.provide(Layer.succeedContext(b.database)),
            Layer.build,
          ),
          SpectrumReports.SpectrumReportService,
        );
        yield* dispatch({
          type: "thread.stop",
          threadId: result.threadId,
          commandId: CommandId.make(`automation384-${mode}-stop`),
        });
        const before = Option.getOrThrow(
          yield* SpectrumStore.readSpectrum(result.threadId).pipe(
            Effect.provide(Layer.succeedContext(b.database)),
          ),
        );
        if (before.report === null) return yield* Effect.die("Missing exact retired report");
        yield* reports.abandon(result.threadId, before.report.commandId, "isolated-human-384");
        yield* dispatch({
          type: "thread.unsettle",
          reason: "user",
          threadId: result.threadId,
          commandId: CommandId.make(`automation384-${mode}-fresh-reopen`),
        });
        const reopened = Option.getOrThrow(
          yield* SpectrumStore.readSpectrum(result.threadId).pipe(
            Effect.provide(Layer.succeedContext(b.database)),
          ),
        );
        expect(reopened.automationCertificateId).not.toBe(before.automationCertificateId);
        expect(reopened.generation).toBe(before.generation + 1);
        yield* a.stopConnections;
        const makeRounds = SpectrumRounds.layer.pipe(
          Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
          Layer.provide(Layer.succeedContext(b.runtime)),
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.fresh,
          Layer.build,
        );
        const rounds = Context.get(yield* makeRounds, SpectrumRounds.SpectrumRoundService);
        const prepared = yield* rounds.prepare(result.threadId);
        const pending = prepared.outbox.find((command) => command.type === "message.dispatch");
        if (pending?.type !== "message.dispatch" || prepared.automationCertificateId === undefined)
          return yield* Effect.die("Missing accepted round command");
        const owner = yield* b.authority.automationExecution(
          prepared.automationCertificateId,
          pending.commandId,
        );
        const accepted = yield* b.threads
          .dispatch(pending)
          .pipe(
            Effect.provideService(PeerDispatchAuthority, owner.id),
            Effect.provideService(ForkDispatchPlans, [
              spectrumLaunchAdmission(result.threadId, prepared.generation, pending),
            ]),
          );
        expect(accepted.storedEvents.some((stored) => stored.event.type === "run.created")).toBe(
          true,
        );
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const effects =
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE command_id=${pending.commandId}`;
        expect(effects.length).toBeGreaterThan(0);
        expect(
          yield* sql`SELECT 1 FROM peer_control_frames WHERE answer_json IS NULL AND json_extract(frame_json,'$.body.type') IN ('validate','capture-automation')`,
        ).toHaveLength(0);
        const offline = yield* nativeCall(b, pending.threadId, "t3_project_list", {
          environmentId: a.environmentId,
        });
        expect(offline.isError, json(offline)).toBe(true);
        yield* b.auth.revokeSession(grant.sessionId);
        const reconstructed = Context.get(yield* makeRounds, SpectrumRounds.SpectrumRoundService);
        const recovered = yield* reconstructed.dispatch(result.threadId, pending.commandId);
        expect(
          recovered.round?.slots.find((slot) => slot.commandId === pending.commandId)?.runId,
        ).not.toBeNull();
        expect(
          yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE command_id=${pending.commandId}`,
        ).toEqual(effects);
        expect(
          yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id=${pending.commandId} AND status='accepted'`,
        ).toHaveLength(1);
        expect((yield* Effect.result(b.authority.checkRecord(owner.id)))._tag).toBe("Failure");
        const next = recovered.outbox.find((command) => command.type === "message.dispatch");
        if (next !== undefined) {
          expect(
            (yield* Effect.result(reconstructed.dispatch(result.threadId, next.commandId)))._tag,
          ).toBe("Failure");
          expect(
            yield* sql`SELECT effect_id FROM orchestration_v2_effect_outbox WHERE command_id=${next.commandId}`,
          ).toHaveLength(0);
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each(["council", "free"] as const)(
  "automation384: %s Colors and White finish through real native effects with the source disconnected",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf(`automation384-${mode}-complete-source`, "Complete source", {
            peerAuthority: 1,
          }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        let callerThreadId: ThreadId | undefined;
        let council: typeof SpectrumMcp.StartSpectrumResult.Type | undefined;
        const startedTurns: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const root = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped({
          prefix: "automation384-rounds-",
        });
        const b = yield* makeNode(
          descriptorOf(`automation384-${mode}-complete-destination`, "Complete destination", {
            peerAuthority: 1,
          }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: root,
            providerAdapter: automationAdapter(startedTurns, opened, (turn, events) => {
              if (turn.threadId === callerThreadId) return Effect.void;
              const pending = council?.participants.find(
                (color) => !startedTurns.some((started) => started.threadId === color.threadId),
              );
              const text =
                mode === "free" && turn.threadId === council?.white.threadId
                  ? turn.message.text.includes("closing summary")
                    ? "Isolated final summary"
                    : pending === undefined
                      ? "DONE"
                      : `NEXT: ${pending.label}`
                  : "Isolated Color answer";
              return automationReply(turn, events, text);
            }),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make(`automation384-${mode}-complete-parent`);
        yield* seedParent(a, parent);
        expect(
          (yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId }))
            .isError,
        ).not.toBe(true);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({ ...grant, resourceScope: "environment-resources" });
        const { childId } = yield* delegate(a, b, parent, `automation384-${mode}-complete-child`);
        callerThreadId = childId;
        const callerPrepared = yield* b.threads
          .streamStoredEventsFrom({
            threadId: childId,
            afterSequence: 0,
            eventType: "run.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "run.updated" && stored.event.payload.status !== "preparing",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        expect(
          callerPrepared.event.type === "run.updated"
            ? callerPrepared.event.payload.status
            : "missing",
          json(callerPrepared),
        ).not.toBe("failed");
        yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
        expect(startedTurns.some((turn) => turn.threadId === childId)).toBe(true);
        const openedSpectrum = yield* nativeCall(b, childId, "start_spectrum", {
          question: "Isolated native complete round proof",
          mode,
          relayRounds: 2,
          turnsPerColor: 1,
          colors: [
            { label: "Blue", selection },
            { label: "Red", selection },
          ],
          clientRequestId: `automation384-${mode}-complete-start`,
        });
        expect(openedSpectrum.isError, json(openedSpectrum)).not.toBe(true);
        const result = yield* decodeAutomationSpectrumResult(openedSpectrum.structuredContent);
        council = result;
        yield* a.stopConnections;
        const services = yield* SpectrumController.layer.pipe(
          Layer.provide(
            Layer.mergeAll(SpectrumRounds.layer, SpectrumReports.layer, SpectrumTranscript.layer),
          ),
          Layer.provide(Receipts.layer),
          Layer.provide(ThreadCommandExecutor.layer),
          Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
          Layer.provide(Layer.succeedContext(b.runtime)),
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const controller = Context.get(services, SpectrumController.SpectrumController);
        const worker = Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2);
        const read = SpectrumStore.readSpectrum(result.threadId).pipe(
          Effect.provide(Layer.succeedContext(b.database)),
          Effect.map(Option.getOrThrow),
        );
        for (let barrier = 0; barrier < 10; barrier++) {
          const state = yield* controller.resume(result.threadId);
          if (state.status !== "active") break;
          const runs =
            state.round?.slots.flatMap((slot) =>
              slot.runId === null || slot.replyIds !== null
                ? []
                : [{ runId: slot.runId, threadId: slot.threadId }],
            ) ?? [];
          expect(runs.length).toBeGreaterThan(0);
          yield* worker.drain();
          for (const run of runs) {
            const completed = yield* finishAutomationRun(b, run.threadId, run.runId);
            expect(
              completed.event.type === "run.updated" ? completed.event.payload.status : "missing",
              json(completed),
            ).toBe("completed");
          }
        }
        const settled = yield* read;
        expect(
          settled.status,
          json({
            report: settled.report,
            round: settled.round,
            started: startedTurns.map((turn) => turn.threadId),
          }),
        ).toBe("settled");
        const whiteTurns = startedTurns.filter((turn) => turn.threadId === result.white.threadId);
        expect(whiteTurns.length).toBeGreaterThanOrEqual(mode === "council" ? 1 : 2);
        if (mode === "free")
          expect(whiteTurns.some((turn) => turn.message.text.includes("closing summary"))).toBe(
            true,
          );
        for (const color of result.participants)
          expect(startedTurns.some((turn) => turn.threadId === color.threadId)).toBe(true);
        expect(settled.report).not.toBeNull();
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(
          yield* sql`SELECT 1 FROM peer_control_frames WHERE answer_json IS NULL AND json_extract(frame_json,'$.body.type') IN ('validate','capture-automation')`,
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each(
  (["council", "free"] as const).flatMap((mode) =>
    (["offline", "revoked", "narrowed", "expired", "tombstone"] as const).map((reason) => ({
      mode,
      reason,
    })),
  ),
)(
  "automation384-r2-lifecycle: completed peer $mode members allow live follow-ups and refuse $reason original contributors",
  ({ mode, reason }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf(`automation384-${mode}-complete-source`, "Complete source", {
            peerAuthority: 1,
          }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        let callerThreadId: ThreadId | undefined;
        let council: typeof SpectrumMcp.StartSpectrumResult.Type | undefined;
        const startedTurns: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const root = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped({
          prefix: "automation384-rounds-",
        });
        const b = yield* makeNode(
          descriptorOf(`automation384-${mode}-complete-destination`, "Complete destination", {
            peerAuthority: 1,
          }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: root,
            providerAdapter: automationAdapter(startedTurns, opened, (turn, events) => {
              if (turn.threadId === callerThreadId) return Effect.void;
              const pending = council?.participants.find(
                (color) => !startedTurns.some((started) => started.threadId === color.threadId),
              );
              const text =
                mode === "free" && turn.threadId === council?.white.threadId
                  ? turn.message.text.includes("closing summary")
                    ? "Isolated final summary"
                    : pending === undefined
                      ? "DONE"
                      : `NEXT: ${pending.label}`
                  : "Isolated Color answer";
              return automationReply(turn, events, text);
            }),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make(`automation384-${mode}-complete-parent`);
        yield* seedParent(a, parent);
        expect(
          (yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId }))
            .isError,
        ).not.toBe(true);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          preview: true,
          onward: reason === "narrowed" ? true : grant.onward,
        });
        const { childId } = yield* delegate(a, b, parent, `automation384-${mode}-complete-child`);
        callerThreadId = childId;
        const callerPrepared = yield* b.threads
          .streamStoredEventsFrom({
            threadId: childId,
            afterSequence: 0,
            eventType: "run.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "run.updated" && stored.event.payload.status !== "preparing",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        expect(
          callerPrepared.event.type === "run.updated"
            ? callerPrepared.event.payload.status
            : "missing",
          json(callerPrepared),
        ).not.toBe("failed");
        yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
        expect(startedTurns.some((turn) => turn.threadId === childId)).toBe(true);
        const openedSpectrum = yield* nativeCall(b, childId, "start_spectrum", {
          question: "Isolated native complete round proof",
          mode,
          relayRounds: 2,
          turnsPerColor: 1,
          colors: [
            { label: "Blue", selection },
            { label: "Red", selection },
          ],
          clientRequestId: `automation384-${mode}-complete-start`,
        });
        expect(openedSpectrum.isError, json(openedSpectrum)).not.toBe(true);
        const result = yield* decodeAutomationSpectrumResult(openedSpectrum.structuredContent);
        council = result;
        const services = yield* SpectrumController.layer.pipe(
          Layer.provide(
            Layer.mergeAll(SpectrumRounds.layer, SpectrumReports.layer, SpectrumTranscript.layer),
          ),
          Layer.provide(Receipts.layer),
          Layer.provide(ThreadCommandExecutor.layer),
          Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
          Layer.provide(Layer.succeedContext(b.runtime)),
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const controller = Context.get(services, SpectrumController.SpectrumController);
        const worker = Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2);
        const read = SpectrumStore.readSpectrum(result.threadId).pipe(
          Effect.provide(Layer.succeedContext(b.database)),
          Effect.map(Option.getOrThrow),
        );
        for (let barrier = 0; barrier < 10; barrier++) {
          const state = yield* controller.resume(result.threadId);
          if (state.status !== "active") break;
          const runs =
            state.round?.slots.flatMap((slot) =>
              slot.runId === null || slot.replyIds !== null
                ? []
                : [{ runId: slot.runId, threadId: slot.threadId }],
            ) ?? [];
          expect(runs.length).toBeGreaterThan(0);
          yield* worker.drain();
          for (const run of runs) {
            const completed = yield* finishAutomationRun(b, run.threadId, run.runId);
            expect(
              completed.event.type === "run.updated" ? completed.event.payload.status : "missing",
              json(completed),
            ).toBe("completed");
          }
        }
        const settled = yield* read;
        expect(
          settled.status,
          json({
            report: settled.report,
            round: settled.round,
            started: startedTurns.map((turn) => turn.threadId),
          }),
        ).toBe("settled");
        const whiteTurns = startedTurns.filter((turn) => turn.threadId === result.white.threadId);
        expect(whiteTurns.length).toBeGreaterThanOrEqual(mode === "council" ? 1 : 2);
        if (mode === "free")
          expect(whiteTurns.some((turn) => turn.message.text.includes("closing summary"))).toBe(
            true,
          );
        for (const color of result.participants)
          expect(startedTurns.some((turn) => turn.threadId === color.threadId)).toBe(true);
        expect(settled.report).not.toBeNull();
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const births = yield* sql`SELECT * FROM peer_thread_authority ORDER BY thread_id`;
        expect((yield* Effect.result(b.authority.checkThread(childId)))._tag).toBe("Success");
        for (const member of [...result.participants, result.white]) {
          const follow = yield* b.threads.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`r2-live-follow:${member.threadId}`),
            threadId: member.threadId,
            messageId: MessageId.make(`r2-live-message:${member.threadId}`),
            text: "Ordinary authorized follow-up after completion",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const run = follow.storedEvents.find((stored) => stored.event.type === "run.created");
          if (run?.event.type !== "run.created")
            return yield* Effect.die("Missing peer member follow-up run");
          const completed = yield* drainAutomationRun(b, member.threadId, run.event.payload);
          expect(
            completed.event.type === "run.updated" ? completed.event.payload.status : undefined,
          ).toBe("completed");
        }
        expect(yield* sql`SELECT * FROM peer_thread_authority ORDER BY thread_id`).toEqual(births);
        expect(
          (yield* Effect.result(
            b.authority.automationExecution(
              settled.automationCertificateId!,
              "retired-must-not-run",
            ),
          ))._tag,
        ).toBe("Failure");
        if (reason === "narrowed")
          expect((yield* b.authority.checkThread(result.white.threadId))?.ceiling.onward).toBe(
            true,
          );
        if (reason === "offline") yield* a.stopConnections;
        else if (reason === "revoked") yield* b.auth.revokeSession(grant.sessionId);
        else if (reason === "narrowed")
          yield* b.authority.setGrant({
            ...grant,
            resourceScope: "environment-resources",
            preview: false,
            onward: false,
          });
        else if (reason === "expired")
          yield* sql`UPDATE auth_sessions SET expires_at='2020-01-01T00:00:00.000Z' WHERE session_id=${grant.sessionId}`;
        else {
          yield* a.links.unlink(b.environmentId);
          const source = yield* a.reconstructAutomation;
          if (source.peers.drainAutomationDisables === undefined)
            return yield* Effect.die("Missing actual disable driver");
          yield* source.peers.drainAutomationDisables;
          expect(
            yield* sql`SELECT 1 FROM fork_automation_disable_acks WHERE certificate_id=${settled.automationCertificateId!}`,
          ).toHaveLength(1);
        }
        if (reason === "narrowed")
          expect(yield* sql`SELECT revoked FROM fork_automation_certificates`).toEqual([
            { revoked: 0 },
          ]);
        const before = yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`;
        const offline = yield* Effect.result(
          b.threads.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make("r2-offline-follow"),
            threadId: result.white.threadId,
            messageId: MessageId.make("r2-offline-message"),
            text: "Offline original contributor must deny",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          }),
        );
        expect(offline._tag).toBe("Failure");
        expect((yield* Effect.result(b.authority.checkThread(result.white.threadId)))._tag).toBe(
          "Failure",
        );
        expect(yield* sql`SELECT * FROM orchestration_v2_effect_outbox ORDER BY effect_id`).toEqual(
          before,
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each(["council", "free"] as const)(
  "automation384-r1: completed local %s Colors and White retain ordinary follow-ups",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        let callerThreadId: ThreadId | undefined;
        let council: typeof SpectrumMcp.StartSpectrumResult.Type | undefined;
        const startedTurns: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const root = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped({
          prefix: "automation384-rounds-",
        });
        const b = yield* makeNode(
          descriptorOf(`automation384-r1-${mode}-local-destination`, "Complete destination", {
            peerAuthority: 1,
          }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: root,
            providerAdapter: automationAdapter(startedTurns, opened, (turn, events) => {
              if (turn.threadId === callerThreadId) return Effect.void;
              const pending = council?.participants.find(
                (color) => !startedTurns.some((started) => started.threadId === color.threadId),
              );
              const text =
                mode === "free" && turn.threadId === council?.white.threadId
                  ? turn.message.text.includes("closing summary")
                    ? "Isolated final summary"
                    : pending === undefined
                      ? "DONE"
                      : `NEXT: ${pending.label}`
                  : "Isolated Color answer";
              return automationReply(turn, events, text);
            }),
          },
        );
        const childId = ThreadId.make(`automation384-r1-local-${mode}-caller`);
        callerThreadId = childId;
        yield* seedParent(b, childId);
        yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();

        const changes = yield* Context.get(b.services, PeerLinkEvents.PeerLinkEvents)
          .subscribeAutomationChanges;
        const openedSpectrum = yield* nativeCall(b, childId, "start_spectrum", {
          question: "Isolated native complete round proof",
          mode,
          relayRounds: 2,
          turnsPerColor: 1,
          colors: [
            { label: "Blue", selection },
            { label: "Red", selection },
          ],
          clientRequestId: `automation384-${mode}-complete-start`,
        });
        expect(openedSpectrum.isError, json(openedSpectrum)).not.toBe(true);
        const result = yield* decodeAutomationSpectrumResult(openedSpectrum.structuredContent);
        council = result;
        yield* PubSub.take(changes);
        const services = yield* SpectrumController.layer.pipe(
          Layer.provide(
            Layer.mergeAll(SpectrumRounds.layer, SpectrumReports.layer, SpectrumTranscript.layer),
          ),
          Layer.provide(Receipts.layer),
          Layer.provide(ThreadCommandExecutor.layer),
          Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
          Layer.provide(Layer.succeedContext(b.runtime)),
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const controller = Context.get(services, SpectrumController.SpectrumController);
        const worker = Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2);
        const read = SpectrumStore.readSpectrum(result.threadId).pipe(
          Effect.provide(Layer.succeedContext(b.database)),
          Effect.map(Option.getOrThrow),
        );
        for (let barrier = 0; barrier < 10; barrier++) {
          const state = yield* controller.resume(result.threadId);
          if (state.status !== "active") break;
          const runs =
            state.round?.slots.flatMap((slot) =>
              slot.runId === null || slot.replyIds !== null
                ? []
                : [{ runId: slot.runId, threadId: slot.threadId }],
            ) ?? [];
          expect(runs.length).toBeGreaterThan(0);
          yield* worker.drain();
          for (const run of runs) {
            const completed = yield* finishAutomationRun(b, run.threadId, run.runId);
            expect(
              completed.event.type === "run.updated" ? completed.event.payload.status : "missing",
              json(completed),
            ).toBe("completed");
          }
        }
        const settled = yield* read;
        expect(
          settled.status,
          json({
            report: settled.report,
            round: settled.round,
            started: startedTurns.map((turn) => turn.threadId),
          }),
        ).toBe("settled");
        const whiteTurns = startedTurns.filter((turn) => turn.threadId === result.white.threadId);
        expect(whiteTurns.length).toBeGreaterThanOrEqual(mode === "council" ? 1 : 2);
        if (mode === "free")
          expect(whiteTurns.some((turn) => turn.message.text.includes("closing summary"))).toBe(
            true,
          );
        for (const color of result.participants)
          expect(startedTurns.some((turn) => turn.threadId === color.threadId)).toBe(true);
        expect(settled.report).not.toBeNull();
        expect(settled.automationCertificateId).toBeUndefined();
        for (const member of [...result.participants, result.white]) {
          const followed = yield* b.threads.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`local-spectrum-r1:${member.threadId}`),
            threadId: member.threadId,
            messageId: MessageId.make(`local-spectrum-r1-message:${member.threadId}`),
            text: "Ordinary local follow-up after completion",
            attachments: [],
            dispatchMode: { type: "start_immediately" },
            createdBy: "user",
            creationSource: "web",
          });
          const run = followed.storedEvents.find((event) => event.event.type === "run.created");
          if (run?.event.type !== "run.created")
            return yield* Effect.die("Missing local Spectrum follow-up run");
          const completed = yield* drainAutomationRun(b, member.threadId, run.event.payload);
          expect(
            completed.event.type === "run.updated" ? completed.event.payload.status : undefined,
          ).toBe("completed");
          expect(yield* b.authority.checkThread(member.threadId)).toBeUndefined();
        }

        const commands = yield* SpectrumCommands.layer.pipe(
          Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const dispatch = (command: Parameters<typeof b.threads.dispatch>[0]) =>
          b.threads
            .dispatch(command)
            .pipe(
              Effect.provideService(
                ForkCommandInterceptor,
                Context.get(commands, ForkCommandInterceptor),
              ),
            );
        yield* dispatch({
          type: "thread.stop",
          threadId: result.threadId,
          commandId: CommandId.make("local-spectrum-r1-stop"),
        });
        const stopped = yield* read;
        if (stopped.report === null)
          return yield* Effect.die("Missing local Spectrum report obligation");
        const reportContext = yield* SpectrumReports.layer.pipe(
          Layer.provide(Layer.succeedContext(b.runtime)),
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        yield* Context.get(reportContext, SpectrumReports.SpectrumReportService).abandon(
          result.threadId,
          stopped.report.commandId,
          "isolated-local-r1",
        );
        yield* dispatch({
          type: "thread.unsettle",
          threadId: result.threadId,
          reason: "user",
          commandId: CommandId.make("local-spectrum-r1-reopen"),
        });
        const reopened = yield* read;
        expect(reopened.status).toBe("active");
        expect(reopened.generation).toBe(stopped.generation + 1);
        expect(reopened.automationCertificateId).toBeUndefined();
        const sql = Context.get(b.database, SqlClient.SqlClient);
        expect(
          yield* sql`SELECT 1 FROM peer_control_frames WHERE answer_json IS NULL AND json_extract(frame_json,'$.body.type') IN ('validate','capture-automation')`,
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each(["council", "free"] as const)(
  "automation384-r3: completed %s member can create live-authorized onward automation",
  (mode) =>
    Effect.scoped(
      Effect.gen(function* () {
        const a = yield* makeNode(
          descriptorOf(`automation384-${mode}-complete-source`, "Complete source", {
            peerAuthority: 1,
          }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        let callerThreadId: ThreadId | undefined;
        let council: typeof SpectrumMcp.StartSpectrumResult.Type | undefined;
        const startedTurns: Adapter.ProviderAdapterV2TurnInput[] = [];
        const opened: Adapter.ProviderAdapterV2OpenSessionInput[] = [];
        const root = yield* (yield* FileSystem.FileSystem).makeTempDirectoryScoped({
          prefix: "automation384-rounds-",
        });
        const b = yield* makeNode(
          descriptorOf(`automation384-${mode}-complete-destination`, "Complete destination", {
            peerAuthority: 1,
          }),
          "disk",
          {
            productionKernel: true,
            attachmentRequests: true,
            ownerWorkspace: root,
            providerAdapter: automationAdapter(startedTurns, opened, (turn, events) => {
              if (turn.threadId === callerThreadId || turn.message.text === "R3 live follow-up")
                return Effect.void;
              const pending = council?.participants.find(
                (color) => !startedTurns.some((started) => started.threadId === color.threadId),
              );
              const text =
                mode === "free" && turn.threadId === council?.white.threadId
                  ? turn.message.text.includes("closing summary")
                    ? "Isolated final summary"
                    : pending === undefined
                      ? "DONE"
                      : `NEXT: ${pending.label}`
                  : "Isolated Color answer";
              return automationReply(turn, events, text);
            }),
          },
        );
        yield* linkTo(a.links, b, "full-access");
        const parent = ThreadId.make(`automation384-${mode}-complete-parent`);
        yield* seedParent(a, parent);
        expect(
          (yield* nativeCall(a, parent, "t3_project_list", { environmentId: b.environmentId }))
            .isError,
        ).not.toBe(true);
        const grant = (yield* b.authority.grants)[0]!;
        yield* b.authority.setGrant({
          ...grant,
          resourceScope: "environment-resources",
          preview: true,
          onward: true,
        });
        const { childId } = yield* delegate(a, b, parent, `automation384-${mode}-complete-child`);
        callerThreadId = childId;
        const callerPrepared = yield* b.threads
          .streamStoredEventsFrom({
            threadId: childId,
            afterSequence: 0,
            eventType: "run.updated",
          })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "run.updated" && stored.event.payload.status !== "preparing",
            ),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );
        expect(
          callerPrepared.event.type === "run.updated"
            ? callerPrepared.event.payload.status
            : "missing",
          json(callerPrepared),
        ).not.toBe("failed");
        yield* Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2).drain();
        expect(startedTurns.some((turn) => turn.threadId === childId)).toBe(true);
        const openedSpectrum = yield* nativeCall(b, childId, "start_spectrum", {
          question: "Isolated native complete round proof",
          mode,
          relayRounds: 2,
          turnsPerColor: 1,
          colors: [
            { label: "Blue", selection },
            { label: "Red", selection },
          ],
          clientRequestId: `automation384-${mode}-complete-start`,
        });
        expect(openedSpectrum.isError, json(openedSpectrum)).not.toBe(true);
        const result = yield* decodeAutomationSpectrumResult(openedSpectrum.structuredContent);
        council = result;
        const services = yield* SpectrumController.layer.pipe(
          Layer.provide(
            Layer.mergeAll(SpectrumRounds.layer, SpectrumReports.layer, SpectrumTranscript.layer),
          ),
          Layer.provide(Receipts.layer),
          Layer.provide(ThreadCommandExecutor.layer),
          Layer.provide(Layer.succeed(PeerAuthority.PeerAuthority, b.authority)),
          Layer.provide(Layer.succeedContext(b.runtime)),
          Layer.provide(Layer.succeedContext(b.database)),
          Layer.build,
        );
        const controller = Context.get(services, SpectrumController.SpectrumController);
        const worker = Context.get(b.runtime, EffectWorker.OrchestrationEffectWorkerV2);
        const read = SpectrumStore.readSpectrum(result.threadId).pipe(
          Effect.provide(Layer.succeedContext(b.database)),
          Effect.map(Option.getOrThrow),
        );
        for (let barrier = 0; barrier < 10; barrier++) {
          const state = yield* controller.resume(result.threadId);
          if (state.status !== "active") break;
          const runs =
            state.round?.slots.flatMap((slot) =>
              slot.runId === null || slot.replyIds !== null
                ? []
                : [{ runId: slot.runId, threadId: slot.threadId }],
            ) ?? [];
          expect(runs.length).toBeGreaterThan(0);
          yield* worker.drain();
          for (const run of runs) {
            const completed = yield* finishAutomationRun(b, run.threadId, run.runId);
            expect(
              completed.event.type === "run.updated" ? completed.event.payload.status : "missing",
              json(completed),
            ).toBe("completed");
          }
        }
        const settled = yield* read;
        expect(
          settled.status,
          json({
            report: settled.report,
            round: settled.round,
            started: startedTurns.map((turn) => turn.threadId),
          }),
        ).toBe("settled");
        const whiteTurns = startedTurns.filter((turn) => turn.threadId === result.white.threadId);
        expect(whiteTurns.length).toBeGreaterThanOrEqual(mode === "council" ? 1 : 2);
        if (mode === "free")
          expect(whiteTurns.some((turn) => turn.message.text.includes("closing summary"))).toBe(
            true,
          );
        for (const color of result.participants)
          expect(startedTurns.some((turn) => turn.threadId === color.threadId)).toBe(true);
        expect(settled.report).not.toBeNull();
        const sql = Context.get(b.database, SqlClient.SqlClient);
        const c = yield* makeNode(
          descriptorOf("r3-onward-c", "Onward C", { peerAuthority: 1 }),
          "disk",
          { productionKernel: true, attachmentRequests: true },
        );
        yield* linkTo(b.links, c, "full-access");
        expect(
          (yield* nativeCall(b, childId, "t3_project_list", { environmentId: c.environmentId }))
            .isError,
        ).not.toBe(true);
        yield* c.authority.setGrant({
          ...(yield* c.authority.grants)[0]!,
          resourceScope: "environment-resources",
        });
        const args = {
          environmentId: c.environmentId,
          projectId: c.project.id,
          purposeIntent: { version: 1, intent: { type: "unscoped" } },
          prompt: "R3 onward admission",
          bindToCurrentThread: false,
          schedule: { type: "interval", everyMs: 60000 },
        };
        const control = yield* nativeCall(b, childId, "schedule_task", {
          ...args,
          clientRequestId: "r3-control",
        });
        expect(control.isError, json(control)).not.toBe(true);
        const member = result.white;
        const follow = yield* b.threads.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("r3-follow"),
          threadId: member.threadId,
          messageId: MessageId.make("r3-message"),
          text: "R3 live follow-up",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        const run = follow.storedEvents.find((stored) => stored.event.type === "run.created");
        if (run?.event.type !== "run.created") return yield* Effect.die("Missing follow-up");
        const followRunId = run.event.payload.id;
        if (run.event.payload.status === "preparing")
          yield* b.threads
            .streamStoredEventsFrom({
              threadId: member.threadId,
              afterSequence: 0,
              eventType: "run.updated",
            })
            .pipe(
              Stream.filter(
                (stored) =>
                  stored.event.type === "run.updated" &&
                  stored.event.payload.id === followRunId &&
                  stored.event.payload.status !== "preparing",
              ),
              Stream.runHead,
              Effect.flatMap(Effect.fromOption),
            );
        yield* worker.drain();
        expect((yield* Effect.result(b.authority.checkThread(member.threadId)))._tag).toBe(
          "Success",
        );
        const renamed = yield* nativeCall(a, parent, "t3_thread_update", {
          environmentId: b.environmentId,
          threadId: member.threadId,
          action: "rename",
          title: "Live authorized Spectrum member",
        });
        expect(renamed.isError, json(renamed)).not.toBe(true);
        expect((yield* b.threads.getThreadShell(member.threadId))?.title).toBe(
          "Live authorized Spectrum member",
        );
        const onward = yield* nativeCall(b, member.threadId, "schedule_task", {
          ...args,
          clientRequestId: "r3-member",
        });
        expect(onward.isError, json(onward)).not.toBe(true);
        const receipt = yield* decodeAutomationScheduleReceipt(onward.structuredContent);
        const csql = Context.get(c.database, SqlClient.SqlClient);
        const accepted = (yield* csql<{
          certificate_id: string;
        }>`SELECT certificate_id FROM fork_automation_bindings WHERE kind='schedule' AND resource_id=${receipt.scheduledTaskId}`)[0]!;
        const cdomain = Context.get(
          yield* AutomationAuthority.layer.pipe(
            Layer.provide(Layer.succeedContext(c.database)),
            Layer.build,
          ),
          AutomationAuthority.DestinationAutomationAuthority,
        );
        expect((yield* Effect.result(cdomain.claim(accepted.certificate_id)))._tag).toBe("Success");
        const domain = Context.get(
          yield* AutomationAuthority.layer.pipe(
            Layer.provide(Layer.succeedContext(b.database)),
            Layer.build,
          ),
          AutomationAuthority.DestinationAutomationAuthority,
        );
        yield* domain.disableLocal("spectrum", result.threadId);
        expect((yield* Effect.result(cdomain.claim(accepted.certificate_id)))._tag).toBe("Success");
        expect(
          (yield* b.authority.pendingAutomationDisables).filter(
            (item) => item.dependency.destinationEnvironmentId === c.environmentId,
          ),
        ).toEqual([]);
        const before = (yield* c.scheduled.list()).tasks;
        expect(before.some((task) => task.id === receipt.scheduledTaskId)).toBe(true);
        if (mode === "council") {
          yield* b.auth.revokeSession(grant.sessionId);
          const pending = (yield* b.authority.pendingAutomationDisables).filter(
            (item) => item.dependency.certificateId === accepted.certificate_id,
          );
          expect(pending.length).toBeGreaterThan(0);
          const peers = Context.get(b.services, PeerMcpClient.PeerMcpClient);
          if (peers.drainAutomationDisables === undefined)
            return yield* Effect.die("Missing production disable driver");
          yield* peers.drainAutomationDisables;
          expect((yield* Effect.result(cdomain.claim(accepted.certificate_id)))._tag).toBe(
            "Failure",
          );
          const rows = yield* csql<{
            ack_json: string;
          }>`SELECT ack_json FROM fork_automation_disable_acks WHERE certificate_id=${accepted.certificate_id}`;
          const acknowledged = yield* Effect.forEach(rows, (row) =>
            decodeAutomationAck(row.ack_json),
          );
          for (const item of pending)
            expect(acknowledged.map((ack) => ack.dependency)).toContainEqual(item.dependency);
          expect(
            (yield* b.authority.pendingAutomationDisables).filter(
              (item) => item.dependency.certificateId === accepted.certificate_id,
            ),
          ).toEqual([]);
        } else yield* a.stopConnections;
        const denied = yield* Effect.exit(
          nativeCall(b, member.threadId, "schedule_task", {
            ...args,
            clientRequestId: "r3-denied",
          }),
        );
        expect(denied._tag === "Failure" || denied.value.isError === true, json(denied)).toBe(true);
        expect((yield* c.scheduled.list()).tasks).toEqual(before);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
