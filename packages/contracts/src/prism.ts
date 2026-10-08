import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";

import { TrimmedNonEmptyString, TrimmedString } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

/**
 * Prism (Model Router) roles and their kits (toolboxmd/model-router#115).
 *
 * A role is a kit (instructions, skills) plus an ordered list of models;
 * only the worker keeps one list per lane. Every role may use every thread
 * tool. The kits live in server settings under `prismRoles`, overridable per
 * project like any key in `PROJECT_SCOPED_SERVER_SETTING_KEYS`;
 * `delegate_task` and `t3_thread_launch` apply them through Prism.
 */
export const PRISM_ROLES = [
  "promachos",
  "planner",
  "dispatcher",
  "reviewer",
  "worker",
  "retry",
  "escalation",
] as const;
export const PrismRole = Schema.Literals(PRISM_ROLES);
export type PrismRole = typeof PrismRole.Type;

/**
 * Keys Retry and Escalation had before they were renamed. Settings and
 * child thread ids saved with them still read as the new roles.
 */
export const LEGACY_PRISM_ROLE_KEYS: Readonly<Record<string, PrismRole>> = {
  correction: "retry",
  recovery: "escalation",
};

/** Work difficulty a job or spawn runs at; the worker keeps one model list per lane. */
const PRISM_LANES = ["easy", "medium", "hard"] as const;
export const PrismLane = Schema.Literals(PRISM_LANES);
export type PrismLane = typeof PrismLane.Type;
export const DEFAULT_PRISM_LANE: PrismLane = "medium";

/**
 * One entry of a role's model list. The same model at another effort is a
 * separate entry.
 */
export const PrismModelPreference = Schema.Struct({
  instanceId: ProviderInstanceId,
  model: TrimmedNonEmptyString,
  /** Claude effort, Codex/Grok reasoningEffort, OpenCode variant. */
  effort: Schema.optionalKey(TrimmedNonEmptyString),
});
export type PrismModelPreference = typeof PrismModelPreference.Type;

const modelList = Schema.Array(PrismModelPreference).pipe(
  Schema.withDecodingDefault(Effect.succeed([])),
);

export const PrismLaneModels = Schema.Struct({
  easy: modelList,
  medium: modelList,
  hard: modelList,
}).pipe(Schema.withDecodingDefault(Effect.succeed({})));
export type PrismLaneModels = typeof PrismLaneModels.Type;

/** Roles the user may switch off in Prism; the router then skips that step. */
export const PRISM_SWITCHABLE_ROLES = ["retry", "escalation"] as const;
export type PrismSwitchableRole = (typeof PRISM_SWITCHABLE_ROLES)[number];

/** Names shown to people. */
export const PRISM_ROLE_LABELS: Record<PrismRole, string> = {
  promachos: "Promachos",
  planner: "Planner",
  dispatcher: "Dispatcher",
  reviewer: "Reviewer",
  worker: "Worker",
  retry: "Retry",
  escalation: "Escalation",
};

/** Saved kits may still carry `threadTools`; decoding drops it like any unknown key. */
const kitFields = {
  /** Prepended to the first message of every thread started in this role. */
  instructions: TrimmedString.pipe(Schema.withDecodingDefault(Effect.succeed(""))),
  /** Skill names the role is told to use. */
  skills: Schema.Array(TrimmedNonEmptyString).pipe(Schema.withDecodingDefault(Effect.succeed([]))),
};

/**
 * The worker keeps one list per lane: the primary model first and fallbacks
 * after it. Eligible = these ∩ models enabled in Providers for the project
 * and environment.
 */
const workerKit = Schema.Struct({ ...kitFields, lanes: PrismLaneModels }).pipe(
  Schema.withDecodingDefault(Effect.succeed({})),
);

/**
 * Every other role keeps one ordered list. Settings saved before that kept
 * one list per lane; the medium list becomes the single list.
 */
const singleListKit = <Fields extends Schema.Struct.Fields>(extra: Fields) => {
  const kit = Schema.Struct({ ...kitFields, models: modelList, ...extra });
  return Schema.Record(Schema.String, Schema.Unknown).pipe(
    Schema.decodeTo(
      kit,
      SchemaTransformation.transform({
        decode: ({ lanes, ...fields }) => {
          const legacy = (lanes as { medium?: unknown } | undefined)?.medium;
          return (
            fields.models === undefined && legacy !== undefined
              ? { ...fields, models: legacy }
              : fields
          ) as typeof kit.Encoded;
        },
        encode: (fields) => fields as Record<string, unknown>,
      }),
    ),
    Schema.withDecodingDefault(Effect.succeed({})),
  );
};

const switchable = {
  /** Off: the router skips this step of its ladder. */
  enabled: Schema.Boolean.pipe(Schema.withDecodingDefault(Effect.succeed(true))),
};

const roleKits = Schema.Struct({
  promachos: singleListKit({}),
  planner: singleListKit({}),
  dispatcher: singleListKit({}),
  reviewer: singleListKit({}),
  worker: workerKit,
  retry: singleListKit(switchable),
  escalation: singleListKit(switchable),
});

/** All role kits; a role saved under its legacy key loads as the renamed role. */
export const PrismRoleKits = Schema.Record(Schema.String, Schema.Unknown).pipe(
  Schema.decodeTo(
    roleKits,
    SchemaTransformation.transform({
      decode: (saved) => {
        const kits: Record<string, unknown> = {};
        for (const [key, kit] of Object.entries(saved)) {
          const role = LEGACY_PRISM_ROLE_KEYS[key];
          if (role === undefined) kits[key] = kit;
          else if (saved[role] === undefined) kits[role] = kit;
        }
        return kits as typeof roleKits.Encoded;
      },
      encode: (kits) => kits as Record<string, unknown>,
    }),
  ),
);
export type PrismRoleKits = typeof PrismRoleKits.Type;
export type PrismRoleKit = PrismRoleKits[PrismRole];

export const DEFAULT_PRISM_ROLE_KITS: PrismRoleKits = Schema.decodeSync(PrismRoleKits)({});

/** The ordered list a spawn in `role` walks: the worker's lane, else the role's single list. */
export function prismRoleModels(
  kits: PrismRoleKits,
  role: PrismRole,
  lane: PrismLane,
): ReadonlyArray<PrismModelPreference> {
  return role === "worker" ? kits.worker.lanes[lane] : kits[role].models;
}

const kitPatchFields = {
  instructions: Schema.optionalKey(TrimmedString),
  skills: Schema.optionalKey(Schema.Array(TrimmedNonEmptyString)),
};
const singleListKitPatch = Schema.Struct({
  ...kitPatchFields,
  models: Schema.optionalKey(Schema.Array(PrismModelPreference)),
});
const switchableKitPatch = Schema.Struct({
  ...singleListKitPatch.fields,
  enabled: Schema.optionalKey(Schema.Boolean),
});

/** Per-role, per-field, per-lane update; arrays (skills, a model list) replace whole. */
export const PrismRoleKitsPatch = Schema.Struct({
  promachos: Schema.optionalKey(singleListKitPatch),
  planner: Schema.optionalKey(singleListKitPatch),
  dispatcher: Schema.optionalKey(singleListKitPatch),
  reviewer: Schema.optionalKey(singleListKitPatch),
  worker: Schema.optionalKey(
    Schema.Struct({
      ...kitPatchFields,
      lanes: Schema.optionalKey(
        Schema.Struct({
          easy: Schema.optionalKey(Schema.Array(PrismModelPreference)),
          medium: Schema.optionalKey(Schema.Array(PrismModelPreference)),
          hard: Schema.optionalKey(Schema.Array(PrismModelPreference)),
        }),
      ),
    }),
  ),
  retry: Schema.optionalKey(switchableKitPatch),
  escalation: Schema.optionalKey(switchableKitPatch),
});
export type PrismRoleKitsPatch = typeof PrismRoleKitsPatch.Type;

/**
 * Thread activity recorded at each turn end with the turn's provider stream
 * statistics (toolboxmd/t3code#55). Measurement only: clients hide it.
 */
export const PRISM_STREAM_STATS_ACTIVITY_KIND = "prism.stream-stats";

export const PrismStaleReason = Schema.Literals(["silence", "provider-dead", "provider-error"]);
export type PrismStaleReason = typeof PrismStaleReason.Type;
