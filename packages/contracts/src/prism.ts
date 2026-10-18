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
const PRISM_ROLES = [
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
const LEGACY_PRISM_ROLE_KEYS: Readonly<Record<string, PrismRole>> = {
  correction: "retry",
  recovery: "escalation",
};

/** Work difficulty a job or spawn runs at; the worker keeps one model list per lane. */
const PRISM_LANES = ["easy", "medium", "hard"] as const;
export const PrismLane = Schema.Literals(PRISM_LANES);
export type PrismLane = typeof PrismLane.Type;
export const DEFAULT_PRISM_LANE: PrismLane = "medium";

/**
 * A Color: one provider instance, model and effort used together. Prism role
 * lists, the Promachos kit and Spectrum slots all store Colors; the same
 * model at another effort is a separate Color.
 */
export const Color = Schema.Struct({
  instanceId: ProviderInstanceId,
  model: TrimmedNonEmptyString,
  /** Claude effort, Codex/Grok reasoningEffort, OpenCode variant. */
  effort: Schema.optionalKey(TrimmedNonEmptyString),
});
export type Color = typeof Color.Type;

const modelList = Schema.Array(Color).pipe(Schema.withDecodingDefault(Effect.succeed([])));

export const PrismLaneModels = Schema.Struct({
  easy: modelList,
  medium: modelList,
  hard: modelList,
}).pipe(Schema.withDecodingDefault(Effect.succeed({})));
export type PrismLaneModels = typeof PrismLaneModels.Type;

/** Roles with legacy enabled flags retained for saved-settings compatibility. */
const PRISM_SWITCHABLE_ROLES = ["retry", "escalation"] as const;
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
  /** Legacy preference only; explicit kit requests and automatic recovery ignore it. */
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

/**
 * A role saved under its legacy key loads as the renamed role. `kit` adapts
 * each saved kit before the role schema decodes it.
 */
const withLegacyRoleKeys = <S extends Schema.Top>(
  kits: S,
  kit: (role: string, saved: unknown) => unknown = (_role, saved) => saved,
) =>
  Schema.Record(Schema.String, Schema.Unknown).pipe(
    Schema.decodeTo(
      kits,
      SchemaTransformation.transform({
        decode: (saved) => {
          const renamed: Record<string, unknown> = {};
          for (const [key, value] of Object.entries(saved)) {
            const role = LEGACY_PRISM_ROLE_KEYS[key];
            if (role === undefined) renamed[key] = kit(key, value);
            else if (saved[role] === undefined) renamed[role] = kit(role, value);
          }
          return renamed as S["Encoded"];
        },
        encode: (decoded) => decoded as Record<string, unknown>,
      }),
    ),
  );

/** All role kits. */
export const PrismRoleKits = withLegacyRoleKeys(roleKits);
export type PrismRoleKits = typeof PrismRoleKits.Type;
export type PrismRoleKit = PrismRoleKits[PrismRole];

export const DEFAULT_PRISM_ROLE_KITS: PrismRoleKits = Schema.decodeSync(PrismRoleKits)({});

/** The ordered list a spawn in `role` walks: the worker's lane, else the role's single list. */
export function prismRoleModels(
  kits: PrismRoleKits,
  role: PrismRole,
  lane: PrismLane,
): ReadonlyArray<Color> {
  return role === "worker" ? kits.worker.lanes[lane] : kits[role].models;
}

const kitPatchFields = {
  instructions: Schema.optionalKey(TrimmedString),
  skills: Schema.optionalKey(Schema.Array(TrimmedNonEmptyString)),
};
const singleListKitPatch = Schema.Struct({
  ...kitPatchFields,
  models: Schema.optionalKey(Schema.Array(Color)),
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
          easy: Schema.optionalKey(Schema.Array(Color)),
          medium: Schema.optionalKey(Schema.Array(Color)),
          hard: Schema.optionalKey(Schema.Array(Color)),
        }),
      ),
    }),
  ),
  retry: Schema.optionalKey(switchableKitPatch),
  escalation: Schema.optionalKey(switchableKitPatch),
});
export type PrismRoleKitsPatch = typeof PrismRoleKitsPatch.Type;

/**
 * A project's `prismRoles` override: only the roles, fields and worker lanes
 * the project set, with no filled-in defaults. Each present value overlays
 * the environment kit and everything absent inherits it, so a later
 * environment change still reaches the project. A present empty model list
 * is a real value: that role runs on the caller's model.
 */
export const PrismProjectRoleKits = withLegacyRoleKeys(PrismRoleKitsPatch, (role, saved) => {
  // Like `singleListKit`: a saved medium lane becomes the single list.
  if (role === "worker" || typeof saved !== "object" || saved === null) return saved;
  const { lanes, ...fields } = saved as { lanes?: { medium?: unknown }; models?: unknown };
  return fields.models === undefined && lanes?.medium !== undefined
    ? { ...fields, models: lanes.medium }
    : fields;
});
export type PrismProjectRoleKits = typeof PrismProjectRoleKits.Type;

/** `kits` with each present role field (and worker lane) of `patch` replacing its own. */
export function overlayPrismRoleKits<Kits extends PrismRoleKits | PrismRoleKitsPatch>(
  kits: Kits,
  patch: PrismRoleKitsPatch,
): Kits {
  const next: Record<string, unknown> = { ...kits };
  for (const [role, fields] of Object.entries(patch)) {
    if (fields === undefined) continue;
    const current = (kits as Record<string, { lanes?: object } | undefined>)[role];
    next[role] =
      "lanes" in fields && fields.lanes !== undefined
        ? { ...current, ...fields, lanes: { ...current?.lanes, ...fields.lanes } }
        : { ...current, ...fields };
  }
  return next as Kits;
}

/**
 * Drop the empty lists, blank instructions and default `enabled` that a
 * whole-kit Prism override carries for roles the project never set, so those
 * roles inherit the environment. Set values are kept; `undefined` when
 * nothing is left.
 */
function dropUnsetPrismRoleFields(kits: PrismProjectRoleKits): PrismProjectRoleKits | undefined {
  const isSet = (key: string, value: unknown) =>
    value !== undefined &&
    value !== "" &&
    !(Array.isArray(value) && value.length === 0) &&
    !(key === "enabled" && value === true);
  const next: Record<string, Record<string, unknown>> = {};
  for (const [role, kit] of Object.entries(kits)) {
    const fields: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(kit ?? {})) {
      if (key !== "lanes") {
        if (isSet(key, value)) fields[key] = value;
        continue;
      }
      const lanes = Object.fromEntries(
        Object.entries(value ?? {}).filter(([lane, models]) => isSet(lane, models)),
      );
      if (Object.keys(lanes).length > 0) fields.lanes = lanes;
    }
    if (Object.keys(fields).length > 0) next[role] = fields;
  }
  return Object.keys(next).length > 0 ? (next as PrismProjectRoleKits) : undefined;
}

/** A project override entry with its Prism kits reduced to the roles it actually set. */
export function withoutUnsetPrismRoles<
  Entry extends { readonly prismRoles?: PrismProjectRoleKits },
>(entry: Entry): Entry {
  if (entry.prismRoles === undefined) return entry;
  const { prismRoles, ...rest } = entry;
  const kits = dropUnsetPrismRoleFields(prismRoles);
  return (kits === undefined ? rest : { ...rest, prismRoles: kits }) as Entry;
}

export const PrismStaleReason = Schema.Literals(["silence", "provider-dead", "provider-error"]);
export type PrismStaleReason = typeof PrismStaleReason.Type;
