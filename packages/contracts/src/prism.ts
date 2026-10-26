import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaTransformation from "effect/SchemaTransformation";

import { PositiveInt, TrimmedNonEmptyString, TrimmedString } from "./baseSchemas.ts";
import { ProviderOptionSelections } from "./model.ts";
import {
  defaultInstanceIdForDriver,
  ProviderDriverKind,
  ProviderInstanceId,
} from "./providerInstance.ts";

/**
 * Prism (Model Router) roles and their kits (toolboxmd/model-router#115).
 *
 * A role is a kit (instructions, skills) plus an ordered list of Colors;
 * only the worker keeps one list per lane. Every role may use every thread
 * tool. The kits live in server settings under `prismRoles`, overridable per
 * project like any key in `PROJECT_SCOPED_SERVER_SETTING_KEYS`;
 * `delegate_task` and `t3_thread_launch` apply them through Prism.
 */
export const PRISM_DELEGATE_ROLES = [
  "planner",
  "researcher",
  "designer",
  "worker",
  "computer-use",
  "roast",
  "reviewer",
  "judge",
] as const;
const PRISM_ROLES = [...PRISM_DELEGATE_ROLES, "promachos"] as const;
export const PrismRole = Schema.Literals(PRISM_ROLES);
export type PrismRole = typeof PrismRole.Type;

/** Roles `delegate_task` accepts. Promachos is a launch kit, not delegated work. */
export const PrismDelegateRole = Schema.Literals(PRISM_DELEGATE_ROLES);
export type PrismDelegateRole = typeof PrismDelegateRole.Type;

export const PRISM_DELEGATE_ROLE_ERROR = `prismRole must be one of: ${PRISM_DELEGATE_ROLES.join(", ")}.`;

/** Delegated roles an older build accepted; only a replay of such a request may name one. */
const REMOVED_DELEGATE_ROLES = ["dispatcher", "retry", "escalation"] as const;

/**
 * `delegate_task`'s `prismRole`. A removed name decodes so that retrying a
 * request an older build accepted can return its task; `admitDelegateRole`
 * refuses it on any new request.
 */
export const PrismDelegateRoleInput = Schema.Literals([
  ...PRISM_DELEGATE_ROLES,
  ...REMOVED_DELEGATE_ROLES,
]).annotate({ message: PRISM_DELEGATE_ROLE_ERROR });
export type PrismDelegateRoleInput = typeof PrismDelegateRoleInput.Type;

/**
 * Removed roles that stored records (scheduled tasks, remote launches) may
 * still name, read as the role that now does that work.
 */
const REMOVED_PRISM_ROLES = {
  dispatcher: "planner",
  retry: "worker",
  escalation: "worker",
  correction: "worker",
  recovery: "worker",
} as const satisfies Record<string, PrismRole>;

/** A role read back from storage; removed names decode as their successor. */
export const StoredPrismRole = Schema.Literals([
  ...PRISM_ROLES,
  ...(Object.keys(REMOVED_PRISM_ROLES) as Array<keyof typeof REMOVED_PRISM_ROLES>),
]).pipe(
  Schema.decodeTo(
    PrismRole,
    SchemaTransformation.transform({
      decode: (role) =>
        role in REMOVED_PRISM_ROLES
          ? REMOVED_PRISM_ROLES[role as keyof typeof REMOVED_PRISM_ROLES]
          : (role as PrismRole),
      encode: (role) => role,
    }),
  ),
);

/**
 * The role a `delegate_task` call runs as, or `undefined` when it names a
 * removed role on a request no older build accepted.
 */
export function admitDelegateRole(
  role: PrismDelegateRoleInput,
  isReplay: boolean,
): PrismDelegateRole | undefined {
  if (!(role in REMOVED_PRISM_ROLES)) return role as PrismDelegateRole;
  return isReplay ? REMOVED_PRISM_ROLES[role as keyof typeof REMOVED_PRISM_ROLES] : undefined;
}

/**
 * Read-only roles, the only ones whose Run may exceed 1: one delegation then
 * starts that many of the role's Colors as one Run group.
 */
const PRISM_RUN_GROUP_ROLES = ["researcher", "designer", "roast", "reviewer", "judge"] as const;
export type PrismRunGroupRole = (typeof PRISM_RUN_GROUP_ROLES)[number];
export function isPrismRunGroupRole(role: PrismRole): role is PrismRunGroupRole {
  return (PRISM_RUN_GROUP_ROLES as ReadonlyArray<PrismRole>).includes(role);
}

/**
 * The worker's lane: a difficulty, or `ui` for UI and frontend work. The
 * worker keeps one model list per lane; `ui` runs only on its own list.
 */
const PRISM_LANES = ["easy", "medium", "hard", "ui"] as const;
export const PrismLane = Schema.Literals(PRISM_LANES);
export type PrismLane = typeof PrismLane.Type;
export const DEFAULT_PRISM_LANE: PrismLane = "medium";

/**
 * A Color: one provider instance, model, effort and model options used together. Prism role
 * lists, the Promachos kit and Spectrum slots all store Colors; the same
 * model at another effort or with different options is a separate Color.
 */
export const Color = Schema.Struct({
  instanceId: ProviderInstanceId,
  model: TrimmedNonEmptyString,
  /** Claude effort, Codex/Grok reasoningEffort, OpenCode variant. */
  effort: Schema.optionalKey(TrimmedNonEmptyString),
  options: Schema.optionalKey(ProviderOptionSelections),
});
export type Color = typeof Color.Type;

const modelList = Schema.Array(Color).pipe(Schema.withDecodingDefault(Effect.succeed([])));

export const PrismLaneModels = Schema.Struct({
  easy: modelList,
  medium: modelList,
  hard: modelList,
  ui: Schema.Array(Color).pipe(
    Schema.withDecodingDefault(
      Effect.succeed([
        {
          instanceId: defaultInstanceIdForDriver(ProviderDriverKind.make("claudeAgent")),
          model: "claude-opus-5-5",
          effort: "medium",
        },
      ]),
    ),
  ),
}).pipe(Schema.withDecodingDefault(Effect.succeed({})));
export type PrismLaneModels = typeof PrismLaneModels.Type;

/** Names shown to people. */
export const PRISM_ROLE_LABELS: Record<PrismRole, string> = {
  planner: "Planner",
  researcher: "Researcher",
  designer: "Designer",
  worker: "Worker",
  "computer-use": "Computer use",
  roast: "Roast",
  reviewer: "Reviewer",
  judge: "Judge",
  promachos: "Promachos",
};

/** What each role does; the settings page and the MCP tool descriptions show these. */
export const PRISM_ROLE_DESCRIPTIONS: Record<PrismRole, string> = {
  planner:
    "Owns judgment, plans and acceptance and delegates the rest. Lets the user chat with one model while another plans.",
  researcher:
    "Investigates read-only and reports cited facts, unknowns and options. Changes nothing.",
  designer:
    "Proposes or compares designs, interfaces and plans for a goal, and recommends one. Changes nothing.",
  worker:
    "Implements the task and commits as it goes. Use optional lane for difficulty: easy, medium, hard; or ui for UI and frontend work, which runs only on the ui lane's Colors.",
  "computer-use":
    'Drives the browser (preview_* tools) and, where available, the desktop through computer-use tools. Reports what it saw and did. For browser or desktop driving beyond a quick check, delegate with delegate_task using prismRole: "computer-use".',
  roast:
    "Attacks a plan or idea before work starts: weak assumptions, missing cases, cheaper alternatives.",
  reviewer:
    "Checks a finished change against its brief and reports defects with evidence and a verdict.",
  judge:
    "Scores or labels supplied outputs against a caller-provided rubric, one structured verdict per item. Never fixes.",
  promachos: "Starts a new Promachos conversation on the first Color with capacity.",
};

/** `delegate_task` roles with their descriptions, for tool and instruction text. */
export const PRISM_DELEGATE_ROLE_GUIDE = PRISM_DELEGATE_ROLES.map(
  (role) =>
    `${role} (${PRISM_ROLE_DESCRIPTIONS[role]}${isPrismRunGroupRole(role) ? " Read-only; supports Run groups when Run is above 1 in Prism settings." : ""})`,
).join("; ");

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

/** Every other role keeps one ordered list. */
const singleList = Schema.Struct({ ...kitFields, models: modelList });
/** How many of the role's Colors one delegation starts; Run above 1 makes a Run group. */
const runField = { run: PositiveInt.pipe(Schema.withDecodingDefault(Effect.succeed(1))) };
const runGroupList = Schema.Struct({ ...kitFields, ...runField, models: modelList });
/** Settings saved before one list per role kept one list per lane; medium becomes the list. */
const fromLegacyLanes = SchemaTransformation.transform({
  decode: ({ lanes, ...saved }: Record<string, unknown>) => {
    const legacy = (lanes as { medium?: unknown } | undefined)?.medium;
    return (
      saved.models === undefined && legacy !== undefined ? { ...saved, models: legacy } : saved
    ) as never;
  },
  encode: (decoded: object) => decoded as Record<string, unknown>,
});
const singleListKit = Schema.Record(Schema.String, Schema.Unknown).pipe(
  Schema.decodeTo(singleList, fromLegacyLanes),
  Schema.withDecodingDefault(Effect.succeed({})),
);
const runGroupKit = Schema.Record(Schema.String, Schema.Unknown).pipe(
  Schema.decodeTo(runGroupList, fromLegacyLanes),
  Schema.withDecodingDefault(Effect.succeed({})),
);

const computerUseKit = Schema.Struct({
  ...kitFields,
  models: Schema.Array(Color).pipe(
    Schema.withDecodingDefault(
      Effect.succeed([
        {
          instanceId: defaultInstanceIdForDriver(ProviderDriverKind.make("claudeAgent")),
          model: "claude-haiku-5-5",
        },
      ]),
    ),
  ),
}).pipe(Schema.withDecodingDefault(Effect.succeed({})));

/** Kits of removed roles in saved settings are dropped like any unknown key. */
export const PrismRoleKits = Schema.Struct({
  planner: singleListKit,
  researcher: runGroupKit,
  designer: runGroupKit,
  worker: workerKit,
  "computer-use": computerUseKit,
  roast: runGroupKit,
  reviewer: runGroupKit,
  judge: runGroupKit,
  promachos: singleListKit,
});
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
/** Worker and Planner may write only a Run of 1: they change things, so they never run as a group. */
const singleRun = {
  run: Schema.optionalKey(
    Schema.Literal(1).annotate({ message: "Run above 1 is only for read-only roles." }),
  ),
};
const runGroupKitPatch = Schema.Struct({
  ...singleListKitPatch.fields,
  run: Schema.optionalKey(PositiveInt),
});

/** Per-role, per-field, per-lane update; arrays (skills, a model list) replace whole. */
export const PrismRoleKitsPatch = Schema.Struct({
  planner: Schema.optionalKey(Schema.Struct({ ...singleListKitPatch.fields, ...singleRun })),
  "computer-use": Schema.optionalKey(Schema.Struct({ ...singleListKitPatch.fields, ...singleRun })),
  researcher: Schema.optionalKey(runGroupKitPatch),
  designer: Schema.optionalKey(runGroupKitPatch),
  worker: Schema.optionalKey(
    Schema.Struct({
      ...kitPatchFields,
      ...singleRun,
      lanes: Schema.optionalKey(
        Schema.Struct({
          easy: Schema.optionalKey(Schema.Array(Color)),
          medium: Schema.optionalKey(Schema.Array(Color)),
          hard: Schema.optionalKey(Schema.Array(Color)),
          ui: Schema.optionalKey(Schema.Array(Color)),
        }),
      ),
    }),
  ),
  roast: Schema.optionalKey(runGroupKitPatch),
  reviewer: Schema.optionalKey(runGroupKitPatch),
  judge: Schema.optionalKey(runGroupKitPatch),
  promachos: Schema.optionalKey(singleListKitPatch),
});
export type PrismRoleKitsPatch = typeof PrismRoleKitsPatch.Type;

/**
 * A project's `prismRoles` override: only the roles, fields and worker lanes
 * the project set, with no filled-in defaults. Each present value overlays
 * the environment kit and everything absent inherits it, so a later
 * environment change still reaches the project. A present empty model list
 * is a real value: that role runs on the caller's model. Removed roles are
 * dropped like any unknown key.
 */
export const PrismProjectRoleKits = Schema.Record(Schema.String, Schema.Unknown).pipe(
  Schema.decodeTo(
    PrismRoleKitsPatch,
    SchemaTransformation.transform({
      decode: (saved) =>
        Object.fromEntries(
          Object.entries(saved).map(([role, kit]) => {
            // Like `singleListKit`: a saved medium lane becomes the single list.
            if (role === "worker" || typeof kit !== "object" || kit === null) return [role, kit];
            const { lanes, ...fields } = kit as { lanes?: { medium?: unknown }; models?: unknown };
            return [
              role,
              fields.models === undefined && lanes?.medium !== undefined
                ? { ...fields, models: lanes.medium }
                : fields,
            ];
          }),
        ) as typeof PrismRoleKitsPatch.Encoded,
      encode: (decoded) => decoded as Record<string, unknown>,
    }),
  ),
);
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
 * Drop the empty lists, blank instructions and Run of 1 that a
 * whole-kit Prism override carries for roles the project never set, so those
 * roles inherit the environment. Set values are kept; `undefined` when
 * nothing is left.
 */
function dropUnsetPrismRoleFields(kits: PrismProjectRoleKits): PrismProjectRoleKits | undefined {
  const isSet = (value: unknown) =>
    value !== undefined && value !== "" && !(Array.isArray(value) && value.length === 0);
  const next: Record<string, Record<string, unknown>> = {};
  for (const [role, kit] of Object.entries(kits)) {
    const fields: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(kit ?? {})) {
      if (key !== "lanes") {
        // A whole kit fills Run with its default of 1, which the project never chose.
        if (isSet(value) && !(key === "run" && value === 1)) fields[key] = value;
        continue;
      }
      const lanes = Object.fromEntries(
        Object.entries(value ?? {}).filter(([, models]) => isSet(models)),
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
