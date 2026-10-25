import {
  type ThreadBare,
  type BareReceipt,
  type BareEvidence,
  BareEvidenceCategory,
  type ProviderDriverKind,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

export class BareLaunchError extends Schema.TaggedError<BareLaunchError>()("BareLaunchError", {
  reason: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message() {
    return this.reason;
  }
}
export function unsupportedBare(driver: ProviderDriverKind) {
  return {
    supported: false as const,
    reason:
      driver === "antigravity"
        ? "No installed/enabled stripping interface for Antigravity."
        : `No demonstrated bare stripping interface for ${driver}.`,
  };
}
export const namesT3Mcp = (bare: ThreadBare) =>
  bare.additions.some((addition) => addition.type === "mcp" && addition.server === "t3-code");

export function bareT3ToolNames(bare: ThreadBare | undefined): ReadonlyArray<string> | undefined {
  if (bare === undefined) return undefined;
  const additions = bare.additions.filter((addition) => addition.type === "mcp");
  return additions.some((addition) => addition.tool === undefined)
    ? undefined
    : additions.flatMap((addition) => (addition.tool === undefined ? [] : [addition.tool]));
}

/** Reconcile only files owned by the bare runtime. Never copy credentials. */
export const writeBareConfig = Effect.fn("BareLaunch.writeConfig")(function* (
  file: string,
  text: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fs.makeDirectory(path.dirname(file), { recursive: true, mode: 0o700 });
  if ((yield* fs.exists(file)) && (yield* fs.readFileString(file)) === text) return;
  const temporary = yield* fs.makeTempFile({
    directory: path.dirname(file),
    prefix: ".bare-config-",
  });
  yield* fs.writeFileString(temporary, text, { mode: 0o600 });
  yield* fs
    .rename(temporary, file)
    .pipe(Effect.onError(() => fs.remove(temporary).pipe(Effect.ignore)));
}, Effect.provide(Path.layer));

export function bareReceipt(input: {
  driver: ProviderDriverKind;
  bare: ThreadBare;
  mode: RuntimeMode;
  effective: string;
  controls: ReadonlyArray<string>;
  retained: ReadonlyArray<string>;
  root: string | null;
  sessionId: string | null;
  evidence?: ReadonlyArray<BareEvidence>;
}): BareReceipt {
  const evidence = input.evidence ?? [];
  return {
    driver: input.driver,
    permissions: { requested: null, admitted: input.mode, effective: input.effective },
    additions: input.bare.additions,
    planned: {
      controls: input.controls,
      removes: [
        "Chromeria instructions",
        "Prism kits",
        "auto-discovered instructions",
        "memory",
        "skills",
        "hooks",
        "plugins",
        "unnamed MCP/apps",
      ],
    },
    knownRetained: input.retained,
    evidence: [
      ...evidence,
      ...BareEvidenceCategory.literals
        .filter((category) => !evidence.some((entry) => entry.category === category))
        .map((category) => ({
          category,
          status: "unobserved" as const,
          identities: [],
          source: "No native evidence collected for this category.",
        })),
    ],
    nativeRecord: {
      harness: input.driver === "claudeAgent" ? "claude" : input.driver,
      root: input.root,
      sessionId: input.sessionId,
    },
  };
}
export function bareNoticeMessage(receipt: BareReceipt): string {
  const bounds = [256, 224, 320, 256, 320, 180, 256, 100];
  return [
    `Bare launch. Admitted permissions: ${receipt.permissions.admitted}; effective: ${receipt.permissions.effective}.`,
    `Planned removal: ${receipt.planned.removes.join(", ")}.`,
    `Kept by harness: ${receipt.knownRetained.join("; ") || "none known"}.`,
    `Named additions: ${receipt.additions.map((a) => (a.type === "mcp" ? a.server : a.path)).join(", ") || "none"}.`,
    `Native evidence: ${
      receipt.evidence
        .filter((e) => e.status !== "unobserved")
        .map((e) => `${e.category} ${e.status}: ${e.identities.join(", ")}`)
        .join("; ") || "none yet"
    }.`,
    `Unobserved: ${receipt.evidence
      .filter((e) => e.status === "unobserved")
      .map((e) => e.category)
      .join(", ")}.`,
    `Native record: ${receipt.nativeRecord.root ?? "unknown"}.`,
    `Native session: ${receipt.nativeRecord.sessionId ?? "unknown"}.`,
  ]
    .map((line, index) =>
      line.length <= bounds[index]! ? line : `${line.slice(0, bounds[index]! - 3)}...`,
    )
    .join("\n");
}

/** Inventories describe identities only; absence of an inventory stays unobserved. */
export function bareInventoryEvidence(
  category: BareEvidence["category"],
  identities: ReadonlyArray<string>,
  allowed: ReadonlyArray<string>,
  source: string,
  retained = false,
): BareEvidence {
  return {
    category,
    identities,
    source,
    status: identities.some((name) => !allowed.includes(name))
      ? "observed_unexpected"
      : retained && identities.length > 0
        ? "observed_retained"
        : "observed_clean",
  };
}
