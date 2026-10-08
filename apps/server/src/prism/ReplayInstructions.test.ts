import { ProviderDriverKind } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "@effect/vitest";
import {
  decodeProviderReplayNdjson,
  materializeReplayTranscriptRuntimeInstructions,
} from "../orchestration-v2/testkit/ReplayTranscriptNdjson.ts";

const museDriver = Schema.decodeUnknownSync(ProviderDriverKind)("muse");

const recorded =
  decodeProviderReplayNdjson(`{"type":"transcript_start","provider":"muse","protocol":"muse.msp-jsonl","version":"1","scenario":"runtime-context"}
{"type":"expect_outbound","frame":{}}`);

const userInput = [
  { type: "text", text: "Keep this user message exact." },
  { type: "image", url: "attachment://fixture" },
];

function transcript(context: string, reasoningEffort?: string) {
  return recorded.pipe(
    Effect.map((header) => ({
      ...header,
      entries: [
        {
          type: "expect_outbound" as const,
          frame: {
            jsonrpc: "2.0",
            id: 17,
            method: "turn/start",
            params: {
              sessionId: "fixture-session",
              input: [{ type: "text", text: context }, ...userInput],
              displayText: "exact display",
              ...(reasoningEffort === undefined ? {} : { reasoningEffort }),
            },
          },
        },
      ],
    })),
  );
}

describe("Muse replay runtime materialization", () => {
  it.effect.each([undefined, "high"])(
    "refreshes only recorded runtime context with effort %s",
    (effort) =>
      Effect.gen(function* () {
        const original = yield* transcript("<runtime_info>old context</runtime_info>", effort);
        const result = materializeReplayTranscriptRuntimeInstructions(original, {
          driver: museDriver,
          model: "muse-fixture-model",
        });
        const frame =
          result.entries[0]?.type === "expect_outbound" ? result.entries[0].frame : undefined;
        expect(frame).toEqual({
          ...original.entries[0]!.frame,
          params: {
            ...original.entries[0]!.frame.params,
            input: [{ type: "text", text: expect.stringContaining("Prism") }, ...userInput],
          },
        });
        expect(frame).toHaveProperty(
          "params.input.0.text",
          expect.stringContaining("through the Muse Code harness, as muse-fixture-model"),
        );
        if (effort === undefined) {
          expect(frame).toHaveProperty(
            "params.input.0.text",
            expect.not.stringContaining("reasoning effort"),
          );
        } else {
          expect(frame).toHaveProperty(
            "params.input.0.text",
            expect.stringContaining("high reasoning effort"),
          );
        }
        expect(
          materializeReplayTranscriptRuntimeInstructions(result, {
            driver: museDriver,
            model: "muse-fixture-model",
          }),
        ).toEqual(result);
      }),
  );

  it.effect("does not manufacture missing runtime context or change a user block", () =>
    Effect.gen(function* () {
      const original = yield* transcript("This is user content, not runtime context.", "high");
      expect(
        materializeReplayTranscriptRuntimeInstructions(original, {
          driver: museDriver,
          model: "muse-fixture-model",
        }),
      ).toEqual(original);
    }),
  );
});
