import { codeToTokens } from "@pierre/highlights";
import { pierreDark, pierreLight } from "@pierre/highlights/themes";
import { MOBILE_THEME_IDS } from "@t3tools/shared/themePalettes";
import { describe, expect, it, vi } from "vite-plus/test";

import { withReadableDarkPunctuation } from "../../scripts/generate-highlights.mts";
import { resolveHighlightLanguage } from "./highlightLanguage";
import { highlightCode, highlightCommand, tokenizeCode, type HighlightedLines } from "./highlights";
import { getMobileThemeVariables } from "./mobileTheme";

vi.mock("react-native-nitro-modules", async () => {
  const { createWasmHighlightsModule } = await import("./highlights.testing");
  const highlights = createWasmHighlightsModule();
  return { NitroModules: { hasHybridObject: () => true, createHybridObject: () => highlights } };
});

describe("highlightCode", () => {
  it("produces @pierre/highlights' own tokens and colors", () => {
    const code = [
      "/** 🎉 docs\r",
      " */",
      "export const greet = (name: string) => <b title={`hi ${name}`}>é</b>;",
      "const multi = `first",
      "second`; // done",
      "",
    ].join("\n");

    for (const [scheme, theme] of [
      ["light", pierreLight],
      ["dark", withReadableDarkPunctuation(pierreDark)],
    ] as const) {
      const expected = codeToTokens(code, { lang: "tsx", theme }).tokens.map((line) =>
        line.map((token) => ({
          content: token.content,
          color: token.color ?? null,
          fontStyle: token.fontStyle ?? null,
        })),
      );
      expect(highlightCode(code, "tsx", scheme)).toEqual(expected);
    }
  });

  it("draws dark punctuation at 4.5:1 on every dark code fence", () => {
    const channels = (hex: string) =>
      hex
        .slice(1)
        .match(/.{2}/g)!
        .map((channel) => Number.parseInt(channel, 16));
    const luminance = (rgb: ReadonlyArray<number>) => {
      const [r, g, b] = rgb.map((channel) => {
        const c = channel / 255;
        return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4;
      });
      return 0.2126 * r! + 0.7152 * g! + 0.0722 * b!;
    };
    const contrast = (a: ReadonlyArray<number>, b: ReadonlyArray<number>) =>
      (Math.max(luminance(a), luminance(b)) + 0.05) / (Math.min(luminance(a), luminance(b)) + 0.05);
    const fences = MOBILE_THEME_IDS.flatMap((themeId) => {
      const variables = getMobileThemeVariables(themeId, "dark");
      const [, ...overlay] = /^rgba\((\d+), (\d+), (\d+), ([\d.]+)\)$/
        .exec(variables["--color-md-user-fence-bg"])!
        .map(Number);
      const userFence = channels(variables["--color-user-bubble"]).map(
        (channel, index) => overlay[index]! * overlay[3]! + channel * (1 - overlay[3]!),
      );
      return [channels(variables["--color-md-code-bg"]), userFence];
    });
    const punctuation = highlightCode("int main() { return 0; }", "cpp", "dark")
      .flat()
      .flatMap(({ content, color }) =>
        [...content].filter((char) => "(){};".includes(char)).map((char) => ({ char, color })),
      );

    expect(punctuation.map(({ char }) => char).join("")).toBe("(){;}");
    for (const { color } of punctuation) {
      for (const fence of fences) {
        expect(contrast(channels(color!), fence)).toBeGreaterThanOrEqual(4.5);
      }
    }
  });

  it("colors a command's embedded script with that script's grammar", () => {
    const body = "import json\r\nprint(json.dumps({'ok': True}))";
    const code = `python3 - <<'PY'\r\n${body}\r\nPY`;
    const colors = (lines: HighlightedLines) =>
      lines.map((line) => line.map(({ content, color }) => [content, color]));
    const lines = highlightCommand(code, "shellscript", "dark");

    expect(lines.map((line) => line.map((token) => token.content).join(""))).toEqual(
      code.split("\r\n"),
    );
    expect(colors(lines.slice(1, 3))).toEqual(colors(highlightCode(body, "python", "dark")));
  });

  it("keeps an over-long line as one plain token", () => {
    const longLine = `const data = "${"x".repeat(1_000)}";`;
    const [first, second] = highlightCode(`let a = 1;\n${longLine}`, "typescript", "dark");

    expect(first?.length).toBeGreaterThan(1);
    expect(second).toEqual([{ content: longLine, color: null, fontStyle: null }]);
  });
});

it("normalizes only requested lines from full-source records, including CRLF and a long comment opener", () => {
  const code = `const first = 1;\r\n${" ".repeat(1_001)}/*\r\ninside\r\n*/\r\nconst last = 2;`;
  const source = tokenizeCode(code, "typescript");
  expect(source.byteLength).toBeGreaterThan(0);
  for (let line = 0; line < 5; line++) {
    expect(source.highlightLine(line, "dark")).toEqual(
      highlightCode(code, "typescript", "dark")[line],
    );
  }
  expect(source.highlightLine(2, "dark")?.[0]?.color).toBe("#737373");
  expect(source.highlightLine(100, "dark")).toBeUndefined();
});

const SAMPLES = [
  {
    name: "assembly",
    code: "mov eax, 42 ; answer\nret",
    hints: [
      "asm",
      "assembly",
      "s",
      ...["asm", "ASM", "s", "S", "nasm", "NASM"].map((ext) => `src/start.${ext}`),
    ],
  },
  {
    name: "verilog",
    code: "module top(input wire clk);\n  reg [7:0] q; // count\nendmodule",
    hints: ["verilog", "src/top.v", "src/defs.vh"],
  },
  {
    name: "systemverilog",
    code: "module top(input logic clk);\n  always_ff @(posedge clk) q <= 8'd1; // count\nendmodule",
    hints: ["systemverilog", "system-verilog", "src/top.sv", "src/defs.svh"],
  },
  {
    name: "vhdl",
    code: "entity top is\n  port (clk : in std_logic); -- clock\nend entity;",
    hints: ["vhdl", "src/top.vhd", "src/top.vhdl"],
  },
].flatMap(({ name, code, hints }) => hints.map((hint) => ({ name, code, hint })));

it.each(SAMPLES)("highlights $name in $hint", ({ code, hint }) => {
  const language = resolveHighlightLanguage(
    hint.includes("/") ? { path: hint } : { language: hint },
  );
  for (const scheme of ["light", "dark"] as const) {
    const tokens = highlightCode(code, language, scheme);
    expect(tokens.map((line) => line.map((token) => token.content).join("")).join("\n")).toBe(code);
    expect(new Set(tokens.flat().map((token) => token.color)).size).toBeGreaterThan(1);
  }
});
