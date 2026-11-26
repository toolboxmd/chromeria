// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { expect, it } from "@effect/vitest";

import { makeAcpMockRequestLogAppender } from "./acpMockRequestLog.ts";

it("logs a request split across stdin chunks as one complete line", () => {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-acp-mock-log-"));
  try {
    const path = NodePath.join(dir, "requests.ndjson");
    const append = makeAcpMockRequestLogAppender(path);
    const initialize = '{"id":0,"method":"initialize"}';
    const sessionNew = '{"id":1,"method":"session/new","params":{"cwd":"/w"}}';

    append(`${initialize}\n${sessionNew.slice(0, 20)}`);
    expect(NodeFS.readFileSync(path, "utf8")).toBe(`${initialize}\n`);

    append(`${sessionNew.slice(20)}\n`);
    expect(NodeFS.readFileSync(path, "utf8")).toBe(`${initialize}\n${sessionNew}\n`);
  } finally {
    NodeFS.rmSync(dir, { recursive: true, force: true });
  }
});
