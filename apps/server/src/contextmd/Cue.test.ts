// @effect-diagnostics nodeBuiltinImport:off globalTimers:off globalDate:off - the transport test runs a real local HTTP server and times a bounded wait.
import * as NodeHttp from "node:http";
import type * as NodeNet from "node:net";

import { assert, describe, it } from "@effect/vitest";

import {
  CueClient,
  httpTransport,
  transcriptItems,
  type CueRequest,
  type Transport,
} from "./Cue.ts";

/** A wait long enough for a fake transport that answers at once. */
const WAIT = 500;

const REQ: CueRequest = {
  items: [["user", "Open the PR"]],
  moment: { kind: "user_message" },
  entries: [{ id: "a", doc: "A: when" }],
};

const OK = JSON.stringify({
  adapter: "v1",
  threshold: 0.5,
  scores: { a: 1.25 },
  ms: 300,
  queue_ms: 0,
});

/** A transport that answers from a queue and records each call. */
const scripted = (
  answers: Array<Awaited<ReturnType<Transport>> | Promise<Awaited<ReturnType<Transport>>>>,
) => {
  const calls: Array<{ method: string; path: string; body: string | null }> = [];
  const transport: Transport = (method, path, body) => {
    calls.push({ method, path, body });
    const next = answers.shift();
    if (next === undefined) throw new Error("unexpected call");
    return Promise.resolve(next);
  };
  return { transport, calls };
};

describe("Cue client", () => {
  it("scores a message and returns the adapter, threshold and scores", async () => {
    const { transport, calls } = scripted([{ kind: "reply", status: 200, body: OK }]);
    const r = await new CueClient(transport).score(REQ, WAIT);
    assert.strictEqual(r.status, "ok");
    assert.deepStrictEqual([r.adapter, r.threshold, r.scores], ["v1", 0.5, { a: 1.25 }]);
    assert.deepStrictEqual(r.detail, { ms: 300, queue_ms: 0 });
    assert.strictEqual(calls[0]!.path, "/score");
    assert.deepStrictEqual(JSON.parse(calls[0]!.body!).entries, REQ.entries);
  });

  it("marks the service unavailable on a connect failure, skips it, and returns after a good probe", async () => {
    const { transport, calls } = scripted([
      { kind: "connect_failed" },
      { kind: "connect_failed" },
      { kind: "reply", status: 200, body: "{}" },
      { kind: "reply", status: 200, body: OK },
    ]);
    const c = new CueClient(transport);
    assert.strictEqual((await c.score(REQ, WAIT)).status, "connect_failed");
    assert.isFalse(c.isAvailable());
    assert.strictEqual((await c.score(REQ, WAIT)).status, "not_called");
    assert.strictEqual(c.start("k", REQ).status, "not_called");
    assert.strictEqual((await c.collect("k")).status, "not_called");
    assert.strictEqual((await c.collect("no-start-while-down")).status, "not_called");
    assert.strictEqual(calls.length, 1);
    assert.isFalse(await c.probe());
    assert.isTrue(await c.probe());
    assert.strictEqual((await c.score(REQ, WAIT)).status, "ok");
  });

  it("logs a slow or refused reply for that call only", async () => {
    const { transport } = scripted([
      { kind: "timeout" },
      { kind: "reply", status: 503, body: "{}" },
      { kind: "reply", status: 200, body: "not json" },
    ]);
    const c = new CueClient(transport);
    assert.strictEqual((await c.score(REQ, WAIT)).status, "timeout");
    assert.strictEqual((await c.score(REQ, WAIT)).status, "overload");
    assert.strictEqual((await c.score(REQ, WAIT)).status, "error");
    assert.isTrue(c.isAvailable());
  });

  it("logs a refused request as rejected with the service's reason, and keeps the service available", async () => {
    const { transport } = scripted([
      { kind: "reply", status: 400, body: JSON.stringify({ error: "NoMessage: no user message" }) },
      { kind: "reply", status: 200, body: OK },
    ]);
    const c = new CueClient(transport);
    const r = await c.score(REQ, WAIT);
    assert.strictEqual(r.status, "rejected");
    assert.deepStrictEqual(r.detail, { http: 400, error: "NoMessage: no user message" });
    assert.isTrue(c.isAvailable());
    assert.strictEqual((await c.score(REQ, WAIT)).status, "ok");
  });

  it("collects what a pre-tool moment started, once, and calls a missing start lost", async () => {
    let answer!: (r: Awaited<ReturnType<Transport>>) => void;
    const pending = new Promise<Awaited<ReturnType<Transport>>>((resolve) => (answer = resolve));
    const { transport } = scripted([pending]);
    const c = new CueClient(transport);
    assert.strictEqual(c.start("call-1", REQ).status, "started");
    // Still running when the tool result arrives: timeout for this collect, the request stays.
    assert.strictEqual((await c.collect("call-1", 20)).status, "timeout");
    answer({ kind: "reply", status: 200, body: OK });
    const r = await c.collect("call-1", 20);
    assert.strictEqual(r.status, "ok");
    assert.isAtMost(r.waitMs!, 20);
    assert.strictEqual((await c.collect("call-1", 20)).status, "lost");
    assert.strictEqual((await c.collect("never-started", 20)).status, "lost");
  });

  it("never rejects or throws when the transport does, so the server keeps running", async () => {
    const throwing: Transport = () => {
      throw new Error("boom");
    };
    const c = new CueClient(throwing);
    assert.strictEqual((await c.score(REQ, WAIT)).status, "error");
    assert.strictEqual(c.start("k", REQ).status, "started");
    assert.strictEqual((await c.collect("k", 50)).status, "error");
    assert.isTrue(await c.probe());
  });

  it("waits at most the given time for a score it needs now", async () => {
    const { transport } = scripted([new Promise(() => {})]);
    const t0 = Date.now();
    const r = await new CueClient(transport).score(REQ, 30);
    assert.strictEqual(r.status, "timeout");
    assert.isBelow(Date.now() - t0, 500);
  });
});

describe("Cue HTTP transport", () => {
  const listen = (handler: NodeHttp.RequestListener) =>
    new Promise<{ url: string; close: () => void }>((resolve) => {
      const server = NodeHttp.createServer(handler);
      server.listen(0, "127.0.0.1", () => {
        const { port } = server.address() as NodeNet.AddressInfo;
        resolve({ url: `http://127.0.0.1:${port}`, close: () => server.close() });
      });
    });

  it("tells a refused connection from a slow reply", async () => {
    const closed = await listen(() => {});
    closed.close();
    assert.deepStrictEqual(await httpTransport(closed.url)("GET", "/health", null, 1000), {
      kind: "connect_failed",
    });
    const slow = await listen(() => {});
    try {
      assert.deepStrictEqual(await httpTransport(slow.url)("GET", "/health", null, 100), {
        kind: "timeout",
      });
    } finally {
      slow.close();
    }
  });

  it("treats a URL that is not plain HTTP as unreachable instead of throwing", async () => {
    for (const base of ["https://127.0.0.1:1", "ftp://example", "not a url"]) {
      assert.deepStrictEqual(await httpTransport(base)("GET", "/health", null, 1000), {
        kind: "connect_failed",
      });
    }
  });

  it("posts the body and returns status and reply", async () => {
    const s = await listen((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (c: Buffer) => chunks.push(c));
      req.on("end", () => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ echo: Buffer.concat(chunks).toString("utf8"), path: req.url }));
      });
    });
    try {
      const r = await httpTransport(s.url)("POST", "/score", '{"x":1}', 1000);
      assert.strictEqual(r.kind, "reply");
      if (r.kind === "reply") {
        assert.strictEqual(r.status, 200);
        assert.deepStrictEqual(JSON.parse(r.body), { echo: '{"x":1}', path: "/score" });
      }
    } finally {
      s.close();
    }
  });
});

describe("transcript items", () => {
  it("keeps user and agent text and one short line per tool call, in order", () => {
    let ordinal = 0;
    const row = (type: string, payload: Record<string, unknown>) => ({
      ordinal: ++ordinal,
      type,
      payload_json: JSON.stringify(payload),
    });
    const items = transcriptItems([
      row("user_message", { text: "Fix the bug" }),
      row("assistant_message", { text: "   " }),
      row("command_execution", { input: "pytest -q" }),
      row("dynamic_tool", { toolName: "mcp__t3__task_status", input: { taskId: "t1" } }),
      row("file_change", { fileName: "/repo/a.ts", diffStr: "huge diff" }),
      row("assistant_message", { text: "Done." }),
      row("reasoning", { text: "hidden" }),
      { ordinal: 99, type: "user_message", payload_json: "{not json" },
    ]);
    assert.deepStrictEqual(items, [
      ["user", "Fix the bug"],
      ["tool", "command_execution: pytest -q"],
      ["tool", 'mcp__t3__task_status: {"taskId":"t1"}'],
      ["tool", "file_change: /repo/a.ts"],
      ["asst", "Done."],
    ]);
  });
});
