// @effect-diagnostics nodeBuiltinImport:off - the tests write ContextMD stores and workspaces on disk.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { extract, firstMatch, globRegex, hasCommand, words } from "./Matcher.ts";
import { reads } from "./Reads.ts";
import { select, type ServingConfig } from "./Select.ts";
import * as Store from "./Store.ts";

const tmp = () =>
  NodeFS.realpathSync(NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "contextmd-")));

const entry = (
  dir: string,
  name: string,
  body: string,
  fields: Record<string, string | string[]>,
) => {
  const lines = ["---"];
  for (const [k, v] of Object.entries(fields)) {
    for (const value of Array.isArray(v) ? v : [v]) lines.push(`${k}: ${value}`);
  }
  lines.push("---", body, "");
  const file = NodePath.join(dir, `${name}.md`);
  NodeFS.writeFileSync(file, lines.join("\n"));
  return file;
};

// PR #23 SegmentsTest, unchanged cases.
describe("command keys", () => {
  const hit = (command: string, key = "gh pr create") =>
    firstMatch(
      { commands: [key.split(" ")], paths: [], errors: [] },
      extract("Bash", { command }, null, ["Bash"]),
      [],
    ) !== null;

  it("match the key's words anywhere, in order", () => {
    for (const command of [
      "gh pr create",
      "cd x && env A=1 /usr/local/bin/gh pr create --fill",
      "echo ok &&\ngh pr create",
      "printf '%s' '>'&gh pr create",
      "printf '%s' \\>&gh pr create",
      "printf '%s\\n' '<<EOF'\ngh pr create\nEOF",
      'bash -c "gh pr create"',
      'echo "$(gh pr create)"',
      "x=`gh pr create`",
      "{ gh pr create; }",
      "printf '%s' ';' gh pr create",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "gh pr list",
      "gh issue create",
      "ghx pr create",
      "echo gh pr",
      "gh pr createx",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("let the first word be an executable path", () => {
    assert.isTrue(hasCommand(words("/opt/bin/git push"), ["git", "push"]));
    assert.isFalse(hasCommand(words("git status push"), ["git", "push"]));
  });

  it("glob paths: ** spans directories, * stays in one part", () => {
    const rx = globRegex("**/SKILL.md");
    assert.isTrue(rx.test("SKILL.md"));
    assert.isTrue(rx.test("skills/ops/SKILL.md"));
    assert.isFalse(rx.test("skills/ops/SKILL.md.bak"));
    assert.isTrue(globRegex("docs/*.md").test("docs/a.md"));
    assert.isFalse(globRegex("docs/*.md").test("docs/x/a.md"));
  });

  it("refuse tool input they cannot read", () => {
    assert.throws(() => extract("Bash", "not an object", null, ["Bash"]));
    assert.throws(() => extract("mcp__x__y", { q: 1 }, null, ["Bash"]));
  });
});

// PR #23 StoreTest, plus pointer targets.
describe("store", () => {
  it.effect("reports bad entries instead of loading them", () =>
    Effect.gen(function* () {
      const d = tmp();
      entry(d, "good", "Use the template.", { command: "gh pr create" });
      entry(d, "nokey", "No key here.", {});
      entry(d, "big", "x".repeat(Store.MAX_ENTRY_BYTES + 1), { command: "ls" });
      entry(d, "long", "Rule.", { id: "x".repeat(121), command: "gh pr create" });
      entry(d, "tgt", "Read it.", { command: "git push", target: "x.md" });
      NodeFS.writeFileSync(NodePath.join(d, "README.md"), "not an entry");
      const st = yield* Store.load([d], new Map());
      assert.deepStrictEqual(
        st.entries.map((e) => e.id),
        ["good"],
      );
      assert.strictEqual(st.errors.length, 4);
      assert.isTrue(st.errors.some((e) => e.includes("id is 121 characters, over 120")));
      assert.isTrue(
        st.errors.some((e) => e.includes("target is only for must-know-before entries")),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("gives an edited entry a new version and reloads it from the cache", () =>
    Effect.gen(function* () {
      const d = tmp();
      const cache: Store.ParseCache = new Map();
      const file = entry(d, "a", "one", { command: "ls" });
      const v1 = (yield* Store.load([d], cache)).entries[0]!.version;
      assert.strictEqual(v1.length, 12);
      assert.strictEqual((yield* Store.load([d], cache)).entries[0]!.version, v1);
      entry(d, "a", "two!", { command: "ls" });
      NodeFS.utimesSync(file, 2_000_000_000, 2_000_000_000);
      assert.notStrictEqual((yield* Store.load([d], cache)).entries[0]!.version, v1);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("resolves a pointer target relative to its entry", () =>
    Effect.gen(function* () {
      const d = tmp();
      NodeFS.mkdirSync(NodePath.join(d, "procedures"));
      entry(d, "vc", "Read the procedure.", {
        command: "git push",
        "must-know-before": "true",
        target: "procedures/vc.md",
      });
      const e = (yield* Store.load([d], new Map())).entries[0]!;
      assert.strictEqual(e.target, NodePath.join(d, "procedures", "vc.md"));
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("records the commit the store directory is at", () =>
    Effect.gen(function* () {
      const d = tmp();
      NodeFS.mkdirSync(NodePath.join(d, ".git", "refs", "heads"), { recursive: true });
      NodeFS.writeFileSync(NodePath.join(d, ".git", "HEAD"), "ref: refs/heads/main\n");
      NodeFS.writeFileSync(NodePath.join(d, ".git", "refs", "heads", "main"), "feedface\n");
      entry(d, "a", "A.", { command: "ls" });
      assert.deepStrictEqual((yield* Store.load([d], new Map())).owners, { [d]: "feedface" });
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe("reads", () => {
  it.effect("count reader commands after cd, and Read-like tools, only for files that exist", () =>
    Effect.gen(function* () {
      const d = tmp();
      const sub = NodePath.join(d, "sub");
      NodeFS.mkdirSync(sub);
      const f = NodePath.join(sub, "a.md");
      NodeFS.writeFileSync(f, "x");
      const r = (tool: string, input: unknown) => reads(tool, input, d, ["Bash"]);
      assert.deepStrictEqual(yield* r("Bash", { command: `cd ${sub} && cat a.md` }), [
        { path: f, kind: "shell_full" },
      ]);
      assert.strictEqual(
        (yield* r("Bash", { command: `grep -n x ${f}` }))[0]!.kind,
        "shell_partial",
      );
      assert.deepStrictEqual(yield* r("Bash", { command: `ls ${f}` }), []);
      assert.deepStrictEqual(yield* r("Bash", { command: "cat missing.md" }), []);
      assert.deepStrictEqual(yield* r("Read", { file_path: f }), [{ path: f, kind: "read_tool" }]);
      assert.deepStrictEqual(yield* r("Edit", { file_path: f }), []);
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe("select, per harness (shapes captured inside Chromeria, 2026-10-12)", () => {
  const setup = () => {
    const d = tmp();
    const keys = NodePath.join(d, "keys");
    const ws = NodePath.join(d, "ws");
    NodeFS.mkdirSync(keys);
    NodeFS.mkdirSync(NodePath.join(ws, ".git"), { recursive: true });
    const target = NodePath.join(ws, "RULE.md");
    NodeFS.writeFileSync(
      target,
      "# Rules\n\nPushes need the deploy key alias.\nAsk before force-pushing a branch.\n",
    );
    entry(keys, "push", "A push rule applies.", {
      command: "git push",
      "must-know-before": "true",
      target,
    });
    entry(keys, "err", "Use the deploy key alias.", { error: "Permission denied (publickey)" });
    entry(keys, "skills", "SKILL.md edits need a pressure scenario.", { path: "**/SKILL.md" });
    const config: ServingConfig = { exactKeys: [keys], sessionStart: [] };
    const sel = (harness: string, raw: unknown) => select(harness, raw, config, new Map());
    return { ws, target, sel };
  };

  it.effect("reads Claude Code events: deny candidates, reads, path keys", () =>
    Effect.gen(function* () {
      const { ws, target, sel } = setup();
      const base = { session_id: "S", cwd: ws };
      const pre = yield* sel("claude", {
        ...base,
        hook_event_name: "PreToolUse",
        tool_name: "Bash",
        tool_input: { command: "git push" },
        tool_use_id: "toolu_1",
      });
      assert.deepStrictEqual([pre.moment.kind, pre.moment.tool_use_id], ["pre_tool", "toolu_1"]);
      assert.deepStrictEqual(
        pre.candidates.map((c) => [c.entry, c.rule]),
        [["push", "pointer"]],
      );
      assert.include(pre.candidates[0]!.deny_text!, `Read ${target}`);
      const read = yield* sel("claude", {
        ...base,
        hook_event_name: "PostToolUse",
        tool_name: "Read",
        tool_input: { file_path: target },
        tool_response: {},
      });
      assert.deepStrictEqual(read.moment.reads, [{ path: target, kind: "read_tool" }]);
      const edit = yield* sel("claude", {
        ...base,
        hook_event_name: "PostToolUse",
        tool_name: "Edit",
        tool_input: { file_path: NodePath.join(ws, "skills/ops/SKILL.md") },
      });
      assert.deepStrictEqual(
        edit.candidates.map((c) => c.entry),
        ["skills"],
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("treats a printed non-zero exit as a failure and matches error keys in output", () =>
    Effect.gen(function* () {
      const { ws, sel } = setup();
      const ok = {
        hook_event_name: "PostToolUse",
        session_id: "S",
        cwd: ws,
        tool_name: "Bash",
        tool_input: { command: 'git push; echo "exit=$?"' },
        tool_response: { stdout: "Permission denied (publickey).\nexit=128", stderr: "" },
      };
      const out = yield* sel("claude", ok);
      assert.deepStrictEqual(
        [out.moment.kind, out.moment.failed, out.moment.event],
        // The printed exit routes error keys; whether files were read stays the harness's report.
        ["tool_failure", false, "PostToolUse"],
      );
      assert.deepInclude(out.notes, { matcher: "failure", outcome: "failed_by_output" });
      assert.deepStrictEqual(
        out.candidates.map((c) => c.entry),
        ["err", "push"],
      );
      const clean = yield* sel("claude", {
        ...ok,
        tool_response: { stdout: "exit=0", stderr: "" },
      });
      assert.strictEqual(clean.moment.kind, "tool_result");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reads Codex output as the failure evidence it lacks an exit code for", () =>
    Effect.gen(function* () {
      const { ws, target, sel } = setup();
      const out = yield* sel("codex", {
        hook_event_name: "PostToolUse",
        session_id: "S",
        cwd: ws,
        tool_name: "Bash",
        tool_input: { command: `cat ${target}` },
        tool_response:
          "# Rules\n\nPushes need the deploy key alias.\nAsk before force-pushing a branch.\n",
        tool_use_id: "exec-1",
      });
      assert.strictEqual(out.moment.failed, null);
      assert.deepStrictEqual(out.moment.reads, [{ path: target, kind: "shell_full" }]);
      // Codex reports no failure, so selection only names the files; the serving state waits
      // for the projected exit status before counting them (ServingState and route tests).
      const nl = yield* sel("codex", {
        hook_event_name: "PostToolUse",
        session_id: "S",
        cwd: ws,
        tool_name: "Bash",
        tool_input: { command: `nl -ba ${target}` },
        tool_response: "     1\t# Rules\n",
      });
      assert.deepStrictEqual(nl.moment.reads, [{ path: target, kind: "shell_full" }]);
      const fail = yield* sel("codex", {
        hook_event_name: "PostToolUse",
        session_id: "S",
        cwd: ws,
        tool_name: "Bash",
        tool_input: { command: "ssh host" },
        tool_response: "git@github.com: Permission denied (publickey).\n",
      });
      assert.deepStrictEqual(
        fail.candidates.map((c) => c.entry),
        ["err"],
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reads Grok's snake_case events, exit code and compaction", () =>
    Effect.gen(function* () {
      const { ws, target, sel } = setup();
      const base = { session_id: "S", cwd: ws };
      const failure = yield* sel("grok", {
        ...base,
        hook_event_name: "post_tool_use",
        tool_name: "run_terminal_command",
        tool_input: { command: "ls /nope" },
        toolResult: { exit_code: 1, output_for_prompt: "exit: 1\nls: /nope" },
      });
      assert.strictEqual(failure.moment.kind, "tool_failure");
      assert.strictEqual(failure.profile.carry_to, "pre_tool");
      const read = yield* sel("grok", {
        ...base,
        hook_event_name: "post_tool_use",
        tool_name: "read_file",
        tool_input: { target_file: "RULE.md" },
        toolResult: { type: "ReadFile" },
      });
      assert.deepStrictEqual(read.moment.reads, [{ path: target, kind: "read_tool" }]);
      const compact = yield* sel("grok", { ...base, hook_event_name: "post_compact" });
      assert.strictEqual(compact.moment.kind, "compaction");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reads OpenCode plugin events", () =>
    Effect.gen(function* () {
      const { ws, sel } = setup();
      const before = yield* sel("opencode", {
        hook: "tool.execute.before",
        directory: ws,
        input: { tool: "bash", sessionID: "ses_1", callID: "c1" },
        output: { args: { command: "git push" } },
      });
      assert.deepStrictEqual([before.moment.kind, before.moment.session], ["pre_tool", "ses_1"]);
      assert.deepStrictEqual(
        before.candidates.map((c) => c.entry),
        ["push"],
      );
      const after = yield* sel("opencode", {
        hook: "tool.execute.after",
        directory: ws,
        input: { tool: "bash", sessionID: "ses_1", callID: "c1", args: { command: "false" } },
        output: { metadata: { exit: 1 }, output: "" },
      });
      assert.strictEqual(after.moment.kind, "tool_failure");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("says in every served head that the rule is the user's own", () =>
    Effect.gen(function* () {
      const { ws, sel } = setup();
      const out = yield* sel("claude", { hook_event_name: "Stop", session_id: "S", cwd: ws });
      assert.include(out.profile.heads.deny, "the user's own rule store");
      assert.include(out.profile.heads.context, "rule from the user");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("refuses unknown harnesses and events", () =>
    Effect.gen(function* () {
      const config: ServingConfig = { exactKeys: [], sessionStart: [] };
      const refused = (harness: string, raw: unknown) =>
        select(harness, raw, config, new Map()).pipe(
          Effect.flip,
          Effect.map((e) => e.reason),
        );
      assert.strictEqual(
        yield* refused("nope", { hook_event_name: "PreToolUse", session_id: "S" }),
        "unknown harness nope",
      );
      assert.include(
        yield* refused("claude", { hook_event_name: "pre_tool_use", session_id: "S" }),
        "unsupported event",
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});
