// @effect-diagnostics nodeBuiltinImport:off - isolated fixture files exercise the real Library loader.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as FileSystem from "effect/FileSystem";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { parse as yaml } from "yaml";

import corpus from "./library-corpus.json" with { type: "json" };
import { cueCandidate, select } from "./Select.ts";
import * as Store from "./Store.ts";

const temp = () =>
  NodeFS.realpathSync(NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "library-contract-")));
const emptyConfig = { exactKeys: [], cue: [], cueUrl: null, sessionStart: [] };
const fixture = (
  root: string,
  row: {
    dirs: ReadonlyArray<string>;
    files: Readonly<Record<string, string | undefined>>;
    links: Readonly<Record<string, string | undefined>>;
  },
) => {
  for (const directory of row.dirs)
    NodeFS.mkdirSync(NodePath.join(root, directory), { recursive: true });
  for (const [file, text] of Object.entries(row.files)) {
    assert.isDefined(text);
    NodeFS.mkdirSync(NodePath.dirname(NodePath.join(root, file)), { recursive: true });
    NodeFS.writeFileSync(NodePath.join(root, file), text!);
  }
  for (const [file, target] of Object.entries(row.links)) {
    assert.isDefined(target);
    NodeFS.mkdirSync(NodePath.dirname(NodePath.join(root, file)), { recursive: true });
    NodeFS.symlinkSync(NodePath.join(root, target!), NodePath.join(root, file), "dir");
  }
};

describe("Library entry contract", () => {
  it.each(corpus.entries)("$name", (row) => {
    const raw = row.raw_b64 ? Buffer.from(row.raw_b64, "base64") : Buffer.from(row.text!);
    const version = NodeCrypto.createHash("sha256").update(raw).digest("hex").slice(0, 12);
    const result = Store.parseBytes(row.file, raw, version, row.mode === "legacy");
    if (result instanceof Store.EntryError) {
      assert.deepStrictEqual(
        { code: result.code, line: result.line, field: result.field },
        row.error,
      );
    } else {
      assert.isUndefined(row.error);
      assert.deepStrictEqual(result.fields, row.fields);
      assert.deepStrictEqual(
        {
          id: result.id,
          body: result.body,
          long: result.long,
          exact: result.exact,
          cue: result.name !== null,
          legacy: result.legacy !== null,
        },
        row.expect,
      );
      assert.strictEqual(result.version, version);
    }
    if (row.yaml === "none" || row.yaml === "spec_invalid") return;
    const lines = row
      .text!.replace(/^\uFEFF/u, "")
      .replace(/\r\n/gu, "\n")
      .split("\n");
    const end = lines.indexOf("---", 1);
    const header = lines.slice(1, end).join("\n");
    const oracle = () =>
      yaml(header, { version: "1.2", schema: "core", uniqueKeys: true, strict: true });
    if (row.yaml === "invalid") assert.throws(oracle);
    else if (row.yaml === "same") assert.deepStrictEqual(oracle(), row.fields);
    else assert.doesNotThrow(oracle);
  });
});

describe("Library sources and cache", () => {
  it.effect("keeps duplicate precedence when a later file finishes first", () =>
    Effect.gen(function* () {
      const root = temp();
      yield* Effect.addFinalizer(() => Effect.sync(() => NodeFS.rmSync(root, { recursive: true })));
      const first = NodePath.join(root, "first"),
        second = NodePath.join(root, "second");
      NodeFS.mkdirSync(first);
      NodeFS.mkdirSync(second);
      const a = NodePath.join(first, "a.md"),
        z = NodePath.join(first, "z.md"),
        later = NodePath.join(second, "a.md");
      for (const [file, text, target] of [
        [a, "First knowledge.", "first-record"],
        [z, "Later name.", "ignored-record"],
        [later, "Later source.", "ignored-record"],
      ]) {
        NodeFS.writeFileSync(
          file!,
          `---\nid: shared\ncommand: x\nname: Shared\napplies: Public work.\ntarget: ${target}\n---\n${text}\n`,
        );
      }
      const fs = yield* FileSystem.FileSystem;
      const releaseFirst = yield* Deferred.make<void>();
      const targets: string[] = [];
      const library = yield* Store.load([first, second], new Map()).pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          readFile: (file) =>
            fs
              .readFile(file)
              .pipe(
                Effect.tap(() =>
                  file === a
                    ? Deferred.await(releaseFirst)
                    : file === z
                      ? Deferred.succeed(releaseFirst, undefined)
                      : Effect.void,
                ),
              ),
          realPath: (file) => {
            if (file.endsWith("-record")) targets.push(file);
            return fs.realPath(file);
          },
        }),
      );
      assert.deepEqual(
        library.entries.map((entry) => entry.text),
        ["First knowledge."],
      );
      assert.deepEqual(
        library.cue.map((entry) => entry.text),
        ["First knowledge."],
      );
      assert.deepEqual(library.errors, [
        `${z}: id 'shared' already used by ${a}`,
        `${later}: id 'shared' already used by ${a}`,
      ]);
      assert.deepEqual(targets, [NodePath.join(first, "first-record")]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("expands the joined checkout, Library and record from the injected home", () =>
    Effect.gen(function* () {
      const home = temp();
      yield* Effect.addFinalizer(() => Effect.sync(() => NodeFS.rmSync(home, { recursive: true })));
      NodeFS.mkdirSync(NodePath.join(home, "repo", ".git"), { recursive: true });
      NodeFS.mkdirSync(NodePath.join(home, "repo", "sub"));
      NodeFS.mkdirSync(NodePath.join(home, "repo", "library"));
      NodeFS.mkdirSync(NodePath.join(home, "records"));
      NodeFS.writeFileSync(NodePath.join(home, "records", "guide.md"), "Record.");
      NodeFS.writeFileSync(
        NodePath.join(home, "repo", "library", "rule.md"),
        '---\ncommand: x\ntarget: "~/records/guide.md"\n---\nKnowledge.',
      );
      const found = yield* Store.libraryDir("~/repo/sub").pipe(
        Effect.provideService(HostProcess.HomeDirectory, home),
      );
      assert.strictEqual(found, NodePath.join(home, "repo", "library"));
      const library = yield* Store.load(["~/repo/library"], new Map()).pipe(
        Effect.provideService(HostProcess.HomeDirectory, home),
      );
      assert.deepStrictEqual(library.errors, []);
      assert.strictEqual(library.entries[0]?.target, NodePath.join(home, "records", "guide.md"));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("reports an unreadable directory", () =>
    Effect.gen(function* () {
      if (process.getuid?.() === 0) return; // Root bypasses directory permissions.
      const d = temp(),
        lib = NodePath.join(d, "library");
      NodeFS.mkdirSync(lib);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          NodeFS.chmodSync(lib, 0o700);
          NodeFS.rmSync(d, { recursive: true });
        }),
      );
      NodeFS.chmodSync(lib, 0);
      const result = yield* Store.load([lib], new Map());
      assert.deepStrictEqual(result.entries, []);
      assert.deepStrictEqual(result.errors, [`${lib}: unreadable directory`]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect("refreshes a file alias's bytes when size and mtime stay equal", () =>
    Effect.gen(function* () {
      const d = temp(),
        lib = NodePath.join(d, "library"),
        records = NodePath.join(d, "records");
      yield* Effect.addFinalizer(() => Effect.sync(() => NodeFS.rmSync(d, { recursive: true })));
      NodeFS.mkdirSync(lib);
      NodeFS.mkdirSync(records);
      const a = NodePath.join(records, "a.md"),
        b = NodePath.join(records, "b.md"),
        alias = NodePath.join(lib, "alias.md");
      NodeFS.writeFileSync(a, "---\ncommand: x\n---\nBody.");
      NodeFS.writeFileSync(b, "---\ncommand: y\n---\nBody.");
      for (const file of [a, b]) NodeFS.utimesSync(file, 1_700_000_000, 1_700_000_000);
      NodeFS.symlinkSync(a, alias);
      const cache: Store.ParseCache = new Map();
      const before = (yield* Store.load([lib], cache)).entries[0]!;
      assert.strictEqual(before.id, "alias");
      assert.deepStrictEqual(before.commands, [["x"]]);
      NodeFS.unlinkSync(alias);
      NodeFS.symlinkSync(b, alias);
      const after = (yield* Store.load([lib], cache)).entries[0]!;
      assert.strictEqual(after.id, "alias");
      assert.deepStrictEqual(after.commands, [["y"]]);
      assert.notStrictEqual(after.version, before.version);
      assert.strictEqual(after.self, b);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect.each(corpus.stores)("$name", (row) =>
    Effect.gen(function* () {
      const d = temp();
      yield* Effect.addFinalizer(() => Effect.sync(() => NodeFS.rmSync(d, { recursive: true })));
      fixture(d, row);
      const library = yield* Store.load(
        row.sources.map((s) => ({ ...s, directory: NodePath.join(d, s.directory) })),
        new Map(),
      );
      assert.deepStrictEqual(
        {
          exact: library.entries.map((e) => e.id),
          cue: library.cue.map((e) => e.id),
          bodies: [...library.entries, ...library.cue].map((e) => e.text),
          errors: library.errors.map((e) => NodePath.relative(d, e.split(": ")[0]!)),
          legacy: library.legacy.length,
          owners: Object.keys(library.owners).map((p) => NodePath.relative(d, p)),
        },
        row.expect,
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.effect.each(corpus.discovery)("$name", (row) =>
    Effect.gen(function* () {
      const d = temp();
      yield* Effect.addFinalizer(() => Effect.sync(() => NodeFS.rmSync(d, { recursive: true })));
      fixture(d, row);
      const found = yield* Store.libraryDir(NodePath.join(d, row.thread));
      assert.strictEqual(found === null ? null : NodePath.relative(d, found), row.expect);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps parsing modes independent while refreshing symlink targets", () =>
    Effect.gen(function* () {
      const d = temp(),
        lib = NodePath.join(d, "library"),
        records = NodePath.join(d, "records");
      NodeFS.mkdirSync(lib);
      NodeFS.mkdirSync(records);
      NodeFS.symlinkSync(records, NodePath.join(lib, "docs"), "dir");
      const file = NodePath.join(lib, "a.md");
      NodeFS.writeFileSync(file, "---\ncommand: x\ncommand: y\ntarget: docs/record.md\n---\nBody.");
      const cache: Store.ParseCache = new Map();
      assert.strictEqual(
        (yield* Store.load([{ directory: lib, registered: false }], cache)).errors.length,
        1,
      );
      const before = yield* Store.load([lib], cache);
      assert.strictEqual(before.entries.length, 1);
      assert.deepStrictEqual(before.entries[0]!.commands, [["x"], ["y"]]);
      assert.strictEqual(before.entries[0]!.target, NodePath.join(lib, "docs", "record.md"));
      NodeFS.writeFileSync(NodePath.join(records, "record.md"), "Record.");
      assert.strictEqual(
        (yield* Store.load([lib], cache)).entries[0]!.target,
        NodePath.join(records, "record.md"),
      );
      assert.strictEqual(
        (yield* Store.load([{ directory: lib, registered: false }], cache)).errors.length,
        1,
      );
      const next = NodePath.join(d, "next");
      NodeFS.mkdirSync(next);
      NodeFS.writeFileSync(NodePath.join(next, "record.md"), "Next.");
      NodeFS.unlinkSync(NodePath.join(lib, "docs"));
      NodeFS.symlinkSync(next, NodePath.join(lib, "docs"), "dir");
      assert.strictEqual(
        (yield* Store.load([lib], cache)).entries[0]!.target,
        NodePath.join(next, "record.md"),
      );
      NodeFS.rmSync(d, { recursive: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "uses checkout and worktree boundaries, reads flat, and rejects a both-entry wholly",
    () =>
      Effect.gen(function* () {
        const d = temp(),
          a = NodePath.join(d, "a"),
          b = NodePath.join(d, "b");
        NodeFS.mkdirSync(NodePath.join(a, ".git"), { recursive: true });
        NodeFS.writeFileSync(NodePath.join(a, ".git", "HEAD"), "ref: refs/heads/main\n");
        const lib = NodePath.join(a, "library");
        NodeFS.mkdirSync(lib);
        const sub = NodePath.join(a, "sub");
        NodeFS.mkdirSync(sub);
        assert.strictEqual(yield* Store.libraryDir(sub), lib);
        NodeFS.mkdirSync(b);
        NodeFS.writeFileSync(NodePath.join(b, ".git"), "gitdir: /unused");
        const other = NodePath.join(b, "library");
        NodeFS.mkdirSync(other);
        assert.strictEqual(yield* Store.libraryDir(b), other);
        NodeFS.writeFileSync(
          NodePath.join(lib, "a.md"),
          "---\nid: same\ncommand: x\n---\nProject.",
        );
        NodeFS.writeFileSync(
          NodePath.join(other, "both.md"),
          "---\nid: same\ncommand: y\nname: Cue\napplies: When.\n---\nShared.",
        );
        NodeFS.mkdirSync(NodePath.join(lib, "nested"));
        NodeFS.writeFileSync(NodePath.join(lib, "nested", "bad.md"), "bad");
        const alias = NodePath.join(d, "alias");
        NodeFS.symlinkSync(lib, alias, "dir");
        const library = yield* Store.load(
          [{ directory: lib, registered: false }, alias, other],
          new Map(),
        );
        assert.deepStrictEqual(
          library.entries.map((e) => e.text),
          ["Project."],
        );
        assert.deepStrictEqual(library.cue, []);
        assert.strictEqual(library.errors.length, 1);
        assert.include(library.errors[0]!, "already used");
        NodeFS.rmSync(d, { recursive: true });
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps the known Unicode normalizer exception explicit (#69)", () =>
    Effect.gen(function* () {
      const d = temp();
      NodeFS.writeFileSync(NodePath.join(d, "e.md"), "---\nerror: Erreur ١٢, id 34\n---\nBody.");
      const library = yield* Store.load([d], new Map());
      assert.deepStrictEqual(library.entries[0]!.errors, ["erreur ١٢, id #"]);
      NodeFS.rmSync(d, { recursive: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe("Library rendering", () => {
  it.effect("keeps short knowledge whole while shrinking non-ASCII Cue display text", () =>
    Effect.gen(function* () {
      const d = temp(),
        body = "K".repeat(1100);
      NodeFS.writeFileSync(
        NodePath.join(d, "e.md"),
        `---\nname: ${"名".repeat(2000)}\napplies: ${"文".repeat(2000)}\ntarget: record.md\n---\n${body}`,
      );
      const library = yield* Store.load([d], new Map());
      const selection = yield* select(
        "claude",
        { hook_event_name: "Stop", session_id: "S" },
        emptyConfig,
        library,
      );
      const candidate = cueCandidate(library.cue[0]!, selection.profile);
      assert.include(candidate.text, body);
      assert.isAtMost(
        Buffer.byteLength(candidate.text) + Buffer.byteLength(selection.profile.heads.context) + 2,
        2000,
      );
      NodeFS.rmSync(d, { recursive: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "turns a long body with an authored record into a self-read and fits Grok's inline overflow",
    () =>
      Effect.gen(function* () {
        const d = temp(),
          file = NodePath.join(d, "e.md");
        const header = "---\ncommand: x\nmust-know-before: true\n";
        NodeFS.writeFileSync(file, header + "target: record.md\n---\n" + "K".repeat(1801));
        const event = {
          hook_event_name: "PreToolUse",
          session_id: "S",
          tool_name: "Bash",
          tool_input: { command: "x" },
        };
        const first = yield* select(
          "claude",
          event,
          emptyConfig,
          yield* Store.load([d], new Map()),
        );
        assert.strictEqual(first.candidates[0]!.rule, "entry");
        assert.strictEqual(first.candidates[0]!.target, file);
        assert.include(first.candidates[0]!.deny_text!, `Read ${file}`);
        assert.include(first.candidates[0]!.deny_text!, NodePath.join(d, "record.md"));
        NodeFS.writeFileSync(file, header + "---\n" + "K".repeat(500));
        const grok = yield* select(
          "grok",
          { ...event, hook_event_name: "pre_tool_use", tool_name: "run_terminal_command" },
          emptyConfig,
          yield* Store.load([d], new Map()),
        );
        assert.strictEqual(grok.candidates[0]!.rule, "entry");
        assert.include(grok.candidates[0]!.deny_text!, `Read ${file} in full`);
        assert.isAtMost(
          Buffer.byteLength(grok.candidates[0]!.deny_text!) +
            Buffer.byteLength(grok.profile.heads.deny) +
            2,
          256,
        );
        NodeFS.rmSync(d, { recursive: true });
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});
