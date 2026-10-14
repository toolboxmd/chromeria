// @effect-diagnostics nodeBuiltinImport:off - the tests write ContextMD stores and workspaces on disk.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { extract, firstMatch, globRegex, hasCommand, normalizeError, words } from "./Matcher.ts";
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

  it("skip a key only mentioned in quotes or a heredoc, but not commands those run (#232)", () => {
    for (const command of [
      "zsh -lc 'gh pr create --fill'",
      '/bin/bash -c "cd x && gh pr create"',
      "gh issue view 5 --json body -q .body; gh pr create",
      "cat > notes.md <<'EOF'\nhello\nEOF\ngh pr create",
      "gh pr create --title \"x\" --body-file - <<'EOF'\nbody\nEOF",
    ]) {
      assert.isTrue(hit(command), command);
    }
    // Mentions logged on rocky (#232): strings, heredoc bodies, other programs' code.
    for (const command of [
      'echo "live check: gh pr create"',
      "echo 'gh pr create'",
      "cat <<'EOF'\nthen run gh pr create\nEOF",
      "python3 - <<'PY'\nkeys = ['gh pr create']\nPY",
      "python3 -c \"print('gh pr create')\"",
      'gh issue create --title t --body "open it with gh pr create"',
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("follows what bash runs: expanding heredocs, quoted command words, escapes (Sol review)", () => {
    // Checked with /bin/bash and a stub gh that prints EXECUTED.
    for (const command of [
      "cat <<EOF\n$(gh pr create)\nEOF",
      "cat <<EOF\n`gh pr create`\nEOF",
      "bash -c \"'gh' pr create\"",
      "'gh' pr create",
      '"gh" pr create --fill',
      "g'h' pr create",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "cat <<'EOF'\n$(gh pr create)\nEOF",
      'cat <<"EOF"\n$(gh pr create)\nEOF',
      'echo "\\$(gh pr create)"',
      'echo "\\`gh pr create\\`"',
      "cat <<EOF\nrun gh pr create later\nEOF",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("keeps inner quotes and nested substitution bodies (Sol review)", () => {
    for (const command of [
      'echo "$(gh pr create --title "hello")"',
      'echo "$(gh pr create $(printf x))"',
      'echo "$(gh pr create "$(printf x)")"',
      'echo $(gh pr create --title "hello")',
      'echo "`gh pr create --title "hello"`"',
      'echo "`printf "%s" \\`gh pr create\\``"',
      'cat <<EOF\n$(gh pr create --title "hello" $(printf x))\nEOF',
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "echo '$(gh pr create)'",
      'echo "$(printf "%s" "gh pr create")"',
      'echo "`printf "%s" "gh pr create"`"',
      'echo "$(printf "%s" "$(printf "%s" "gh pr create")")"',
      "cat <<'EOF'\n$(gh pr create --title \"hello\" $(printf x))\nEOF",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("uses even and odd backslash parity for substitutions (Sol review)", () => {
    // Two/four literal backslashes run; one/three leave the operator literal.
    for (const command of [
      'echo "\\\\$(gh pr create)"',
      'echo "\\\\\\\\$(gh pr create)"',
      "cat <<EOF\n\\\\$(gh pr create)\nEOF",
      "cat <<EOF\n\\\\\\\\$(gh pr create)\nEOF",
      'echo "\\\\`gh pr create`"',
      "cat <<EOF\n\\\\`gh pr create`\nEOF",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      'echo "\\$(gh pr create)"',
      'echo "\\\\\\$(gh pr create)"',
      "cat <<EOF\n\\$(gh pr create)\nEOF",
      "cat <<EOF\n\\\\\\$(gh pr create)\nEOF",
      'echo "\\`gh pr create\\`"',
      "cat <<EOF\n\\`gh pr create\\`\nEOF",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("ends a heredoc only on its exact delimiter, stripping tabs only for <<- (Sol review)", () => {
    for (const command of [
      "cat <<'EOF'\nEOF\ngh pr create",
      "cat <<EOF\nEOF\ngh pr create",
      "cat <<-'EOF'\n\tEOF\ngh pr create",
      "cat <<-EOF\n\tEOF\ngh pr create",
      "cat <<A <<'B'\nA\nB\ngh pr create",
      "cat <<'A'\nA\ncat <<'B'\nB\ngh pr create",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "cat <<'EOF'\n EOF\ngh pr create\nEOF",
      "cat <<'EOF'\n\tEOF\ngh pr create\nEOF",
      "cat <<'EOF'\nEOF \ngh pr create\nEOF",
      "cat <<'EOF'\nEOF\t\ngh pr create\nEOF",
      "cat <<-'EOF'\n EOF\ngh pr create\nEOF",
      "cat <<-'EOF'\n\tEOF \ngh pr create\nEOF",
      "cat <<'EOF'\nEOFX\ngh pr create\nEOF",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("keeps here-strings and arithmetic or parameter expansion boundaries distinct", () => {
    for (const command of [
      "cat <<<EOF\ngh pr create",
      'echo "$(gh pr create $((1 + (2))))"',
      'echo "$(gh pr create ${v:-")"})"',
      'echo "$((1 + $(gh pr create)0))"',
      'echo "${v:-$(gh pr create --title "hello")}"',
      "cat <<EOF\n$((1 + $(gh pr create)0))\nEOF",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      'cat <<<"gh pr create"',
      'echo "$((1 + (2))) gh pr create"',
      'echo "${v:-gh pr create}"',
      'echo "$(printf "%s" "${v:-gh pr create}")"',
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("keeps quoted commands after assignments and wrappers (Sol round 3)", () => {
    for (const command of [
      "FLAG=1 'gh' pr create",
      "FLAG=1 g'h' pr create",
      "FLAG='one two' OTHER=2 \"gh\" pr create",
      "env 'gh' pr create",
      "env g'h' pr create",
      "env FLAG=1 'gh' pr create",
      "env -u UNUSED 'gh' pr create",
      "sudo -n 'gh' pr create",
      "sudo -u nobody g'h' pr create",
      "time 'gh' pr create",
      "nohup 'gh' pr create",
      "command -- 'gh' pr create",
      "exec g'h' pr create",
      "exec -a gh-alias 'gh' pr create",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "FLAG=1 printf '%s' 'gh pr create'",
      "env printf '%s' 'gh pr create'",
      "env -u UNUSED printf '%s' 'gh pr create'",
      "sudo -n printf '%s' 'gh pr create'",
      "command printf '%s' 'gh pr create'",
      "exec printf '%s' 'gh pr create'",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("dequotes a whole shell -c argument before scanning its script (Sol round 3)", () => {
    for (const command of [
      'bash -c "\\"g\\"h pr create"',
      'sh -c "\\"g\\"h pr create"',
      'zsh -lc "\\"g\\"h pr create"',
      "bash -c \"g\"'h pr create'",
      'bash -c "\\"g\\"h"\' pr create\'',
      'env FLAG=1 bash -c "\\"g\\"h pr create"',
      "bash -c 'gh pr create'",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      'bash -c "echo \\"gh pr create\\""',
      'sh -c "printf \'%s\' \\"gh pr create\\""',
      'zsh -lc "echo \\"gh pr create\\""',
      'bash -c \'echo \'"\\"gh pr create\\""',
      'bash -c "echo \\"gh pr create\\""suffix',
      "bash -c 'echo \"gh pr create\"'",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("uses ASCII marker boundaries: Unicode and punctuation literal heredocs never match", () => {
    for (const command of [
      "cat <<'ÉOF'\nÉOF\ngh pr create",
      "cat <<'EÖF'\nEÖF\ngh pr create",
      "cat <<'𝔈OF'\n𝔈OF\ngh pr create",
      "cat <<ÉOF\n$(gh pr create)\nÉOF",
      "cat <<EÖF\n$(gh pr create)\nEÖF",
      "cat <<𝔈OF\n$(gh pr create)\n𝔈OF",
      "cat <<'END-MARK'\nEND-MARK\ngh pr create",
      "cat <<'EOF'\nEOF\ngh pr create",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "cat <<'ÉOF'\ngh pr create\nÉOF",
      "cat <<'EÖF'\ngh pr create\nEÖF",
      "cat <<'𝔈OF'\ngh pr create\n𝔈OF",
      "cat <<ÉOF\ngh pr create\nÉOF",
      "cat <<EÖF\ngh pr create\nEÖF",
      "cat <<𝔈OF\ngh pr create\n𝔈OF",
      'cat <<"EÖF"\n$(gh pr create)\nEÖF',
      "cat <<'END-MARK'\ngh pr create\nEND-MARK",
      "cat <<'END.MARK'\nENDXMARK\ngh pr create\nEND.MARK",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("recognizes clustered shell c flags and options before the script (Grok round 3)", () => {
    for (const command of [
      "bash -ce 'gh pr create'",
      "bash -cE 'gh pr create'",
      "bash -ec 'gh pr create'",
      "bash -o pipefail -c 'gh pr create'",
      "bash --noprofile -o pipefail -ce '\"g\"h pr create'",
      "env FLAG=1 bash -o pipefail -ce 'gh pr create'",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "bash -ce 'echo \"gh pr create\"'",
      "bash -o pipefail -c 'echo \"gh pr create\"'",
      "bash -o pipefail -ce 'echo \"gh pr create\"'",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("decodes ANSI-C quoted shell scripts while excluding quoted mentions (Grok round 3)", () => {
    for (const command of [
      "bash -c $'gh pr create'",
      "bash -c $'gh pr create\\0; echo mention'",
      "bash -c $'\\x67h pr create'",
      "bash -c $'\\147h pr create'",
      "bash -c $'\\u0067h pr create'",
      "bash -c $'\\x27gh\\x27 pr create'",
      "bash -c $'gh\\x20pr\\tcreate'",
      "sh -c $'\\x67h pr create'",
      "bash -c $'g'\"h pr create\"",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "bash -c $'echo \\\"gh pr create\\\"'",
      "bash -c $'printf \\'%s\\' \\'gh pr create\\''",
      "bash -c $'echo \\x22gh pr create\\x22'",
      "bash -c $'echo \\047gh pr create\\047'",
      "echo $'gh pr create'",
      "bash -c $'echo \\0; gh pr create'",
      "bash -c $'echo \"$(printf \\'%s\\' \\'gh pr create\\')\"'",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("keep quoted single tokens in every word position and drop quoted prose (#232)", () => {
    // Single quoted tokens count as arguments; this does not claim they execute gh.
    for (const command of [
      "printf '%s ' 'gh' 'pr' 'create'",
      'echo "gh" pr create',
      "echo g'h' pr create",
      'echo g"h" pr create',
      "echo $'\\x67h' pr create",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "printf '%s ' 'gh pr create'",
      'echo "live check: gh pr create"',
      "echo $'gh pr create'",
      "echo g'h pr create'",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("preserve shell commands across numbered redirections (Sol round 4)", () => {
    for (const command of [
      "2>/dev/null 'gh' pr create",
      "2>/dev/null bash -c 'gh pr create'",
      "2>&1 env 'gh' pr create",
      ">/dev/null 'gh' pr create",
      "2>>/dev/null g'h' pr create",
      "2>&1 bash -ce 'gh pr create'",
      "3<<'EOF' bash -c 'gh pr create'\nliteral gh pr create\nEOF",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "2>/dev/null printf '%s' 'gh pr create'",
      "2>/dev/null bash -c 'echo \"gh pr create\"'",
      "2>&1 env printf '%s' 'gh pr create'",
      "3<<'EOF' cat\nliteral gh pr create\nEOF",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("keep quoted words after shell control syntax (Sol round 4)", () => {
    for (const command of [
      "if true; then 'gh' pr create; fi",
      "! 'gh' pr create",
      "{ 'gh' pr create; }",
      "if false; then :; else g'h' pr create; fi",
      'for n in 1; do "gh" pr create; done',
      "time -p 'gh' pr create",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "if true; then printf '%s' 'gh pr create'; fi",
      "! printf '%s' 'gh pr create'",
      "{ printf '%s' 'gh pr create'; }",
      "for n in 1; do printf '%s' 'gh pr create'; done",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("stop shell option parsing after consuming the one -c script (Sol round 4)", () => {
    for (const command of [
      "bash -c 'gh pr create' _ -c 'echo \"gh pr create\"'",
      "bash -c 'echo done' _ -c \"$(gh pr create)\"",
      "sh -c 'gh pr create' -c 'echo \"gh pr create\"'",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "bash -c 'echo \"$@\"' _ -c 'gh pr create'",
      "bash -c 'echo done' -- -c 'gh pr create'",
      "bash -c 'echo done' -c 'gh pr create'",
      "bash -ce 'echo done' _ -ce 'gh pr create'",
      "sh -c 'echo done' _ -c 'gh pr create'",
      "zsh -lc 'echo done' _ -c 'gh pr create'",
      "bash -c $'echo done' _ -c $'gh pr create'",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("match quoted single database tokens and paths, but drop prose (#232, contextmd#39)", () => {
    // Single quoted tokens count as arguments; this does not claim they execute gh.
    for (const command of [
      "sqlite3 'chromeria-v2.sqlite' .tables",
      'sqlite3 "/Users/u/.t3/userdata/chromeria-v2.sqlite" .tables',
      "sqlite3 $'chromeria-v2.sqlite' .tables",
      "sqlite3 chromeria-v2'.sqlite' .tables",
    ]) {
      assert.isTrue(hit(command, "chromeria-v2.sqlite"), command);
    }
    for (const command of [
      "echo 'the file chromeria-v2.sqlite'",
      'echo "open /Users/u/.t3/userdata/chromeria-v2.sqlite later"',
      "echo $'the file chromeria-v2.sqlite'",
      "echo prefix' the file chromeria-v2.sqlite'",
    ]) {
      assert.isFalse(hit(command, "chromeria-v2.sqlite"), command);
    }
  });

  it("recognize shell scripts after control syntax and command-running tools (Sol round 5)", () => {
    for (const command of [
      "if true; then bash -c 'gh pr create'; fi",
      "! bash -c 'gh pr create'",
      "{ bash -c 'gh pr create'; }",
      "for i in 1; do bash -c 'gh pr create'; done",
      "xargs bash -c 'gh pr create'",
      "find . -exec bash -c 'gh pr create' \\;",
      "timeout 5 bash -c 'gh pr create'",
      "ssh host bash -c 'gh pr create'",
      "xargs bash -o pipefail -ce 'gh pr create'",
      "env ksh -c 'gh pr create'",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "if true; then bash -c 'echo \"gh pr create\"'; fi",
      "! bash -c 'echo \"gh pr create\"'",
      "{ bash -c 'echo \"gh pr create\"'; }",
      "for i in 1; do bash -c 'echo \"gh pr create\"'; done",
      "xargs bash -c 'echo \"gh pr create\"'",
      "find . -exec bash -c 'echo \"gh pr create\"' \\;",
      "timeout 5 bash -c 'echo \"gh pr create\"'",
      "ssh host bash -c 'echo \"gh pr create\"'",
      "env ksh -c 'echo \"gh pr create\"'",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("count printed shell invocations in the accepted safe direction (#232)", () => {
    // Printed shell invocations match by policy; these cases do not execute gh.
    for (const command of [
      "echo bash -c 'gh pr create'",
      "echo sh -c 'gh pr create'",
      "echo \"bash\" -ce 'gh pr create'",
      "echo b'a'sh -o pipefail -c 'gh pr create'",
      "echo ksh -c 'gh pr create'",
    ]) {
      assert.isTrue(hit(command), command);
    }
    for (const command of [
      "echo bash -c 'echo \"gh pr create\"'",
      "echo 'bash -c gh pr create'",
      "echo bash -c 'echo done' _ -c 'gh pr create'",
    ]) {
      assert.isFalse(hit(command), command);
    }
  });

  it("uses identical ASCII whitespace for all six divergent code points (Sol round 6)", () => {
    for (const [sep, encoded] of [
      ["\u0085", "\\u0085"],
      ["\u001c", "\\u001c"],
      ["\u001d", "\\u001d"],
      ["\u001e", "\\u001e"],
      ["\u001f", "\\u001f"],
      ["\ufeff", "\\ufeff"],
    ]) {
      const token = `gh${sep}pr${sep}create`;
      for (const command of [`echo ${token}`, `echo '${token}'`]) {
        assert.deepStrictEqual(extract("Bash", { command }, null, ["Bash"]).words, ["echo", token]);
        assert.isFalse(hit(command), encoded);
      }
      assert.deepStrictEqual(
        extract("Bash", { command: `printf '%s' '${token}'` }, null, ["Bash"]).words,
        ["printf", "%s", token],
      );
      assert.deepStrictEqual(
        extract("Bash", { command: `bash -c $'echo gh${encoded}pr${encoded}create'` }, null, [
          "Bash",
        ]).words,
        ["bash", "-c", "echo", token],
      );
      const path = `${sep}/Users/u/.t3/userdata/chromeria-v2.sqlite`;
      const command = `sqlite3 '${path}'`;
      const keys = extract("Bash", { command }, null, ["Bash"]);
      assert.deepStrictEqual(keys.words, ["sqlite3", path]);
      assert.deepStrictEqual(keys.paths, [path]);
      assert.isTrue(hit(command, "chromeria-v2.sqlite"), encoded);
      assert.strictEqual(normalizeError(`${sep}GH${sep}12${sep}`), `${sep}gh${sep}#${sep}`);
    }
    assert.deepStrictEqual(words("gh \t\n\r\v\fpr create"), ["gh", "pr", "create"]);
    assert.isFalse(hit("echo 'gh \t\n\r\v\fpr create'"));
    assert.strictEqual(normalizeError(" \t\n\r\v\fGH 12 \t\n\r\v\f"), "gh #");
  });

  it("keeps the first bake's bare key on single-token arguments (#232, contextmd#39)", () => {
    const db = (command: string) => hit(command, "chromeria-v2.sqlite");
    for (const command of [
      "sqlite3 chromeria-v2.sqlite .tables",
      "sqlite3 ~/.t3/userdata/chromeria-v2.sqlite",
      "sqlite3 /Users/u/.t3/userdata/chromeria-v2.sqlite",
      "sqlite3 -readonly ~/.t3/userdata/chromeria-v2.sqlite .tables",
      "sqlite3 /Users/u/.t3/userdata/chromeria-v2.sqlite 'select 1'",
    ]) {
      assert.isTrue(db(command), command);
    }
    // A quoted path without whitespace is a real file argument and matches the bare key.
    assert.isTrue(db('sqlite3 "/Users/u/.t3/userdata/chromeria-v2.sqlite" .tables'));
    // Neither URI hits the bare command key: the basename includes ?mode=ro.
    for (const command of [
      "sqlite3 'file:/Users/u/.t3/userdata/chromeria-v2.sqlite?mode=ro'",
      "sqlite3 file:/Users/u/.t3/userdata/chromeria-v2.sqlite?mode=ro",
    ]) {
      assert.isFalse(db(command), command);
    }
    // Live bake mentions: candidate 1672 edits a test; 26379 writes an Issue body.
    for (const command of [
      "python3 - <<'EOF'\nfrom pathlib import Path\np = Path('tests/test_serve.py')\np.write_text(p.read_text().replace('state.sqlite', 'chromeria-v2.sqlite'))\nEOF",
      "gh issue create --title \"Record the bake\" --body-file - <<'EOF'\nThe entry names chromeria-v2.sqlite in a test fixture.\nEOF",
      'gh issue create --title "Record the bake" --body "The entry names chromeria-v2.sqlite in a test fixture."',
    ]) {
      assert.isFalse(db(command), command);
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

  it.effect("call a read full only when it can show the whole file", () =>
    Effect.gen(function* () {
      const d = tmp();
      const f = NodePath.join(d, "a.md");
      NodeFS.writeFileSync(f, "x\n");
      NodeFS.mkdirSync(NodePath.join(d, "x")); // so x/../a.md exists
      NodeFS.writeFileSync(NodePath.join(d, "a[.]md"), "y\n"); // a glob could expand elsewhere
      const kind = (tool: string, input: unknown) =>
        reads(tool, input, d, ["Bash"]).pipe(Effect.map((rs) => rs.map((x) => x.kind)));
      for (const command of [`cat ${f}`, `nl -ba ${f}`, `bat ${f}`, `less ${f}`]) {
        assert.deepStrictEqual(yield* kind("Bash", { command }), ["shell_full"], command);
      }
      for (const command of [
        `grep -n x ${f}`,
        `rg x ${f}`,
        `head -n 40 ${f}`,
        `/usr/bin/head -n 5 ${f}`,
        `"${NodePath.relative(d, "/usr/bin/head")}" -n 5 ${f}`,
        `tail -n 20 ${f}`,
        `sed -n '1,200p' ${f}`,
        `awk 'NR<10' ${f}`,
        `cat ${f} | head -n 5`,
      ]) {
        assert.deepStrictEqual(yield* kind("Bash", { command }), ["shell_partial"], command);
      }
      assert.deepStrictEqual(yield* kind("Read", { file_path: f }), ["read_tool"]);
      assert.deepStrictEqual(yield* kind("Read", { file_path: f, offset: null, limit: null }), [
        "read_tool",
      ]);
      assert.deepStrictEqual(yield* kind("Read", { file_path: f, offset: 10 }), ["read_partial"]);
      assert.deepStrictEqual(yield* kind("Read", { file_path: f, limit: 50 }), ["read_partial"]);
      assert.deepStrictEqual(yield* kind("Read", { file_path: f, pages: "1" }), ["read_partial"]);
      // Only file-reading tools, and only their file field, count.
      assert.deepStrictEqual(
        yield* kind("mcp__t3_code__t3_thread_update", { action: "rename", title: f }),
        [],
      );
      assert.deepStrictEqual(yield* kind("Read", { file_path: "missing.md", note: f }), []);
      assert.deepStrictEqual(yield* kind("read", { filePath: `${d}/x/../a.md` }), []);
      assert.deepStrictEqual(yield* kind("read", { filePath: f, offset: 1, limit: 20 }), [
        "read_partial",
      ]);
      // Range options, output that never reaches the agent, and paths that are not operands.
      for (const command of [
        `bat -r 1:5 ${f}`,
        `batcat --line-range 1:5 ${f}`,
        `less +40 ${f}`,
        `cat ${f} > /dev/null`,
        `cat ${f} >&2`,
        `cat <<< ${f}`,
        `bat -r1:5 ${f}`,
        `batcat -pr1:5 ${f}`,
        `echo 'ignore; cat ${f} ; ignore'`,
        `echo "$(true; cat ${f} ; true)" > /dev/null`,
        `false && cat ${f}; true`,
        `cat /dev/null 2< ${f}`,
        `cat /dev/null 2> ${f}`,
        `cat ${f} &`,
        `cat ${f} missing.md`,
        `cat "${f}"-n`,
        `cat '${f}'`,
        `cat ${d}/a[.]md`, // the glob matches a.md, not this file
        `cat ${d}/x/../a.md`,
      ]) {
        assert.deepStrictEqual(yield* kind("Bash", { command }), ["shell_partial"], command);
      }
      for (const command of [`echo ${f} && cat missing.md`, `ls ${f}`, `cd - && cat a.md`]) {
        assert.deepStrictEqual(yield* kind("Bash", { command }), [], command);
      }
      for (const command of [
        `/bin/cat ${f} 2>/dev/null`,
        `${NodePath.relative(d, "/bin/cat")} ${f}`,
        `cat -n ${f}`,
        `cd ${d} && cat a.md`,
        `nl -ba ${f}`,
        `bat --plain ${f}`,
      ]) {
        assert.deepStrictEqual(yield* kind("Bash", { command }), ["shell_full"], command);
      }
      // A full read inside a longer command is not a plain read: logged, never lifting.
      assert.deepStrictEqual(yield* kind("Bash", { command: `grep -n x ${f}; cat ${f}` }), [
        "shell_partial",
      ]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("call a read partial when its result shows it was cut", () =>
    Effect.gen(function* () {
      const d = tmp();
      const f = NodePath.join(d, "a.md");
      NodeFS.writeFileSync(f, "x\n");
      const kind = (tool: string, input: unknown, response: unknown) =>
        reads(tool, input, d, ["Bash"], response).pipe(Effect.map((rs) => rs.map((x) => x.kind)));
      const cat = { command: `cat ${f}` };
      assert.deepStrictEqual(yield* kind("Bash", cat, "x\n"), ["shell_full"]);
      for (const output of [
        "Total output lines: 900\n[... omitted 400 of 900 lines ...]",
        "x\n…2048 tokens truncated…",
        "x … [+54 chars]",
        "x\n... [12045 characters truncated] ...\ny",
        "x\n…812 chars truncated…\ny",
        "x [... truncated (90000 chars total)]",
        "[Output truncated - 120000 bytes total]",
        "x\n... (output truncated) ...",
        { stdout: "x", stderr: "", note: "Output truncated" },
      ]) {
        assert.deepStrictEqual(yield* kind("Bash", cat, output), ["shell_partial"], String(output));
      }
      const file = (numLines: number, startLine: number, totalLines: number) => ({
        type: "text",
        file: { filePath: f, content: "x", numLines, startLine, totalLines },
      });
      assert.deepStrictEqual(yield* kind("Read", { file_path: f }, file(10, 1, 10)), ["read_tool"]);
      assert.deepStrictEqual(
        yield* kind(
          "Read",
          { file_path: f },
          {
            type: "text",
            file: {
              filePath: f,
              content: "x",
              numLines: 1,
              startLine: 1,
              totalLines: 1,
              truncatedByTokenCap: true,
            },
          },
        ),
        ["read_partial"],
      );
      assert.deepStrictEqual(yield* kind("Read", { file_path: f }, file(2000, 1, 2600)), [
        "read_partial",
      ]);
      assert.deepStrictEqual(
        yield* kind(
          "read",
          { filePath: f },
          {
            output: "x... (line truncated to 2000 chars)",
            metadata: { truncated: false },
          },
        ),
        ["read_partial"],
      );
      assert.deepStrictEqual(
        yield* kind("read", { filePath: f }, "x\n(File has more lines. Use 'offset')"),
        ["read_partial"],
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );
});

describe("keys from files a shell command names (#234)", () => {
  // The forms agents use for one database, from rocky's Observer (2026-10-06 to 2026-10-13).
  const setup = () => {
    const d = tmp();
    const keys = NodePath.join(d, "keys");
    NodeFS.mkdirSync(keys);
    const db = NodePath.join(d, "userdata", "chromeria-v2.sqlite");
    entry(keys, "by-path", "Open the projection database read-only.", {
      path: "**/chromeria-v2.sqlite",
    });
    entry(keys, "by-word", "Open the projection database read-only (bake entry).", {
      command: "chromeria-v2.sqlite",
    });
    const config: ServingConfig = { exactKeys: [keys], sessionStart: [], cue: [], cueUrl: null };
    const matched = (command: string, cwd = d) =>
      select(
        "claude",
        {
          hook_event_name: "PreToolUse",
          session_id: "S",
          cwd,
          tool_name: "Bash",
          tool_input: { command },
        },
        config,
        new Map(),
      ).pipe(Effect.map((s) => s.candidates.map((c) => c.key)));
    return { d, db, matched };
  };

  it.effect("matches a path key in every form a shell command names the file", () =>
    Effect.gen(function* () {
      const { d, db, matched } = setup();
      for (const command of [
        `sqlite3 ${db} 'select 1'`,
        `sqlite3 'file:${db}?mode=ro' 'select 1'`,
        `python3 -c "import sqlite3; sqlite3.connect('file://${db}?mode=ro', uri=True)"`,
        `DB=${db}; sqlite3 -readonly "$DB" .tables`,
        "sqlite3 -readonly ~/.t3/userdata/chromeria-v2.sqlite .tables",
      ]) {
        assert.include(yield* matched(command), "path:**/chromeria-v2.sqlite", command);
      }
      // A bare name resolves against the working directory.
      assert.include(
        yield* matched("sqlite3 chromeria-v2.sqlite .tables", NodePath.join(d, "userdata")),
        "path:**/chromeria-v2.sqlite",
      );
      for (const command of ["echo hello", "curl -s https://example.com/chromeria-v2.sqlite.gz"]) {
        assert.notInclude(yield* matched(command), "path:**/chromeria-v2.sqlite", command);
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps the first bake's bare command key matching as before", () =>
    Effect.gen(function* () {
      const { db, matched } = setup();
      // Word matching: the bare name as an argument, and a path whose last part is the name.
      for (const command of [
        "sqlite3 chromeria-v2.sqlite .tables",
        "sqlite3 -readonly ~/.t3/userdata/chromeria-v2.sqlite .tables",
        `sqlite3 ${db} .tables`,
      ]) {
        assert.include(yield* matched(command), "command:chromeria-v2.sqlite", command);
      }
      // The SQLite URI is one word ending in `?mode=ro`, so the bare command key misses it;
      // the path key above covers it.
      assert.notInclude(
        yield* matched(`sqlite3 'file:${db}?mode=ro' .tables`),
        "command:chromeria-v2.sqlite",
      );
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
    const config: ServingConfig = { exactKeys: [keys], sessionStart: [], cue: [], cueUrl: null };
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
      // The target comes first, so a harness that cuts a long deny cuts the rule text, not the path.
      assert.isTrue(
        pre.candidates[0]!.deny_text!.startsWith(
          `Read ${target} in full, then run the same call again.\n`,
        ),
      );
      const read = yield* sel("claude", {
        ...base,
        hook_event_name: "PostToolUse",
        tool_name: "Read",
        tool_input: { file_path: target },
        tool_response: {},
      });
      assert.deepStrictEqual(read.moment.reads, [{ path: target, kind: "read_tool" }]);
      const cut = yield* sel("grok", {
        ...base,
        hook_event_name: "post_tool_use",
        tool_name: "read_file",
        tool_input: { target_file: "RULE.md" },
        toolResult: { type: "ReadFile", tool_output_for_prompt: "# Rules … [+54 chars]" },
      });
      assert.deepStrictEqual(cut.moment.reads, [{ path: target, kind: "read_partial" }]);
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

  it.effect("marks cut reads in each harness's real result shapes (live samples, 2026-10-13)", () =>
    Effect.gen(function* () {
      const { ws, target, sel } = setup();
      const kinds = (harness: string, raw: Record<string, unknown>) =>
        sel(harness, { session_id: "S", cwd: ws, ...raw }).pipe(
          Effect.map((r) => r.moment.reads.map((x) => x.kind)),
        );
      const lines = (n: number) => Array.from({ length: n }, (_, i) => `line ${i + 1}`).join("\n");
      // Claude Code: a long shell output reaches the hook as the first 30,000 characters of
      // stdout, without a marker, plus where the rest was saved.
      assert.deepStrictEqual(
        yield* kinds("claude", {
          hook_event_name: "PostToolUse",
          tool_name: "Bash",
          tool_input: { command: "cat RULE.md" },
          tool_response: {
            stdout: lines(3000).slice(0, 30000),
            stderr: "",
            interrupted: false,
            isImage: false,
            noOutputExpected: false,
            persistedOutputPath: "/Users/u/.claude/projects/p/s/tool-results/b1.txt",
            persistedOutputSize: 1988895,
          },
        }),
        ["shell_partial"],
      );
      // Codex: the hook gets the same cut text the model sees.
      assert.deepStrictEqual(
        yield* kinds("codex", {
          hook_event_name: "PostToolUse",
          tool_name: "Bash",
          tool_input: { command: "cat RULE.md" },
          tool_response:
            "Warning: truncated output (original token count: 19974)\nTotal output lines: 3000\n\nline 1\nline 81 o…18974 tokens truncated…e",
        }),
        ["shell_partial"],
      );
      // Grok 1.0.13 read_file: no footer in the hook payload, only fewer lines than total_lines.
      const grokRead = (rawOutput: string, totalLines: number) =>
        kinds("grok", {
          hook_event_name: "post_tool_use",
          tool_name: "read_file",
          tool_input: { target_file: "RULE.md" },
          toolResult: {
            type: "ReadFile",
            FileContent: {
              content: rawOutput,
              content_concise: rawOutput,
              absolute_path: target,
              offset: null,
              raw_output: rawOutput,
              total_lines: totalLines,
            },
          },
        });
      assert.deepStrictEqual(yield* grokRead(`${lines(1000)}\n`, 3001), ["read_partial"]);
      assert.deepStrictEqual(yield* grokRead(`${lines(4)}\n`, 5), ["read_tool"]);
      // Grok's read-tool footer, which has no "of N" (Grok review of #226).
      assert.deepStrictEqual(
        yield* kinds("grok", {
          hook_event_name: "post_tool_use",
          tool_name: "read_file",
          tool_input: { target_file: "RULE.md" },
          toolResult: {
            type: "ReadFile",
            output_for_prompt: "1→# Rules\n(Showing lines 1-2000. Use offset=2001 to continue.)",
          },
        }),
        ["read_partial"],
      );
      // Grok shell: the cut notice in output_for_prompt.
      assert.deepStrictEqual(
        yield* kinds("grok", {
          hook_event_name: "post_tool_use",
          tool_name: "run_terminal_command",
          tool_input: { command: "cat RULE.md" },
          toolResult: {
            exit_code: 0,
            output_for_prompt:
              "exit: 0 [truncated: showing first/last 25.8 KB of 1.9 MB - full output at: /x/call-3.log]",
          },
        }),
        ["shell_partial"],
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
      const { ws, target, sel } = setup();
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
      // A read OpenCode cut at 2000 lines or 50 KB, as the plugin posts it: no offset or limit
      // in the args, the footer at the end of the forwarded output (Grok review of #226).
      const readOf = (output: string, metadata: Record<string, unknown> = { exit: undefined }) =>
        sel("opencode", {
          hook: "tool.execute.after",
          directory: ws,
          input: { tool: "read", sessionID: "ses_1", callID: "c2", args: { filePath: target } },
          output: { metadata, output },
        }).pipe(Effect.map((r) => r.moment.reads.map((x) => x.kind)));
      assert.deepStrictEqual(yield* readOf("# Rules\n"), ["read_tool"]);
      for (const footer of [
        "(Showing lines 1-2000 of 5000. Use offset=2001 to continue.)",
        "(Output capped at 50 KB. Showing lines 1-120. Use offset=121 to continue.)",
      ]) {
        assert.deepStrictEqual(yield* readOf(`# Rules\n${footer}`), ["read_partial"], footer);
      }
      // The plugin's own flag, set when the marker fell outside its 4 KB slice.
      assert.deepStrictEqual(yield* readOf("# Rules\n", { exit: undefined, truncated: true }), [
        "read_partial",
      ]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("gives Grok a deny that fits its 256-character reason, with a short head (#229)", () =>
    Effect.gen(function* () {
      const { ws, sel } = setup();
      const grok = yield* sel("grok", { hook_event_name: "stop", session_id: "S", cwd: ws });
      const claude = yield* sel("claude", { hook_event_name: "Stop", session_id: "S", cwd: ws });
      assert.strictEqual(grok.profile.deny_limit_bytes, 256);
      assert.isAtMost(Buffer.byteLength(grok.profile.heads.deny), 80);
      assert.include(grok.profile.heads.deny, "user's own");
      assert.isUndefined(claude.profile.deny_limit_bytes);
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
      const config: ServingConfig = { exactKeys: [], sessionStart: [], cue: [], cueUrl: null };
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
