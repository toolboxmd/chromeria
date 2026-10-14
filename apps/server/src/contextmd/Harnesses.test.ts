import { assert, describe, it } from "@effect/vitest";

import { withClaudeRequestRecords } from "./Harnesses.ts";

describe("Claude agent-session request recording defaults", () => {
  it("adds absent defaults without mutating the merged environment", () => {
    const input = { PUBLIC_TEST_KEY: "unchanged" };
    assert.deepEqual(withClaudeRequestRecords(input), {
      PUBLIC_TEST_KEY: "unchanged",
      CLAUDE_CODE_ELEGANT_MEADOW: "1",
      CLAUDE_CODE_TETHER_LIVE: "0",
    });
    assert.deepEqual(input, { PUBLIC_TEST_KEY: "unchanged" });
  });
  it.each(["0", "1", "", "false", "true"])(
    "preserves explicit value %s and extra-body input",
    (value) => {
      const input = {
        CLAUDE_CODE_ELEGANT_MEADOW: value,
        CLAUDE_CODE_TETHER_LIVE: value,
        CLAUDE_CODE_EXTRA_BODY: '{"messages":[]}',
      };
      assert.deepEqual(withClaudeRequestRecords(input), input);
    },
  );
  it("defaults each missing key independently", () => {
    assert.deepEqual(withClaudeRequestRecords({ CLAUDE_CODE_ELEGANT_MEADOW: "0" }), {
      CLAUDE_CODE_ELEGANT_MEADOW: "0",
      CLAUDE_CODE_TETHER_LIVE: "0",
    });
    assert.deepEqual(withClaudeRequestRecords({ CLAUDE_CODE_TETHER_LIVE: "1" }), {
      CLAUDE_CODE_ELEGANT_MEADOW: "1",
      CLAUDE_CODE_TETHER_LIVE: "1",
    });
  });
});
