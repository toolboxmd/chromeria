import { describe, expect, it } from "vite-plus/test";

import { promachosStatusText } from "./status.ts";

describe("promachosStatusText", () => {
  it("keeps the plain words of a step and drops what follows them", () => {
    expect(promachosStatusText("Read /Users/me/promachos/AGENTS.md")).toBe("Read");
    expect(promachosStatusText("Searching the web")).toBe("Searching the web");
    expect(promachosStatusText("Ran `vp test run` in ~/dev")).toBe("Ran");
    expect(promachosStatusText("Fetched https://example.com/docs")).toBe("Fetched");
  });

  it("leaves nothing when the step is only a path or command", () => {
    expect(promachosStatusText("/Users/me/promachos")).toBeNull();
    expect(promachosStatusText("`ls -la`")).toBeNull();
    expect(promachosStatusText(undefined)).toBeNull();
  });
});
