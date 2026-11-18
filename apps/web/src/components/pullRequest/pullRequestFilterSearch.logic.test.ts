import { describe, expect, it } from "vite-plus/test";

import { searchFilterOptions } from "./pullRequestFilterSearch.logic";

const option = (value: string) => ({ value, label: value });
const options = [
  option(""),
  ...Array.from({ length: 12 }, (_, index) => option(`repo-${index}`)),
  option("other"),
];
const values = (result: ReturnType<typeof searchFilterOptions<{ value: string; label: string }>>) =>
  result.shown.map((shown) => shown.value);

describe("searchFilterOptions", () => {
  it("keeps the all option first and shows at most ten matches", () => {
    const result = searchFilterOptions(options, "", "repo");
    expect(values(result)).toEqual(["", ...options.slice(1, 11).map((shown) => shown.value)]);
    expect(result.empty).toBe(false);
  });

  it("keeps the selection after the all option even when the search misses it", () => {
    expect(values(searchFilterOptions(options, "other", "repo-1"))).toEqual([
      "",
      "other",
      "repo-1",
      "repo-10",
      "repo-11",
    ]);
  });

  it("says no matches only for a search that finds nothing", () => {
    expect(searchFilterOptions(options, "", "zzz")).toEqual({ shown: [option("")], empty: true });
    expect(searchFilterOptions([option("")], "", "").empty).toBe(false);
    expect(searchFilterOptions(options, "other", "zzz").empty).toBe(false);
  });
});
