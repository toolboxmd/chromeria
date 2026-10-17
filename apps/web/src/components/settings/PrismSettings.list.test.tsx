// @vitest-environment jsdom

import { ProviderInstanceId, type PrismModelPreference } from "@t3tools/contracts";
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";

import { ModelList } from "./PrismSettings";

const first = { instanceId: ProviderInstanceId.make("codex"), model: "first" };
const second = { instanceId: ProviderInstanceId.make("opencode"), model: "second" };

let root: Root;
let container: HTMLDivElement;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
      unobserve() {}
    },
  );
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(() => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

/**
 * Mirrors the page: one write at a time, and `saving` turns on in the same render as the edit,
 * as the save store does, until its snapshot lands or it fails.
 */
function harness(initial: readonly PrismModelPreference[]) {
  let saved = initial;
  let saving = false;
  let settle: { resolve: () => void; reject: (cause: Error) => void } | null = null;
  const save = vi.fn((models: readonly PrismModelPreference[]) => {
    if (saving) return Promise.reject(new Error("Wait for the current settings update to finish."));
    saving = true;
    root.render(element());
    return new Promise<void>((resolve, reject) => {
      settle = {
        resolve: () => {
          saved = models;
          resolve();
        },
        reject,
      };
    });
  });
  const element = () => (
    <ModelList
      key="list"
      title="Reviewer"
      saved={saved}
      choices={[]}
      mixed={false}
      disabled={false}
      saving={saving}
      save={save}
    />
  );
  const render = () => act(() => root.render(element()));
  const finish = async (outcome: "success" | "failure") => {
    await act(async () => {
      if (outcome === "success") settle!.resolve();
      else settle!.reject(new Error("offline"));
    });
    saving = false;
    await render();
  };
  return { save, render, finish };
}

const rows = () => [...container.querySelectorAll("li")];
const order = () => rows().map((row) => row.textContent?.match(/(first|second)/)?.[1]);
const moveDown = (position: number) =>
  act(() =>
    container
      .querySelector<HTMLButtonElement>(`[aria-label="Move Reviewer preference ${position} down"]`)!
      .click(),
  );
const moveUp = (position: number) =>
  act(() =>
    container
      .querySelector<HTMLButtonElement>(`[aria-label="Move Reviewer preference ${position} up"]`)!
      .click(),
  );

describe("ModelList while a save is pending", () => {
  it("keeps the first edit, ignores the second, and keeps rows mounted when the save lands", async () => {
    const list = harness([first, second]);
    await list.render();
    const [firstRow, secondRow] = rows();
    await moveDown(1);
    await list.render();
    expect(order()).toEqual(["second", "first"]);

    await moveUp(2);
    expect(order()).toEqual(["second", "first"]);
    expect(list.save).toHaveBeenCalledTimes(1);
    expect(
      container.querySelector<HTMLButtonElement>('[aria-label="Move Reviewer preference 2 up"]')!
        .disabled,
    ).toBe(false);

    await list.finish("success");
    expect(order()).toEqual(["second", "first"]);
    expect(rows()).toEqual([secondRow, firstRow]);
  });

  it("restores the last saved order when the pending save fails", async () => {
    const list = harness([first, second]);
    await list.render();
    await moveDown(1);
    await list.render();
    await moveUp(2);
    expect(order()).toEqual(["second", "first"]);

    await list.finish("failure");
    expect(order()).toEqual(["first", "second"]);
  });
});
