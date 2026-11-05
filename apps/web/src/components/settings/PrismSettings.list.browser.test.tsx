import { ProviderDriverKind, ProviderInstanceId, type Color } from "@t3tools/contracts";
import { act, render, type Screen } from "../../../test/render";
import { describe, expect, it, vi } from "vite-plus/test";

import type { ColorProviderChoice } from "./ColorPicker.logic";
import { ColorList } from "./ColorList";

const first = { instanceId: ProviderInstanceId.make("codex"), model: "first" };
const second = { instanceId: ProviderInstanceId.make("opencode"), model: "second" };

/**
 * Mirrors the page: one write at a time, and `saving` turns on in the same render as the edit,
 * as the save store does, until its snapshot lands or it fails.
 */
function harness(
  screen: Screen,
  initial: readonly Color[],
  providers: readonly ColorProviderChoice[] = [],
  title = "Reviewer",
) {
  const { container } = screen;
  let saved = initial;
  let saving = false;
  let settle: { resolve: () => void; reject: (cause: Error) => void } | null = null;
  const save = vi.fn((models: readonly Color[]) => {
    if (saving) return Promise.reject(new Error("Wait for the current settings update to finish."));
    saving = true;
    screen.rerender(element());
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
    <ColorList
      key="list"
      title={title}
      saved={saved}
      providers={providers}
      mixed={false}
      disabled={false}
      saving={saving}
      save={save}
    />
  );
  const render = () => act(() => screen.rerender(element()));
  const finish = async (outcome: "success" | "failure") => {
    await act(async () => {
      if (outcome === "success") settle!.resolve();
      else settle!.reject(new Error("offline"));
    });
    saving = false;
    await render();
  };
  const rows = () => [...container.querySelectorAll("li")];
  const order = () => rows().map((row) => row.textContent?.match(/(first|second)/)?.[1]);
  const moveDown = (position: number) =>
    act(() =>
      container
        .querySelector<HTMLButtonElement>(
          `[aria-label="Move Reviewer preference ${position} down"]`,
        )!
        .click(),
    );
  const moveUp = (position: number) =>
    act(() =>
      container
        .querySelector<HTMLButtonElement>(`[aria-label="Move Reviewer preference ${position} up"]`)!
        .click(),
    );

  return { save, render, finish, rows, order, moveDown, moveUp };
}

describe("ColorList while a save is pending", () => {
  it("keeps the first edit, ignores the second, and keeps rows mounted when the save lands", async () => {
    await using screen = await render(null);
    const { container } = screen;
    const list = harness(screen, [first, second]);
    const { rows, order, moveDown, moveUp } = list;
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
    await using screen = await render(null);
    const list = harness(screen, [first, second]);
    const { order, moveDown, moveUp } = list;
    await list.render();
    await moveDown(1);
    await list.render();
    await moveUp(2);
    expect(order()).toEqual(["second", "first"]);

    await list.finish("failure");
    expect(order()).toEqual(["first", "second"]);
  });
});

describe("ColorList adding a Color", () => {
  it("appends the picked Color and keeps existing rows mounted when the save lands", async () => {
    const codex: ColorProviderChoice = {
      instanceId: first.instanceId,
      driverKind: ProviderDriverKind.make("codex"),
      displayName: "Codex",
      models: [{ slug: "third", name: "Third", efforts: [] }],
    };
    await using screen = await render(null);
    const { container } = screen;
    const list = harness(screen, [first, second], [codex]);
    const { rows } = list;
    await list.render();
    const [firstRow, secondRow] = rows();
    await act(() =>
      container.querySelector<HTMLButtonElement>('[aria-label="Add Reviewer Color"]')!.click(),
    );
    expect(list.save).toHaveBeenCalledWith([
      first,
      second,
      { instanceId: first.instanceId, model: "third" },
    ]);

    await list.finish("success");
    expect(rows()).toHaveLength(3);
    expect(rows().slice(0, 2)).toEqual([firstRow, secondRow]);
  });
});

it("keeps Promachos rows mounted while its reordered Color draft saves", async () => {
  await using screen = await render(null);
  const { container } = screen;
  const list = harness(screen, [first, second], [], "Promachos");
  const { rows } = list;
  await list.render();
  const [firstRow, secondRow] = rows();
  await act(() =>
    container
      .querySelector<HTMLButtonElement>('[aria-label="Move Promachos preference 1 down"]')!
      .click(),
  );
  expect(list.save).toHaveBeenCalledWith([second, first]);
  expect(rows()).toEqual([secondRow, firstRow]);
  await list.finish("success");
  expect(rows()).toEqual([secondRow, firstRow]);
});
