import { ProviderDriverKind, ProviderInstanceId, type Color } from "@t3tools/contracts";
import { act, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { ColorList } from "./ColorList";
import type { ColorProviderChoice } from "./ColorPicker.logic";

const standard: Color = { instanceId: ProviderInstanceId.make("codex"), model: "sol" };
const codex: ColorProviderChoice = {
  instanceId: standard.instanceId,
  driverKind: ProviderDriverKind.make("codex"),
  displayName: "Codex",
  models: [
    {
      slug: "sol",
      name: "Sol",
      efforts: [{ id: "high", label: "High" }],
      options: [
        {
          id: "serviceTier",
          label: "Speed",
          type: "select",
          options: [
            { id: "default", label: "Standard" },
            { id: "priority", label: "Fast" },
          ],
        },
        { id: "enabled", label: "Enabled", type: "boolean" },
      ],
    },
  ],
};
let root: Root;
let container: HTMLDivElement;
let persisted: readonly Color[];
const save = vi.fn(async (colors: readonly Color[]) => {
  persisted = colors;
});
function Settings() {
  const [saved, setSaved] = useState(persisted);
  return (
    <ColorList
      title="Reviewer"
      saved={saved}
      providers={[codex]}
      mixed={false}
      disabled={false}
      saving={false}
      save={async (colors) => {
        await save(colors);
        setSaved(colors);
      }}
    />
  );
}
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  persisted = [standard];
  save.mockClear();
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});
async function choose(label: string, option: string) {
  await userEvent.click(page.getByRole("combobox", { name: label, exact: true }));
  await userEvent.click(page.getByRole("option", { name: option, exact: true }));
}
it("saves Fast, retains it through effort edits and remount, and restores the speed default", async () => {
  await act(async () => root.render(<Settings />));
  await choose("Reviewer preference 1 Speed", "Fast");
  const fast = { ...standard, options: [{ id: "serviceTier", value: "priority" }] };
  expect(save).toHaveBeenLastCalledWith([fast]);
  await choose("Reviewer preference 1 effort", "High");
  expect(save).toHaveBeenLastCalledWith([{ ...fast, effort: "high" }]);
  await act(async () => root.render(<Settings key="reload" />));
  await expect
    .element(page.getByRole("combobox", { name: "Reviewer preference 1 Speed", exact: true }))
    .toHaveTextContent("Fast");
  await choose("Reviewer preference 1 Speed", "Default speed");
  expect(save).toHaveBeenLastCalledWith([{ ...standard, effort: "high" }]);
  await choose("Reviewer preference 1 Enabled", "Off");
  expect(save).toHaveBeenLastCalledWith([
    { ...standard, effort: "high", options: [{ id: "enabled", value: false }] },
  ]);
});
