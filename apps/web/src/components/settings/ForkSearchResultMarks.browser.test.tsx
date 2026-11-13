import { expect, it, vi } from "vite-plus/test";
import { page } from "vite-plus/test/browser";

import { render } from "../../../test/render";
import { SidebarProvider } from "../ui/sidebar";
import { SettingsSidebarNav } from "./SettingsSidebarNav";

vi.mock("./useAvailableSettingsSearchItems", () => ({
  useAvailableSettingsSearchItems: () => [
    { id: "theme", title: "Theme", to: "/settings/appearance" },
    {
      id: "prism-roles",
      title: "Prism role preferences",
      to: "/settings/prism",
      forkAdded: true,
    },
  ],
}));

function renderNav() {
  return render(
    <SidebarProvider defaultOpen>
      <SettingsSidebarNav pathname="/settings/general" />
    </SidebarProvider>,
    { router: "/settings/general" },
  );
}

it("marks fork-added settings search results", async () => {
  await using _screen = await renderNav();
  await page.getByRole("combobox", { name: "Search settings" }).fill("prism role");
  const option = page.getByRole("option", { name: /Prism role/ });
  await expect.element(option).toBeVisible();
  expect(
    option.element().querySelector('[role="img"][aria-label="Chromeria feature"]'),
  ).not.toBeNull();
});

it("leaves upstream settings search results unmarked", async () => {
  await using _screen = await renderNav();
  await page.getByRole("combobox", { name: "Search settings" }).fill("theme");
  const option = page.getByRole("option", { name: /Theme/ });
  await expect.element(option).toBeVisible();
  expect(option.element().querySelector('[role="img"][aria-label="Chromeria feature"]')).toBeNull();
});
