import { act, useRef, useState } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { userEvent } from "vite-plus/test/browser";

import {
  Menu,
  MenuItem,
  MenuPopup,
  MenuSub,
  MenuSubPopup,
  MenuSubTrigger,
  MenuTrigger,
} from "./menu";

let root: Root;
let container: HTMLDivElement;

beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(() => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});

function completion() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function setup({ hover = false, submenu = false, modal = false } = {}) {
  const opened = completion();
  const closed = completion();
  const childOpened = completion();
  const childClosed = completion();
  const changes: string[] = [];
  function Fixture() {
    const [open, setOpen] = useState(false);
    const trigger = useRef<HTMLButtonElement>(null);
    return (
      <>
        <style>{`
          [data-slot="menu-popup"], [data-slot="menu-sub-content"] { transition: opacity 40ms linear; }
          [data-ending-style] { opacity: 0; }
        `}</style>
        <input
          aria-label="Editor"
          onKeyDown={(event) => {
            if (event.ctrlKey && event.key === "y") {
              trigger.current?.focus();
              setOpen(true);
            }
          }}
        />
        <button type="button" style={{ position: "fixed", right: 0, bottom: 0 }}>
          Elsewhere
        </button>
        <Menu
          open={open}
          modal={modal}
          onOpenChange={(next, details) => {
            changes.push(`${next}:${details.reason}:${details.event.type}`);
            setOpen(next);
          }}
          onOpenChangeComplete={(next) => (next ? opened : closed).resolve()}
        >
          <MenuTrigger ref={trigger} openOnHover={hover} delay={0} closeDelay={0}>
            Actions
          </MenuTrigger>
          <MenuPopup>
            <MenuItem>First action</MenuItem>
            {submenu && (
              <MenuSub
                onOpenChangeComplete={(next) => (next ? childOpened : childClosed).resolve()}
              >
                <MenuSubTrigger>More actions</MenuSubTrigger>
                <MenuSubPopup>
                  <MenuItem>Nested action</MenuItem>
                </MenuSubPopup>
              </MenuSub>
            )}
          </MenuPopup>
        </Menu>
      </>
    );
  }
  await act(() => root.render(<Fixture />));
  const input = container.querySelector("input")!;
  const elsewhere = container.querySelector<HTMLButtonElement>("button")!;
  await hoverElement(elsewhere);
  return {
    get trigger() {
      return container.querySelector<HTMLButtonElement>('[data-slot="menu-trigger"]')!;
    },
    input,
    elsewhere,
    opened,
    closed,
    childOpened,
    childClosed,
    changes,
  };
}

async function press(key: string, options: KeyboardEventInit = {}) {
  await act(() =>
    document.activeElement!.dispatchEvent(
      new KeyboardEvent("keydown", {
        key,
        bubbles: true,
        cancelable: true,
        ...options,
      }),
    ),
  );
}

async function complete(milestone: Promise<void>) {
  await milestone;
  await act(async () => {
    await Promise.resolve();
  });
}

async function hoverElement(element: Element, force = false) {
  await act(async () => {
    await userEvent.hover(element, { force });
  });
}

async function openFromInput(fixture: Awaited<ReturnType<typeof setup>>) {
  await act(() => fixture.input.focus());
  await press("y", { ctrlKey: true });
  await complete(fixture.opened.promise);
  expect(document.activeElement?.closest('[role="menu"]')).not.toBeNull();
  return document.querySelector<HTMLElement>('[role="menu"]')!;
}

describe("menu close focus", () => {
  it.each(["over", "away"])(
    "returns focus after Escape with the pointer %s the popup",
    async (pointer) => {
      const fixture = await setup();
      const menu = await openFromInput(fixture);
      if (pointer === "over") await hoverElement(menu);
      await press("Escape");
      await complete(fixture.closed.promise);
      expect(document.querySelector('[role="menu"]')).toBeNull();
      expect(document.activeElement).toBe(fixture.trigger);
    },
  );

  it("respects focus moved elsewhere during Escape close", async () => {
    const fixture = await setup();
    const menu = await openFromInput(fixture);
    await hoverElement(menu);
    await press("Escape");
    expect(document.querySelector('[role="menu"][data-closed]')).not.toBeNull();
    await act(() => fixture.elsewhere.focus());
    await complete(fixture.closed.promise);
    expect(document.querySelector('[role="menu"]')).toBeNull();
    expect(document.activeElement).toBe(fixture.elsewhere);
  });

  it("keeps click-open behavior and outside-click focus", async () => {
    const fixture = await setup({ modal: false });
    await act(async () => {
      await userEvent.click(fixture.trigger);
    });
    await complete(fixture.opened.promise);
    expect(document.querySelector('[role="menu"]:not([data-closed])')).not.toBeNull();
    await act(async () => {
      await userEvent.click(fixture.elsewhere);
    });
    await complete(fixture.closed.promise);
    expect(document.querySelector('[role="menu"]')).toBeNull();
    expect(document.activeElement).toBe(fixture.elsewhere);
  });

  it("dismisses a genuinely hover-opened menu when leaving it", async () => {
    const fixture = await setup({ hover: true, modal: false });
    await hoverElement(fixture.trigger);
    await complete(fixture.opened.promise);
    await hoverElement(document.querySelector('[role="menu"]')!);
    await hoverElement(fixture.elsewhere, true);
    await complete(fixture.closed.promise);
    expect(document.querySelector('[role="menu"]')).toBeNull();
    expect(fixture.changes).toContain("false:trigger-hover:mouseleave");
    expect(document.activeElement).not.toBe(fixture.trigger);
  });

  it("allows keyboard entry, pointer movement into a focused submenu, and Escape back", async () => {
    const fixture = await setup({ submenu: true });
    await openFromInput(fixture);
    await press("ArrowDown");
    await press("ArrowDown");
    const subTrigger = document.querySelector<HTMLElement>('[data-slot="menu-sub-trigger"]')!;
    expect(document.activeElement).toBe(subTrigger);
    await press("ArrowRight");
    await complete(fixture.childOpened.promise);
    const nested = document.querySelector<HTMLElement>(
      '[data-slot="menu-sub-content"] [role="menuitem"]',
    )!;
    expect(document.activeElement).toBe(nested);
    await hoverElement(subTrigger);
    await hoverElement(nested);
    expect(document.querySelectorAll('[role="menu"]').length).toBe(2);
    expect(document.activeElement).toBe(nested);
    await press("Escape");
    await complete(fixture.childClosed.promise);
    expect(document.activeElement).toBe(subTrigger);
    expect(document.querySelector('[role="menu"]:not([data-closed])')).not.toBeNull();
  });
});
