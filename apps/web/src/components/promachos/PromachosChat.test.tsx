import { act } from "react";
import { create } from "react-test-renderer";
import { describe, expect, it } from "vite-plus/test";

import { usePromachosInlineCardHost } from "./PromachosChat";

type HostInput = Parameters<typeof usePromachosInlineCardHost>[0];
type Host = ReturnType<typeof usePromachosInlineCardHost>;

function Probe({ onRender, ...input }: HostInput & { onRender: (host: Host) => void }) {
  onRender(usePromachosInlineCardHost(input));
  return null;
}

function renderHost(initial: HostInput) {
  const rendered: Host[] = [];
  const onRender = (host: Host) => rendered.push(host);
  let renderer!: ReturnType<typeof create>;
  act(() => {
    renderer = create(<Probe {...initial} onRender={onRender} />);
  });
  return {
    get: () => rendered.at(-1)!,
    update: (next: HostInput) =>
      act(() => renderer.update(<Probe {...next} onRender={onRender} />)),
    // React detaches a callback ref with null before attaching its replacement.
    attach: (ref: Host["timelineHostRef"], element: HTMLDivElement | null) =>
      act(() => ref(element)),
  };
}

const element = (name: string) => ({ name }) as unknown as HTMLDivElement;

describe("usePromachosInlineCardHost", () => {
  it("gives the active thread's composer the host its own timeline renders", () => {
    const host = renderHost({
      enabled: true,
      activeThreadKey: "env:a",
      displayedThreadKey: "env:a",
    });
    const a = element("a");
    host.attach(host.get().timelineHostRef, a);
    expect(host.get().composerHost).toBe(a);
  });

  it("keeps the next thread's cards on its composer while the previous timeline is painted", () => {
    const host = renderHost({
      enabled: true,
      activeThreadKey: "env:a",
      displayedThreadKey: "env:a",
    });
    const previousRef = host.get().timelineHostRef;
    host.attach(previousRef, element("a"));

    host.update({ enabled: true, activeThreadKey: "env:b", displayedThreadKey: "env:a" });
    expect(host.get().composerHost).toBeNull();

    host.update({ enabled: true, activeThreadKey: "env:b", displayedThreadKey: "env:b" });
    const b = element("b");
    host.attach(previousRef, null);
    host.attach(host.get().timelineHostRef, b);
    expect(host.get().composerHost).toBe(b);
  });

  it("offers the composer no host outside a Promachos chat", () => {
    const host = renderHost({
      enabled: false,
      activeThreadKey: "env:a",
      displayedThreadKey: "env:a",
    });
    host.attach(host.get().timelineHostRef, element("a"));
    expect(host.get().composerHost).toBeNull();
  });
});
