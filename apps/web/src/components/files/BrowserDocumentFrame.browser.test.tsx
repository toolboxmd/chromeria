import { describe, expect, it, vi } from "vite-plus/test";

import "~/index.css";
import { render } from "../../../test/render";

import { useOpenLink } from "~/browser/useOpenLink";

vi.mock(import("~/browser/useOpenLink"), () => ({ useOpenLink: vi.fn() }));

import { HtmlRenderDocument } from "./BrowserDocumentFrame";

const sizeChanged = (height: number) => ({
  jsonrpc: "2.0",
  method: "ui/notifications/size-changed",
  params: { height },
});

describe("HtmlRenderDocument", () => {
  it("reports the page's first height and ignores messages from other windows", async () => {
    vi.mocked(useOpenLink).mockReturnValue(vi.fn().mockResolvedValue(undefined));
    using pageUrl = (() => {
      const page = `<script>parent.postMessage(${JSON.stringify(sizeChanged(321))}, "*");</script>`;
      const url = URL.createObjectURL(new Blob([page], { type: "text/html" }));
      return { url, [Symbol.dispose]: () => URL.revokeObjectURL(url) };
    })();
    const onContentHeight = vi.fn();
    await using _screen = await render(
      <HtmlRenderDocument src={pageUrl.url} title="Report" onContentHeight={onContentHeight} />,
    );

    await vi.waitFor(() => expect(onContentHeight).toHaveBeenCalledWith(321));

    // Listeners run in registration order, so once this one sees the message the frame's has too.
    const delivered = new Promise<void>((resolve) =>
      window.addEventListener("message", () => resolve(), { once: true }),
    );
    window.postMessage(sizeChanged(999), "*");
    await delivered;
    expect(onContentHeight).toHaveBeenCalledTimes(1);
  });
  it.each(["https://example.com/report", "https://github.com/toolboxmd/chromeria/issues/620"])(
    "forwards the complete native link request to the general opener without an Issue-capable environment: %s",
    async (url) => {
      const openLink = vi.fn().mockResolvedValue(undefined);
      vi.mocked(useOpenLink).mockReturnValue(openLink);
      using _activation = vi
        .spyOn(navigator, "userActivation", "get")
        .mockReturnValue({ isActive: true, hasBeenActive: true });
      await using screen = await render(<HtmlRenderDocument src="about:blank" title="Report" />);
      const frame = screen.container.querySelector("iframe")!;
      frame.focus();
      window.dispatchEvent(
        new MessageEvent("message", {
          source: frame.contentWindow,
          data: { jsonrpc: "2.0", id: "link", method: "ui/open-link", params: { url } },
        }),
      );
      await vi.waitFor(() =>
        expect(openLink).toHaveBeenCalledExactlyOnceWith(url, {
          event: { id: "link", url, metaKey: false, ctrlKey: false },
        }),
      );
    },
  );
});
