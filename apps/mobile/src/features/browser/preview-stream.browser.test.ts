// @vitest-environment jsdom
import { expect, it, vi } from "vite-plus/test";
import { start, stop, updateAccess } from "./preview-stream.browser";

it("keeps the picture-in-picture capture alive across deadline-driven socket replacement", () => {
  vi.useFakeTimers();
  vi.setSystemTime(1_800_000_000_000);
  const messages: Array<{ type: string; active?: boolean }> = [];
  vi.stubGlobal("ReactNativeWebView", {
    postMessage: (message: string) => messages.push(JSON.parse(message)),
  });
  class Socket extends EventTarget {
    static current: Socket;
    readyState = 1;
    constructor(readonly url: string) {
      super();
      Socket.current = this;
    }
    close() {
      this.readyState = 3;
    }
  }
  vi.stubGlobal("WebSocket", Socket);
  vi.stubGlobal(
    "ResizeObserver",
    class {
      constructor(readonly callback: () => void) {}
      observe() {
        this.callback();
      }
      disconnect() {}
    },
  );
  class CaptureTrack {
    stop = vi.fn();
    requestFrame() {}
  }
  const track = new CaptureTrack();
  const stream = { getVideoTracks: () => [track] };
  vi.stubGlobal("CanvasCaptureMediaStreamTrack", CaptureTrack);
  vi.stubGlobal(
    "HTMLVideoElement",
    class {
      webkitSetPresentationMode() {}
    },
  );
  const captureDescriptor = Object.getOwnPropertyDescriptor(
    HTMLCanvasElement.prototype,
    "captureStream",
  );
  const pipDescriptor = Object.getOwnPropertyDescriptor(document, "pictureInPictureElement");
  Object.defineProperty(HTMLCanvasElement.prototype, "captureStream", {
    configurable: true,
    value: () => stream,
  });
  vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockReturnValue(null);
  vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockReturnValue(
    new DOMRect(0, 0, 800, 600),
  );
  const access = {
    httpBase: "https://source.test/api/preview-stream",
    wsBase: "wss://source.test/api/preview-stream",
    query: { viewerTicket: "initial" },
    credentials: false,
    expiresAt: 1_800_000_060_000,
  };
  try {
    start({ access, interactive: true, threadId: "thread", tabId: "tab", background: "#000" });
    vi.advanceTimersByTime(0);
    const oldSocket = Socket.current;
    oldSocket.dispatchEvent(new Event("open"));
    const video = document.querySelector("video")!;
    expect(video).not.toBeNull();
    Object.defineProperty(document, "pictureInPictureElement", {
      configurable: true,
      value: video,
    });
    video.dispatchEvent(new Event("enterpictureinpicture"));
    vi.advanceTimersByTime(60_000);
    expect(messages).toContainEqual({ type: "unauthorized" });
    expect(oldSocket.readyState).toBe(3);
    updateAccess({ ...access, query: { viewerTicket: "renewed" }, expiresAt: 1_800_000_090_000 });
    expect(Socket.current).not.toBe(oldSocket);
    expect(new URL(Socket.current.url).searchParams.get("viewerTicket")).toBe("renewed");
    expect(document.querySelector("video")).toBe(video);
    expect(video.srcObject).toBe(stream);
    expect(document.pictureInPictureElement).toBe(video);
    expect(track.stop).not.toHaveBeenCalled();
    expect(messages.findLast((message) => message.type === "pictureInPicture")?.active).toBe(true);
    vi.advanceTimersByTime(30_000);
    expect(messages.filter((message) => message.type === "unauthorized")).toHaveLength(2);
    stop();
    expect(track.stop).toHaveBeenCalledOnce();
  } finally {
    stop();
    if (captureDescriptor)
      Object.defineProperty(HTMLCanvasElement.prototype, "captureStream", captureDescriptor);
    else Reflect.deleteProperty(HTMLCanvasElement.prototype, "captureStream");
    if (pipDescriptor) Object.defineProperty(document, "pictureInPictureElement", pipDescriptor);
    else Reflect.deleteProperty(document, "pictureInPictureElement");
    document.body.replaceChildren();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    vi.useRealTimers();
  }
});
