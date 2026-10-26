import type { BrowserContext, Page, Download, FileChooser } from "playwright-core";

/** Deterministic Chromium/CDP leaf. No browser, executable, host or policy is replaced here. */
export function makeExternalBrowserContext(onClose?: (context: BrowserContext) => void) {
  const listeners = new Map<string, Set<(...args: unknown[]) => unknown>>();
  const on = (name: string, callback: (...args: unknown[]) => unknown) => {
    const callbacks = listeners.get(name) ?? new Set();
    callbacks.add(callback);
    listeners.set(name, callbacks);
  };
  const off = (name: string, callback: (...args: unknown[]) => unknown) =>
    listeners.get(name)?.delete(callback);
  const emit = (name: string, ...args: unknown[]) => {
    for (const callback of listeners.get(name) ?? []) callback(...args);
  };
  const effects: string[] = [];
  const inputs: Array<{ method: string; params: Record<string, unknown> | undefined }> = [];
  let url = "about:blank";
  let viewport = { width: 1280, height: 800 };
  let closed = false;
  const controls = {
    beforeSession: async () => {},
    afterSetFiles: async (_paths: ReadonlyArray<string>) => {},
    beforeBounds: async () => {},
    beforeLayout: async () => {},
    afterKey: async (_key: string) => {},
    afterInput: async (_method: string) => {},
    afterClose: async () => {},
  };
  const locator = {
    scrollIntoViewIfNeeded: async () => {
      effects.push("scroll-target");
    },
    boundingBox: async () => {
      await controls.beforeBounds();
      return { x: 0, y: 0, width: 100, height: 40 };
    },
    click: async () => {
      effects.push("click");
    },
    hover: async () => {
      effects.push("hover");
    },
    fill: async (_text: string) => {
      effects.push("fill");
    },
    setInputFiles: async (paths: ReadonlyArray<string>) => {
      effects.push("files");
      await controls.afterSetFiles(paths);
    },
    count: async () => 1,
    evaluate: async () => true,
  };
  const cdp = {
    on,
    off,
    detach: async () => {},
    send: async (
      method: string,
      params?: Record<string, unknown>,
    ): Promise<Record<string, unknown>> => {
      if (method.startsWith("Input.")) {
        inputs.push({ method, params });
        await controls.afterInput(method);
      }
      if (method === "Page.getLayoutMetrics") {
        await controls.beforeLayout();
        return { cssVisualViewport: { pageX: 0, pageY: 0 } };
      }
      if (method === "Page.getNavigationHistory") return { currentIndex: 0, entries: [{}] };
      if (method === "Page.getFrameTree") return { frameTree: { frame: { id: "main" } } };
      if (method === "Page.createIsolatedWorld") return { executionContextId: 7 };
      if (method === "Page.captureScreenshot") {
        if (params?.format === "jpeg") {
          effects.push("capture");
          return {
            data: "/9j/4AAQSkZJRgABAQAASABIAAD/4QBMRXhpZgAATU0AKgAAAAgAAYdpAAQAAAABAAAAGgAAAAAAA6ABAAMAAAABAAEAAKACAAQAAAABAAAAAaADAAQAAAABAAAAAQAAAAD/7QA4UGhvdG9zaG9wIDMuMAA4QklNBAQAAAAAAAA4QklNBCUAAAAAABDUHYzZjwCyBOmACZjs+EJ+/8AAEQgAAQABAwEiAAIRAQMRAf/EAB8AAAEFAQEBAQEBAAAAAAAAAAABAgMEBQYHCAkKC//EALUQAAIBAwMCBAMFBQQEAAABfQECAwAEEQUSITFBBhNRYQcicRQygZGhCCNCscEVUtHwJDNicoIJChYXGBkaJSYnKCkqNDU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6g4SFhoeIiYqSk5SVlpeYmZqio6Slpqeoqaqys7S1tre4ubrCw8TFxsfIycrS09TV1tfY2drh4uPk5ebn6Onq8fLz9PX29/j5+v/EAB8BAAMBAQEBAQEBAQEAAAAAAAABAgMEBQYHCAkKC//EALURAAIBAgQEAwQHBQQEAAECdwABAgMRBAUhMQYSQVEHYXETIjKBCBRCkaGxwQkjM1LwFWJy0QoWJDThJfEXGBkaJicoKSo1Njc4OTpDREVGR0hJSlNUVVZXWFlaY2RlZmdoaWpzdHV2d3h5eoKDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uLj5OXm5+jp6vLz9PX29/j5+v/bAEMAAgICAgICAwICAwUDAwMFBgUFBQUGCAYGBgYGCAoICAgICAgKCgoKCgoKCgwMDAwMDA4ODg4ODw8PDw8PDw8PD//bAEMBAgICBAQEBwQEBxALCQsQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEP/dAAQAAf/aAAwDAQACEQMRAD8A/fyiiigD/9k=",
          };
        }
        effects.push("capture");
        return {
          data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR4nGP4DwQACfsD/fteaysAAAAASUVORK5CYII=",
        };
      }
      return { result: { value: "external browser leaf" } };
    },
  };
  const page = {
    on,
    off,
    once: (name: string, callback: (...args: unknown[]) => unknown) => {
      const once = (...args: unknown[]) => {
        off(name, once);
        callback(...args);
      };
      on(name, once);
    },
    context: () => context,
    mainFrame: () => page,
    frames: () => [page],
    url: () => url,
    title: async () => "External browser leaf",
    viewportSize: () => viewport,
    setViewportSize: async (size: typeof viewport) => {
      viewport = size;
      effects.push("viewport");
    },
    setDefaultTimeout: (_timeout: number) => {},
    setDefaultNavigationTimeout: (_timeout: number) => {},
    goto: async (next: string) => {
      url = next;
      effects.push("navigate");
      emit("load");
    },
    emulateMedia: async () => {},
    waitForLoadState: async () => {},
    locator: (_selector: string) => ({ ...locator, last: () => locator }),
    ariaSnapshot: async () => '- button "button" [ref=e1]',
    evaluate: async () => ({
      url,
      title: "External browser leaf",
      loading: false,
      visibleText: "button",
      interactiveElements: [],
    }),
    isClosed: () => closed,
    close: async () => {
      if (!closed) {
        closed = true;
        emit("close");
        await controls.afterClose();
      }
    },
    mouse: {
      click: async () => {
        effects.push("click");
      },
      move: async () => {
        effects.push("hover");
      },
    },
    keyboard: {
      press: async (key: string) => {
        effects.push(`key:${key}`);
        await controls.afterKey(key);
      },
      insertText: async (text: string) => {
        effects.push(`text:${text}`);
      },
    },
  };
  const context = {
    emit,
    emitAsync: (name: string, ...args: unknown[]) =>
      Promise.all([...(listeners.get(name) ?? [])].map((callback) => callback(...args))),
    effects,
    inputs,
    controls,
    page,
    cdp,
    newPage: async () => {
      effects.push("new-page");
      return page as unknown as Page;
    },
    newCDPSession: async () => {
      await controls.beforeSession();
      return cdp;
    },
    grantPermissions: async () => {},
    close: async () => {
      await page.close();
      onClose?.(context as unknown as BrowserContext);
    },
  };
  return context;
}

/** The external download leaf saves only to the native owner's supplied path. */
export function makeExternalBrowserDownload(
  save: (path: string) => Promise<void>,
  beforeReady: () => Promise<void> = async () => {},
) {
  const saves: string[] = [];
  const download = {
    url: () => "https://example.test/report.txt",
    suggestedFilename: () => "report.txt",
    failure: async () => {
      await beforeReady();
      return null;
    },
    saveAs: async (path: string) => {
      saves.push(path);
      await save(path);
    },
    // Upstream deletes the artifact after saveAs so cached contexts do not keep it.
    delete: async () => {},
  };
  return { download: download as unknown as Download, saves };
}

/** Actual external picker leaf; the native owner still controls identity and sequencing. */
export function makeExternalBrowserFileChooser<
  T extends
    | string
    | Extract<Parameters<FileChooser["setFiles"]>[0], ReadonlyArray<{ name: string }>>[number] =
    string,
>(onFiles: (paths: ReadonlyArray<T>) => Promise<void>, multiple = false) {
  const calls: Array<ReadonlyArray<T>> = [];
  const chooser = {
    element: () => ({ getAttribute: async () => "" }),
    isMultiple: () => multiple,
    setFiles: async (paths: ReadonlyArray<T>) => {
      calls.push(paths);
      await onFiles(paths);
    },
  };
  return { chooser: chooser as unknown as FileChooser, calls };
}
