// @effect-diagnostics nodeBuiltinImport:off globalTimers:off - the Cue client times the TCP connect apart from the reply, which fetch cannot, and runs outside Effect beside the synchronous serving state.
/**
 * Asks the Cue service to score a moment's window without holding a local lane: `pre_tool` starts a
 * request its `tool_result` collects; others score at once. A connect failure skips Cue machine-wide
 * until a health probe passes; timeouts, 4xx rejections and lost collects affect one call only.
 */
import * as NodeHttp from "node:http";

/** At most how long a tool result waits for the score its pre-tool moment started. */
export const COLLECT_MS = 300;
/** The service drops a request still queued after this. */
const DEADLINE_MS = 1500;
const CONNECT_MS = 500;
export const PROBE_SECONDS = 30;
const PENDING_SECONDS = 600;
const MAX_PENDING = 256;

export type CueStatus =
  | "ok"
  | "started"
  | "not_called"
  | "no_window"
  | "timeout"
  | "connect_failed"
  | "overload"
  | "rejected"
  | "lost"
  | "error";

export interface CueRequest {
  readonly items: ReadonlyArray<readonly [kind: "user" | "asst" | "tool", text: string]>;
  readonly moment: { readonly kind: string; readonly call?: string; readonly output?: string };
  readonly entries: ReadonlyArray<{ readonly id: string; readonly doc: string }>;
}

export interface CueReply {
  readonly status: CueStatus;
  /** Round trip as this caller saw it, from request start to reply or failure. */
  readonly ms: number | null;
  /** How long the moment itself waited for it. */
  readonly waitMs: number | null;
  readonly adapter?: string;
  readonly threshold?: number;
  readonly scores?: Readonly<Record<string, number>>;
  /** Service-side timing and window size, for the log (never the text). */
  readonly detail?: Readonly<Record<string, unknown>>;
}

/** One HTTP exchange with the service: resolves, never rejects. */
export type Transport = (
  method: "GET" | "POST",
  path: string,
  body: string | null,
  timeoutMs: number,
) => Promise<
  | { readonly kind: "reply"; readonly status: number; readonly body: string }
  | {
      readonly kind: "connect_failed" | "timeout";
    }
>;

export const httpTransport =
  (base: string): Transport =>
  (method, path, body, timeoutMs) =>
    new Promise((resolve) => {
      let url: URL;
      try {
        url = new URL(path, base);
      } catch {
        resolve({ kind: "connect_failed" });
        return;
      }
      // Only plain HTTP over the tailnet: any other scheme can never connect.
      if (url.protocol !== "http:") {
        resolve({ kind: "connect_failed" });
        return;
      }
      let done = false;
      const finish = (r: Awaited<ReturnType<Transport>>) => {
        if (done) return;
        done = true;
        clearTimeout(connectTimer);
        clearTimeout(totalTimer);
        req.destroy();
        resolve(r);
      };
      let req: NodeHttp.ClientRequest;
      try {
        req = NodeHttp.request(url, {
          method,
          headers: body === null ? {} : { "content-type": "application/json" },
        });
      } catch {
        resolve({ kind: "connect_failed" });
        return;
      }
      const connectTimer = setTimeout(() => finish({ kind: "connect_failed" }), CONNECT_MS);
      const totalTimer = setTimeout(() => finish({ kind: "timeout" }), timeoutMs);
      req.on("socket", (socket) => {
        if (!socket.connecting) clearTimeout(connectTimer);
        else socket.once("connect", () => clearTimeout(connectTimer));
      });
      req.on("error", (cause: NodeJS.ErrnoException) =>
        finish({
          kind: ["ECONNREFUSED", "EHOSTUNREACH", "ENETUNREACH", "ENOTFOUND", "EAI_AGAIN"].includes(
            cause.code ?? "",
          )
            ? "connect_failed"
            : "timeout",
        }),
      );
      req.on("response", (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () =>
          finish({
            kind: "reply",
            status: res.statusCode ?? 0,
            body: Buffer.concat(chunks).toString("utf8"),
          }),
        );
        res.on("error", () => finish({ kind: "timeout" }));
      });
      req.end(body ?? undefined);
    });

interface Pending {
  readonly startedAt: number;
  readonly result: Promise<CueReply>;
}

const parseScore = (status: number, body: string, ms: number): CueReply => {
  if (status === 503) return { status: "overload", ms, waitMs: null };
  if (status >= 400 && status < 500) {
    let reason = "";
    try {
      const error = (JSON.parse(body) as Record<string, unknown>)["error"];
      // The service names the problem (`NoMessage: no user message`); keep it short, it is not window text.
      reason = typeof error === "string" ? error.slice(0, 120) : "";
    } catch {
      reason = "";
    }
    return { status: "rejected", ms, waitMs: null, detail: { http: status, error: reason } };
  }
  if (status !== 200) return { status: "error", ms, waitMs: null, detail: { http: status } };
  try {
    const out = JSON.parse(body) as Record<string, unknown>;
    const scores = out["scores"];
    if (
      typeof out["adapter"] !== "string" ||
      typeof out["threshold"] !== "number" ||
      typeof scores !== "object" ||
      scores === null ||
      !Object.values(scores).every((s) => typeof s === "number" && Number.isFinite(s))
    ) {
      return { status: "error", ms, waitMs: null, detail: { reply: "shape" } };
    }
    const { adapter, threshold, scores: _scores, ...detail } = out;
    return {
      status: "ok",
      ms,
      waitMs: null,
      adapter: adapter as string,
      threshold: threshold as number,
      scores: scores as Record<string, number>,
      detail,
    };
  } catch {
    return { status: "error", ms, waitMs: null, detail: { reply: "json" } };
  }
};

/** What `/health` returns while Cue is actually serving. A status line alone is not enough. */
const healthDocument = (body: string): boolean => {
  try {
    const v = JSON.parse(body) as Record<string, unknown>;
    return v["ok"] === true && typeof v["adapter"] === "string" && v["adapter"] !== "";
  } catch {
    return false;
  }
};

export class CueClient {
  private available: boolean;
  private probing = false;
  private readonly pending = new Map<string, Pending>();
  private readonly transport: Transport;
  private readonly clock: () => number;
  private readonly onAvailability: ((available: boolean) => void) | undefined;

  constructor(
    transport: Transport,
    clock: () => number = Date.now,
    initiallyAvailable = true,
    onAvailability?: (available: boolean) => void,
  ) {
    this.transport = transport;
    this.clock = clock;
    this.available = initiallyAvailable;
    this.onAvailability = onAvailability;
  }

  private setAvailable(available: boolean) {
    if (this.available === available) return;
    this.available = available;
    // Recording the belief is best-effort: a disk error must not fail the moment.
    try {
      this.onAvailability?.(available);
    } catch {
      return;
    }
  }

  /** Whether the service is believed reachable (false between a connect failure and a good probe). */
  isAvailable() {
    return this.available;
  }

  /** Never rejects: a transport that throws counts as an error for this call. */
  private request(req: CueRequest): Promise<CueReply> {
    const t0 = this.clock();
    return Promise.resolve()
      .then(() =>
        this.transport(
          "POST",
          "/score",
          JSON.stringify({ ...req, deadline_ms: DEADLINE_MS }),
          DEADLINE_MS + 500,
        ),
      )
      .then((r) => {
        const ms = this.clock() - t0;
        if (r.kind !== "reply") {
          if (r.kind === "connect_failed") this.setAvailable(false);
          return { status: r.kind, ms, waitMs: null } satisfies CueReply;
        }
        return parseScore(r.status, r.body, ms);
      })
      .catch((): CueReply => ({ status: "error", ms: this.clock() - t0, waitMs: null }));
  }

  /** One health probe; marks the service available again only for a Cue health document. */
  async probe(): Promise<boolean> {
    if (this.probing) return this.available;
    this.probing = true;
    try {
      const r = await this.transport("GET", "/health", null, 2000);
      if (r.kind === "reply" && r.status === 200 && healthDocument(r.body)) this.setAvailable(true);
      return this.available;
    } catch {
      return this.available;
    } finally {
      this.probing = false;
    }
  }

  private prune(now: number) {
    for (const [key, p] of this.pending) {
      if (now - p.startedAt > PENDING_SECONDS * 1000 || this.pending.size > MAX_PENDING) {
        this.pending.delete(key);
      }
    }
  }

  /** Start scoring for a call; its tool result collects it. Never waits. */
  start(key: string, req: CueRequest): CueReply {
    const now = this.clock();
    this.prune(now);
    if (!this.available) {
      const skipped: CueReply = { status: "not_called", ms: null, waitMs: null };
      this.pending.set(key, { startedAt: now, result: Promise.resolve(skipped) });
      return skipped;
    }
    this.pending.set(key, { startedAt: now, result: this.request(req) });
    return { status: "started", ms: null, waitMs: 0 };
  }

  /** The score a pre-tool moment started, waiting at most `waitMs` for it. */
  async collect(key: string, waitMs: number = COLLECT_MS): Promise<CueReply> {
    const p = this.pending.get(key);
    if (p === undefined) {
      return { status: this.available ? "lost" : "not_called", ms: null, waitMs: 0 };
    }
    const t0 = this.clock();
    const r = await this.within(p.result, waitMs);
    const waited = this.clock() - t0;
    if (r === null) return { status: "timeout", ms: null, waitMs: waited };
    this.pending.delete(key);
    return { ...r, waitMs: waited };
  }

  /** Drop a started request whose call failed: the failure is rescored with its output. */
  drop(key: string) {
    this.pending.delete(key);
  }

  /** Score now and wait at most `waitMs`. */
  async score(req: CueRequest, waitMs: number): Promise<CueReply> {
    if (!this.available) return { status: "not_called", ms: null, waitMs: 0 };
    const t0 = this.clock();
    const r = await this.within(this.request(req), waitMs);
    const waited = this.clock() - t0;
    if (r === null) return { status: "timeout", ms: null, waitMs: waited };
    return { ...r, waitMs: waited };
  }

  private within<A>(promise: Promise<A>, ms: number): Promise<A | null> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => resolve(null), ms);
      void promise.then((a) => {
        clearTimeout(timer);
        resolve(a);
      });
    });
  }
}

/** A projected turn item, as the serving module reads it for the window. */
export interface TranscriptRow {
  readonly ordinal: number;
  readonly type: string;
  readonly payload_json: string;
}

const ITEM_CHARS = 12_000;
const TOOL_CHARS = 300;

const clip = (text: string, max: number) =>
  text.length <= max ? text : `${text.slice(0, max / 2)}[...]${text.slice(text.length - max / 2)}`;

/**
 * The window items for a thread's projected turn items, oldest first: user and agent messages, and one short
 * line per tool call. Each text is capped; the service's window budget trims further.
 */
export const transcriptItems = (rows: ReadonlyArray<TranscriptRow>): CueRequest["items"] => {
  const out: Array<readonly ["user" | "asst" | "tool", string]> = [];
  for (const row of rows) {
    let p: Record<string, unknown>;
    try {
      p = JSON.parse(row.payload_json) as Record<string, unknown>;
    } catch {
      continue;
    }
    const str = (v: unknown) => (typeof v === "string" ? v : v == null ? "" : JSON.stringify(v));
    if (row.type === "user_message" && typeof p["text"] === "string") {
      out.push(["user", clip(p["text"], ITEM_CHARS)]);
    } else if (row.type === "assistant_message" && typeof p["text"] === "string") {
      if (p["text"].trim()) out.push(["asst", clip(p["text"], ITEM_CHARS)]);
    } else if (
      ["command_execution", "dynamic_tool", "file_change", "file_search", "web_search"].includes(
        row.type,
      )
    ) {
      const name = str(p["toolName"]) || row.type;
      const what = str(p["input"]) || str(p["fileName"]) || str(p["pattern"]) || str(p["patterns"]);
      out.push(["tool", clip(`${name}: ${what}`, TOOL_CHARS)]);
    }
  }
  return out;
};
