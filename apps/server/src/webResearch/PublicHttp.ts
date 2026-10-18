// @effect-diagnostics nodeBuiltinImport:off - DNS pinning and bounded HTTP decompression use Node networking APIs.
import * as NodeDnsPromises from "node:dns/promises";
import * as NodeNet from "node:net";
import * as NodeZlib from "node:zlib";
import * as NodeUtil from "node:util";
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import * as ByteSize from "effect/ByteSize";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpIncomingMessage from "effect/http/HttpIncomingMessage";

import { isLocalAddress } from "../htmlRender/publicProxy.ts";

const USER_AGENT = "ChromeriaResearch/1.0 (+https://github.com/toolboxmd/chromeria)";
const MAX_BYTES = 4 * 1024 * 1024;
const unzip = NodeUtil.promisify(NodeZlib.unzip);
const brotli = NodeUtil.promisify(NodeZlib.brotliDecompress);

export class PublicNetworkBlockedError extends Schema.TaggedError<PublicNetworkBlockedError>()(
  "PublicNetworkBlockedError",
  { host: Schema.String },
) {
  override get message() {
    return "Research reads may only connect to public network addresses.";
  }
}
export class PublicHttpError extends Schema.TaggedError<PublicHttpError>()("PublicHttpError", {
  host: Schema.String,
  stage: Schema.Literals(["resolve", "request", "decode"]),
  cause: Schema.Defect(),
}) {
  override get message() {
    return "The public page request failed.";
  }
}

const isNetworkBlocked = Schema.is(PublicNetworkBlockedError);

export class PublicHttp extends Context.Service<
  PublicHttp,
  {
    readonly get: (url: URL) => Effect.Effect<
      {
        readonly status: number;
        readonly headers: Readonly<Record<string, string>>;
        readonly body: string;
      },
      PublicNetworkBlockedError | PublicHttpError
    >;
  }
>()("t3/webResearch/PublicHttp") {}

const resolve = async (host: string) => {
  const bare = host.replace(/^\[|\]$/g, "");
  const family = NodeNet.isIP(bare);
  const addresses = family
    ? [{ address: bare, family }]
    : await NodeDnsPromises.lookup(bare, { all: true, verbatim: true });
  if (addresses.some((entry) => isLocalAddress(entry.address, entry.family)))
    throw new PublicNetworkBlockedError({ host: bare });
  if (addresses.length === 0) throw new Error("No public address.");
  return addresses;
};

const make = Effect.gen(function* () {
  const client = yield* HttpClient.HttpClient;
  const get = Effect.fn("PublicHttp.get")(function* (url: URL) {
    // Literal IPs bypass an agent's DNS lookup, so validate them here too.
    yield* Effect.tryPromise({
      try: () => resolve(url.hostname),
      catch: (cause) =>
        isNetworkBlocked(cause)
          ? cause
          : new PublicHttpError({ host: url.hostname, stage: "resolve", cause }),
    });
    const response = yield* client
      .get(url.href, {
        headers: {
          "user-agent": USER_AGENT,
          accept: "text/html, text/plain;q=0.9, application/xhtml+xml;q=0.8",
          "accept-encoding": "identity",
        },
      })
      .pipe(
        Effect.mapError((cause) =>
          isNetworkBlocked(cause.reason.cause)
            ? cause.reason.cause
            : new PublicHttpError({ host: url.hostname, stage: "request", cause }),
        ),
      );
    const raw = yield* response.arrayBuffer.pipe(
      Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.bytes(MAX_BYTES)),
      Effect.mapError(
        (cause) => new PublicHttpError({ host: url.hostname, stage: "decode", cause }),
      ),
    );
    const body = yield* Effect.tryPromise({
      try: async () => {
        const encoding = response.headers["content-encoding"]?.toLowerCase();
        const bytes =
          encoding === "gzip" || encoding === "deflate"
            ? await unzip(Buffer.from(raw), { maxOutputLength: MAX_BYTES })
            : encoding === "br"
              ? await brotli(Buffer.from(raw), { maxOutputLength: MAX_BYTES })
              : new Uint8Array(raw);
        const charset =
          /charset\s*=\s*["']?([\w-]+)/i.exec(response.headers["content-type"] ?? "")?.[1] ??
          "utf-8";
        return new TextDecoder(charset).decode(bytes);
      },
      catch: (cause) => new PublicHttpError({ host: url.hostname, stage: "decode", cause }),
    });
    return { status: response.status, headers: response.headers, body };
  });
  return PublicHttp.of({ get });
});

// Every actual connection receives only checked addresses. A DNS preflight alone permits rebinding.
const agents = NodeHttpClient.layerAgentOptions({
  keepAlive: true,
  lookup: (host, options, callback) => {
    void resolve(host).then(
      (addresses) =>
        options.all
          ? callback(null, addresses)
          : callback(null, addresses[0]!.address, addresses[0]!.family),
      (cause: Error) => callback(cause, [], 0),
    );
  },
});
export const layer = Layer.effect(PublicHttp, make).pipe(
  Layer.provide(NodeHttpClient.layerNodeHttpNoAgent.pipe(Layer.provide(agents))),
);
