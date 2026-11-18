// @effect-diagnostics globalFetch:off - This desktop-main adapter keeps the mirror grant out of the renderer.
import {
  COOKIE_MIRROR_GRANT_PATH,
  COOKIE_MIRROR_PATH,
  CookieMirrorGrant,
  type CookieMirrorConnectionInput,
} from "@t3tools/contracts";
import { environmentEndpointUrl } from "@t3tools/client-runtime/environment";
import * as Schema from "effect/Schema";

const decode = Schema.decodeUnknownSync(CookieMirrorGrant);

export async function requestCookieMirrorGrant(
  input: Pick<
    Extract<CookieMirrorConnectionInput, { readonly action: "connect" }>,
    "environmentId"
  >,
  resolve: (environmentId: string) => Promise<CookieMirrorDestination>,
): Promise<string> {
  try {
    const destination = await resolve(input.environmentId);
    const response = await fetch(
      environmentEndpointUrl(destination.httpBaseUrl, COOKIE_MIRROR_GRANT_PATH),
      {
        method: "POST",
        headers: destination.headers,
        redirect: "error",
        signal: AbortSignal.timeout(10_000),
      },
    );
    if (!response.ok) throw new Error("Grant rejected");
    const grant = decode(await response.json());
    if (grant.environmentId !== input.environmentId) throw new Error("Wrong environment");
    const url = new URL(environmentEndpointUrl(destination.httpBaseUrl, COOKIE_MIRROR_PATH));
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set("wsTicket", grant.ticket);
    return url.toString();
  } catch {
    throw new Error("Could not authorize browser cookie mirroring.");
  }
}

export interface CookieMirrorDestination {
  readonly httpBaseUrl: string;
  readonly headers: { readonly authorization: string; readonly dpop?: string };
}
