import {
  cookieMirrorKey,
  LOGIN_IMPORT_MAX_BYTES,
  LOGIN_IMPORT_MAX_COOKIES,
  type CookieMirrorMutation,
  type MirrorCookie,
} from "@t3tools/contracts";

import { isAntiBotVerdictCookie } from "./browserCookieMirror.ts";

/** The host's eTLD+1 under the public suffix list, or undefined for a suffix, IP or single label. */
export type RegistrableDomain = (host: string) => string | undefined;

const GOOGLE_CREDENTIAL_NAME = /^(?:__Secure-[13]P)?(?:SID|PSID|APISID|PAPISID|LSID|HSID|SSID)$/;

const registrableLabel = (host: string, registrableDomain: RegistrableDomain) =>
  registrableDomain(host)?.split(".", 1)[0];

/** Google account sign-in is device-bound, so each machine signs in itself. */
export const isGoogleHost = (host: string, registrableDomain: RegistrableDomain) =>
  registrableLabel(host, registrableDomain) === "google";

const isGoogleCredential = (cookie: MirrorCookie, registrableDomain: RegistrableDomain) => {
  const label = registrableLabel(cookie.domain.replace(/^\./, ""), registrableDomain);
  return (label === "google" || label === "youtube") && GOOGLE_CREDENTIAL_NAME.test(cookie.name);
};

/**
 * Whether the browser sends the cookie to some page of `host`: a host-only cookie
 * only to its own host, a domain cookie to its domain and every subdomain. Chrome
 * refuses domain cookies on a public suffix, so none cover anything here. Every
 * path counts, because the agent may open any page of the host.
 */
const cookieCoversHost = (
  cookie: Pick<MirrorCookie, "domain">,
  host: string,
  registrableDomain: RegistrableDomain,
) => {
  const domain = cookie.domain.toLowerCase();
  if (!domain.startsWith(".")) return domain === host;
  const base = domain.slice(1);
  if (registrableDomain(base) === undefined) return false;
  return host === base || host.endsWith(`.${base}`);
};

const encoder = new TextEncoder();
const cookieBytes = (cookie: MirrorCookie) =>
  encoder.encode(cookie.name).byteLength + encoder.encode(cookie.value).byteLength;

export type LoginSelection =
  | {
      readonly _tag: "selected";
      readonly cookies: ReadonlyArray<MirrorCookie>;
      readonly skipped: { readonly deviceBound: number; readonly partitioned: number };
    }
  | { readonly _tag: "refused"; readonly reason: "device_bound" | "too_many_cookies" };

/**
 * The cookies a browser would send to the listed hosts, as one bounded batch.
 * Partitioned cookies belong to another top-level site and Google credentials to
 * this device, so both stay behind. Over the caps, nothing is selected.
 */
export const selectLoginCookies = (input: {
  readonly cookies: ReadonlyArray<MirrorCookie>;
  readonly hosts: ReadonlyArray<string>;
  readonly registrableDomain: RegistrableDomain;
}): LoginSelection => {
  const { hosts, registrableDomain } = input;
  if (hosts.some((host) => isGoogleHost(host, registrableDomain)))
    return { _tag: "refused", reason: "device_bound" };
  const cookies: MirrorCookie[] = [];
  let deviceBound = 0;
  let partitioned = 0;
  let bytes = 0;
  for (const cookie of input.cookies) {
    if (!hosts.some((host) => cookieCoversHost(cookie, host, registrableDomain))) continue;
    if (isGoogleCredential(cookie, registrableDomain) || isAntiBotVerdictCookie(cookie.name))
      deviceBound++;
    else if (cookie.partitionKey !== undefined) partitioned++;
    else {
      cookies.push(cookie);
      bytes += cookieBytes(cookie);
    }
  }
  if (cookies.length > LOGIN_IMPORT_MAX_COOKIES || bytes > LOGIN_IMPORT_MAX_BYTES)
    return { _tag: "refused", reason: "too_many_cookies" };
  return { _tag: "selected", cookies, skipped: { deviceBound, partitioned } };
};

export type LoginImportPlan =
  | {
      readonly _tag: "planned";
      readonly mutations: ReadonlyArray<CookieMirrorMutation>;
      readonly imported: number;
      readonly sharedDomain: number;
    }
  | { readonly _tag: "refused"; readonly reason: "device_bound" | "too_many_cookies" };

/**
 * Writes the snapshot over the destination. A host-only cookie of a listed host
 * that the snapshot lacks is the source's sign-out, so it goes. A domain cookie
 * also reaches unlisted hosts, so one the snapshot lacks stays and is counted.
 * The snapshot is selected again here: a source cannot write outside the list.
 */
export const planLoginImport = (input: {
  readonly current: ReadonlyArray<MirrorCookie>;
  readonly snapshot: ReadonlyArray<MirrorCookie>;
  readonly hosts: ReadonlyArray<string>;
  readonly registrableDomain: RegistrableDomain;
}): LoginImportPlan => {
  const { hosts, registrableDomain } = input;
  const selected = selectLoginCookies({ cookies: input.snapshot, hosts, registrableDomain });
  if (selected._tag === "refused") return selected;
  const incoming = new Set(selected.cookies.map(cookieMirrorKey));
  const mutations: CookieMirrorMutation[] = selected.cookies.map((cookie) => ({
    type: "set",
    cookie,
  }));
  let sharedDomain = 0;
  for (const cookie of input.current) {
    if (
      cookie.partitionKey !== undefined ||
      incoming.has(cookieMirrorKey(cookie)) ||
      isGoogleCredential(cookie, registrableDomain) ||
      isAntiBotVerdictCookie(cookie.name) ||
      !hosts.some((host) => cookieCoversHost(cookie, host, registrableDomain))
    )
      continue;
    if (cookie.domain.startsWith(".")) sharedDomain++;
    else {
      const { name, domain, path } = cookie;
      mutations.push({ type: "remove", key: { name, domain, path } });
    }
  }
  return { _tag: "planned", mutations, imported: selected.cookies.length, sharedDomain };
};
