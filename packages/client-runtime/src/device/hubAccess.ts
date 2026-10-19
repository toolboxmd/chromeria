/** Credentials for media requests that cannot set bearer or DPoP headers. */
export interface DeviceHubAccess {
  /** Absolute source environment URL for the fixed device or browser media route. */
  readonly httpBase: string;
  /** Same base with the `ws(s)` scheme. */
  readonly wsBase: string;
  /** Empty for cookie sessions; includes a short-lived ticket for bearer and DPoP sessions. */
  readonly query: Readonly<Record<string, string>>;
  /** Whether requests must include session cookies. */
  readonly credentials: boolean;
  /** Derived from the guarded admission, only for local input availability. The server authorizes every input. */
  readonly canOperate?: boolean;
  /** Expiry of the media identity credential or exact resource viewer claim. */
  readonly expiresAt?: number;
  /** Source-owned viewer lifetime, separate from the device tool session. */
  readonly viewer?: { readonly id: string; readonly expiresAt: string };
}

export const withDeviceHubQuery = (url: string, access: DeviceHubAccess): string => {
  const entries = Object.entries(access.query);
  if (entries.length === 0) return url;
  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}${new URLSearchParams(entries).toString()}`;
};
