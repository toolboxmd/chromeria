export interface DeviceHubAccess {
  readonly requestCurrentAccess?: () => Promise<DeviceHubAccess>;
  readonly httpBase: string;
  readonly wsBase: string;
  readonly query: Readonly<Record<string, string>>;
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
