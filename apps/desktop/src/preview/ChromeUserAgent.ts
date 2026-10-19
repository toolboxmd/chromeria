// Include subdomains so another supported site can be added with one entry.
const CHROME_IDENTITY_HOSTS = ["web.whatsapp.com", "whatsapp.net"];

export const usesChromeIdentity = (rawUrl: string): boolean => {
  try {
    const url = new URL(rawUrl);
    return (
      ["https:", "wss:"].includes(url.protocol) &&
      CHROME_IDENTITY_HOSTS.some(
        (host) => url.hostname === host || url.hostname.endsWith(`.${host}`),
      )
    );
  } catch {
    return false;
  }
};

export interface ChromeIdentityHost {
  readonly arch: string;
  readonly platformVersion: string;
}

export const chromeIdentity = (electronUserAgent: string, host: ChromeIdentityHost) => {
  const match =
    /^(Mozilla\/5\.0 \(([^)]+)\) AppleWebKit\/[\d.]+ \(KHTML, like Gecko\)) .*?\bChrome\/((\d+)[\d.]*).*?\bSafari\/([\d.]+)/.exec(
      electronUserAgent,
    );
  const prefix = match?.[1];
  const platformToken = match?.[2];
  const fullVersion = match?.[3];
  const major = match?.[4];
  const safariVersion = match?.[5];
  if (!prefix || !platformToken || !fullVersion || !major || !safariVersion) {
    throw new Error("Preview user agent is missing the desktop Chromium identity.");
  }
  const platform = platformToken.includes("Macintosh")
    ? "macOS"
    : platformToken.includes("Windows")
      ? "Windows"
      : "Linux";
  const brands = [
    { brand: "Not A(Brand", version: "99" },
    { brand: "Chromium", version: major },
    { brand: "Google Chrome", version: major },
  ];
  return {
    userAgent: `${prefix} Chrome/${major}.0.0.0 Safari/${safariVersion}`,
    userAgentMetadata: {
      brands,
      fullVersionList: brands.map(({ brand, version }) => ({
        brand,
        version: brand === "Not A(Brand" ? `${version}.0.0.0` : fullVersion,
      })),
      fullVersion: fullVersion,
      platform,
      platformVersion: host.platformVersion,
      architecture: host.arch.startsWith("arm") ? "arm" : "x86",
      bitness: host.arch.endsWith("64") ? "64" : "32",
      mobile: false,
      model: "",
      wow64: platformToken.includes("WOW64"),
    },
  };
};

export type ChromeIdentity = ReturnType<typeof chromeIdentity>;

const serializeBrands = (brands: ChromeIdentity["userAgentMetadata"]["brands"]) =>
  brands.map(({ brand, version }) => `"${brand}";v="${version}"`).join(", ");

/** Header policy is destination-specific even for subresources of a Chrome-identity tab. */
export const chromeRequestHeaders = (
  headers: Record<string, string>,
  identity: ChromeIdentity,
): Record<string, string> => {
  const { userAgent, userAgentMetadata: metadata } = identity;
  const replacements: Record<string, string> = {
    "user-agent": userAgent,
    "sec-ch-ua": serializeBrands(metadata.brands),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": JSON.stringify(metadata.platform),
    "sec-ch-ua-full-version": JSON.stringify(metadata.fullVersion),
    "sec-ch-ua-full-version-list": serializeBrands(metadata.fullVersionList),
    "sec-ch-ua-platform-version": JSON.stringify(metadata.platformVersion),
    "sec-ch-ua-arch": JSON.stringify(metadata.architecture),
    "sec-ch-ua-bitness": JSON.stringify(metadata.bitness),
    "sec-ch-ua-model": '""',
    "sec-ch-ua-wow64": metadata.wow64 ? "?1" : "?0",
  };
  const next: Record<string, string> = {};
  for (const [name, value] of Object.entries(headers)) {
    next[name] = replacements[name.toLowerCase()] ?? value;
  }
  // High entropy hints still require the site's Accept-CH opt-in.
  for (const name of ["user-agent", "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform"]) {
    if (!Object.keys(next).some((key) => key.toLowerCase() === name))
      next[name] = replacements[name]!;
  }
  return next;
};
