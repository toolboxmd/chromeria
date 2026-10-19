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
  // Chromium 152 user_agent_utils.cc: GREASE and brand permutation are seeded by the major.
  const seed = Number(major);
  const characters = [" ", "(", ":", "-", ".", "/", ")", ";", "=", "?", "_"];
  const greaseBrand = `Not${characters[seed % characters.length]}A${characters[(seed + 1) % characters.length]}Brand`;
  const greaseVersion = ["8", "99", "24"][seed % 3]!;
  const originalBrands = [
    { brand: greaseBrand, version: greaseVersion },
    { brand: "Chromium", version: major },
    { brand: "Google Chrome", version: major },
  ];
  const order = [
    [0, 1, 2],
    [0, 2, 1],
    [1, 0, 2],
    [1, 2, 0],
    [2, 0, 1],
    [2, 1, 0],
  ][seed % 6]!;
  const brands = order.map((_, slot) => originalBrands[order.indexOf(slot)]!);
  return {
    userAgent: `${prefix} Chrome/${major}.0.0.0 Safari/${safariVersion}`,
    userAgentMetadata: {
      brands,
      fullVersionList: brands.map(({ brand, version }) => ({
        brand,
        version: brand === greaseBrand ? `${version}.0.0.0` : fullVersion,
      })),
      fullVersion: fullVersion,
      platform,
      platformVersion: platform === "Linux" ? "" : host.platformVersion,
      architecture: host.arch.startsWith("arm") ? "arm" : "x86",
      bitness: host.arch.endsWith("64") ? "64" : "32",
      mobile: false,
      model: "",
      wow64: platformToken.includes("WOW64"),
    },
  };
};

export type ChromeIdentity = ReturnType<typeof chromeIdentity>;
