/** Keep the host platform and engine version, excluding every app product token. */
export const chromeUserAgent = (electronUserAgent: string): string => {
  const match =
    /^(Mozilla\/5\.0 \([^)]+\) AppleWebKit\/[\d.]+ \(KHTML, like Gecko\)) .*?\bChrome\/([\d.]+).*?\bSafari\/([\d.]+)/.exec(
      electronUserAgent,
    );
  if (!match) throw new Error("Preview user agent is missing the desktop Chromium identity.");
  return `${match[1]} Chrome/${match[2]} Safari/${match[3]}`;
};
