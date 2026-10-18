import { parse, type DefaultTreeAdapterTypes } from "parse5";
import type { PageStatus } from "./schemas.ts";

type Node = DefaultTreeAdapterTypes.Node;
type Element = DefaultTreeAdapterTypes.Element;

const children = (node: Node): ReadonlyArray<Node> => ("childNodes" in node ? node.childNodes : []);
const attribute = (element: Element, name: string) =>
  element.attrs.find((attr) => attr.name === name)?.value;
const plain = (node: Node): string =>
  "value" in node ? node.value : children(node).map(plain).join("");
const compact = (text: string) => text.replace(/\s+/g, " ").trim();
const blocks = new Set([
  "address",
  "article",
  "aside",
  "blockquote",
  "div",
  "dl",
  "dt",
  "dd",
  "fieldset",
  "figcaption",
  "figure",
  "footer",
  "header",
  "main",
  "nav",
  "ol",
  "p",
  "section",
  "ul",
]);
const omitted = new Set(["style", "template", "svg", "canvas", "head"]);

/** Full-document extraction: article-only readers lose venue tables and structured facts. */
export const extract = (html: string, url: string) => {
  const document = parse(html);
  let title = "";
  let refresh: string | undefined;
  let password = false;
  let form = false;
  const structured: Array<string> = [];
  const visit = (node: Node): string => {
    if ("value" in node) return node.value.replace(/\s+/g, " ");
    if (!("tagName" in node)) return children(node).map(visit).join("");
    const tag = node.tagName;
    if (tag === "title") title = compact(plain(node)).slice(0, 500);
    if (tag === "input" && attribute(node, "type")?.toLowerCase() === "password") password = true;
    if (tag === "form") form = true;
    if (tag === "meta" && attribute(node, "http-equiv")?.toLowerCase() === "refresh") {
      const match = /^\s*\d+(?:\.\d+)?\s*;\s*url\s*=\s*(.*?)\s*$/i.exec(
        attribute(node, "content") ?? "",
      );
      if (match?.[1]) refresh = match[1].replace(/^['"]|['"]$/g, "");
    }
    if (tag === "script") {
      if (attribute(node, "type")?.toLowerCase() === "application/ld+json") {
        structured.push(plain(node).trim());
      }
      return "";
    }
    // Inspect metadata before dropping head so redirects and JSON-LD are retained.
    const inner = children(node).map(visit).join("");
    if (
      omitted.has(tag) ||
      attribute(node, "hidden") !== undefined ||
      attribute(node, "aria-hidden") === "true"
    )
      return "";
    if (/^h[1-6]$/.test(tag)) return `\n\n${"#".repeat(Number(tag[1]))} ${inner.trim()}\n\n`;
    if (tag === "br") return "\n";
    if (tag === "li") return `\n- ${inner.trim()}\n`;
    if (tag === "a") {
      const href = attribute(node, "href");
      if (href) {
        try {
          const target = new URL(href, url);
          if (["http:", "https:"].includes(target.protocol))
            return `[${inner.trim() || target.href}](${target.href})`;
        } catch {
          /* A malformed link is still readable as text. */
        }
      }
    }
    if (tag === "tr") return `\n| ${inner.trim()}\n`;
    if (tag === "td" || tag === "th") return `${compact(inner).replaceAll("|", "\\|")} | `;
    if (tag === "table") return `\n\n${inner.trim()}\n\n`;
    return blocks.has(tag) ? `\n\n${inner.trim()}\n\n` : inner;
  };
  const rendered = visit(document)
    .replace(/[\t ]+\n/g, "\n")
    .replace(/\n[\t ]+/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  const text = [rendered, ...structured.map((json) => `JSON-LD:\n${json}`)]
    .filter(Boolean)
    .join("\n\n");
  return { title, text, refresh, password, form, visibleText: compact(rendered) };
};

/** Classify the document itself, so a footer's login link is not mistaken for a wall. */
export const classify = (
  page: ReturnType<typeof extract>,
  status: number,
): typeof PageStatus.Type => {
  const title = page.title.toLowerCase();
  const lead = page.visibleText.slice(0, 2000).toLowerCase();
  const short = page.visibleText.length < 4000;
  if (
    status === 429 ||
    /just a moment|attention required|security check|verify (?:that )?you are human|access denied|robot check/.test(
      title,
    ) ||
    (short &&
      /checking your browser|verify (?:that )?you are human|complete the (?:security )?captcha|cf-chl-|unusual traffic|suspected automated|try again later/.test(
        lead,
      ))
  )
    return "challenge";
  if (status >= 400)
    return status === 401 ? "login" : status === 403 ? "challenge" : "origin_error";
  if (
    /\b(?:500 internal server error|502 bad gateway|503 service unavailable|504 gateway|website not found|site not found|account suspended|domain (?:is )?for sale|error (?:500|502|503|504|520|521|522|523|524))\b/.test(
      title,
    ) ||
    (short &&
      /\b(?:origin (?:is )?(?:unreachable|down)|(?:http\s+|error code[: ]+)?(?:500 internal server error|502 bad gateway|503 service unavailable|504 gateway timeout)|error code[: ]+(?:500|502|503|504|520|521|522|523|524)|this domain (?:is|may be) for sale|buy this domain|domain has expired|upstream connect error)\b/.test(
        lead,
      ))
  )
    return "origin_error";
  if (
    (page.password && short) ||
    /^(?:log ?in|sign ?in)(?:\s*[|:-]|$)/.test(title) ||
    (short &&
      /(?:log ?in|sign ?in) to (?:continue|view|see)|you must (?:log ?in|sign ?in)|you need to (?:log ?in|sign ?in)/.test(
        lead,
      ))
  )
    return "login";
  if (
    /before you continue|consent required|cookie consent|privacy choices/.test(title) ||
    (short &&
      /before you continue to|(?:please )?(?:accept|enable) (?:all )?cookies to (?:continue|view|access)|consent (?:is )?required/.test(
        lead,
      ))
  )
    return "consent";
  return page.visibleText.length < 80 && !page.text.includes("JSON-LD:") ? "thin" : "ok";
};
