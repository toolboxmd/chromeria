/**
 * Messages other agents send into a thread arrive as ordinary user messages
 * whose first line names the sender (built by the server's `threads` MCP
 * toolkit, apps/server/src/mcp/toolkits/threads/handlers.ts). Web and mobile
 * timelines parse that header to show them compactly instead of as user bubbles.
 */
export interface AgentMessage {
  kind: "report" | "message";
  threadId: string;
  title: string;
  body: string;
  /** First non-empty body line without its markdown block marker, for the collapsed row. */
  preview: string;
}

// Titles may contain parentheses or brackets, so anchor on the line's end.
const REPORT_HEADER = /^\[Subagent (.+) \(thread ([^\s()]+)\) finished a turn\]$/;
const MESSAGE_HEADER = /^\[Message from (.+) \(thread ([^\s()]+)\)\]$/;
// Heading, list, numbered list and quote markers render as formatting, not text.
const BLOCK_MARKER = /^(?:#{1,6}|[-*+]|\d+[.)]|>)\s+/;

/** The sender and body of an agent-sent message, or null for a user's own message. */
export function parseAgentMessage(text: string): AgentMessage | null {
  if (!text.startsWith("[")) return null;
  const newline = text.indexOf("\n");
  const header = newline === -1 ? text : text.slice(0, newline);
  const report = REPORT_HEADER.exec(header);
  const match = report ?? MESSAGE_HEADER.exec(header);
  if (!match) return null;
  const body = newline === -1 ? "" : text.slice(newline + 1).replace(/^\n/, "");
  return {
    kind: report ? "report" : "message",
    title: match[1]!,
    threadId: match[2]!,
    body,
    preview:
      body
        .split("\n")
        .find((line) => line.trim().length > 0)
        ?.trim()
        .replace(BLOCK_MARKER, "") ?? "",
  };
}
