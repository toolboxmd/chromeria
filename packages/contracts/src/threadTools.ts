import * as Schema from "effect/Schema";

const Name = Schema.String.check(Schema.isPattern(/^[A-Za-z][A-Za-z0-9_-]{0,63}$/));
const ToolName = Schema.String.check(
  Schema.isPattern(
    /^(?:[A-Za-z][A-Za-z0-9_]{0,63}|mcp:[A-Za-z][A-Za-z0-9_-]{0,63}:[A-Za-z][A-Za-z0-9_]{0,63})$/,
  ),
);
const Allow = Schema.Array(ToolName).check(Schema.isMaxLength(128));
export const ThreadMcpServer = Schema.Struct({
  type: Schema.Literal("http"),
  url: Schema.String.check(Schema.isPattern(/^https?:\/\/[^\s]+$/)),
  headers: Schema.optional(Schema.Record(Schema.String, Schema.String)),
});
export type ThreadMcpServer = typeof ThreadMcpServer.Type;
export const ThreadMcpServers = Schema.Record(Name, ThreadMcpServer);
export type ThreadMcpServers = typeof ThreadMcpServers.Type;

/** Only this request contains credentials. Persist ThreadTools instead. */
export const ThreadToolsRequest = Schema.Struct({
  allow: Allow,
  mcpServers: Schema.optional(ThreadMcpServers),
});
export type ThreadToolsRequest = typeof ThreadToolsRequest.Type;
export const ThreadTools = Schema.Struct({
  allow: Allow,
  mcpConfigId: Schema.optional(Schema.String.check(Schema.isPattern(/^[a-f0-9-]{36}$/))),
});
export type ThreadTools = typeof ThreadTools.Type;
