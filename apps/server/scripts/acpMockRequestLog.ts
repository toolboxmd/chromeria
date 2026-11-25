// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";

// A stdin chunk can end mid-message, so the request log gets only complete lines.
export function makeAcpMockRequestLogAppender(path: string): (chunk: string) => void {
  let pending = "";
  return (chunk) => {
    const end = chunk.lastIndexOf("\n") + 1;
    if (end === 0) {
      pending += chunk;
      return;
    }
    NodeFS.appendFileSync(path, pending + chunk.slice(0, end), "utf8");
    pending = chunk.slice(end);
  };
}
