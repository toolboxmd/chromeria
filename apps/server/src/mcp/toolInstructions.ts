/**
 * Fork (toolboxmd/t3code#46): when to reach for the `t3-code` tools.
 *
 * Harnesses that defer tool schemas show agents only tool names, and Grok not
 * even those, so an agent never learns how to route work with `spawn_thread`. `buildRuntimeInstructions` appends this to every harness's
 * launch prompt. MCP server `instructions` (Effect 4.0.0-rc.116+) would reach
 * Claude Code, Codex, OpenCode and Grok, but not Cursor over ACP, and
 * Antigravity only writes them to a file, so do not move it there. Adding it
 * to both would make Claude Code show it twice.
 */
const T3_CODE_TOOL_INSTRUCTIONS =
  "Reach for the t3-code tools when work should stay visible in this T3 Code thread: delegating to other agents, testing web or native UI, and linking Issues. Delegate with them, never by starting codex exec, claude -p, opencode run or grok in the shell: the user cannot see those runs. Delegate with spawn_thread and a role (worker with a lane by difficulty, reviewer); Prism picks the model. For a job that ends in one PR, start spawn_thread(role: dispatcher) with the brief, unless the user or a comparison job says to coordinate it yourself. When a child fails, hits a limit or goes stale, interrupt_thread it and, once it reports settled, spawn retry, then escalation, then ask whoever started you. For small direct work, or when a specific model and effort is wanted, call spawn_thread, then read_thread and message_thread. You own your children's approvals and questions in every runtime mode, including approval-required. Children with reportBack notify you once per pending request. Use read_thread to inspect waiting requests, then pending_request_respond with decision for approvals or answers keyed by question id for input; the user can still answer from the child thread. For web UI: preview_open, preview_snapshot, then act. For simulators: device_list, then device_open.";

/** The launch-prompt block `buildRuntimeInstructions` appends. */
export const T3_CODE_TOOL_USE_BLOCK = `<t3_code_tool_use>\n${T3_CODE_TOOL_INSTRUCTIONS}\n</t3_code_tool_use>`;
