/**
 * Fork (toolboxmd/chromeria#66): tool `_meta` that keeps a tool out of Claude
 * Code's deferred tool search, so its description is in context from the first
 * turn. Only the delegation tools carry it; an agent that sees only their names
 * starts agent CLIs in the shell instead, which the user cannot see.
 */
export const ALWAYS_LOAD_META = { "anthropic/alwaysLoad": true } as const;
