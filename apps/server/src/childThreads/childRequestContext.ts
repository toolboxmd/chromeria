import type { OrchestrationMessageContext } from "@t3tools/contracts";

/** The relay owns a wake's child attribution; an edit's context can neither change nor drop it. */
export function keepChildRequestAttribution<
  C extends { readonly context?: OrchestrationMessageContext | undefined },
>(next: C, original: OrchestrationMessageContext | undefined): C {
  if (next.context === undefined) return next;
  const { childRequest: _claimed, ...context } = next.context;
  const childRequest = original?.childRequest;
  return {
    ...next,
    context: childRequest === undefined ? context : { ...context, childRequest },
  };
}
