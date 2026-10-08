import { describe, expect, it } from "@effect/vitest";
import { vi } from "vite-plus/test";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSourceControlWriteScope,
  EnvironmentId,
  ISSUE_WS_METHODS,
  type AuthEnvironmentScope,
  type AuthSessionState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { Atom, AtomRegistry, AsyncResult } from "effect/reactivity";

import { EnvironmentRegistry } from "../connection/registry.ts";
import { createCommandPermissions } from "./commandPermissions.ts";

vi.mock("./session.ts", () => ({
  createEnvironmentSessionAtoms: () => ({ sessionStateAtom: sessions }),
}));
const sessions = Atom.family((_id: EnvironmentId) =>
  Atom.make<AsyncResult.AsyncResult<AuthSessionState, string>>(AsyncResult.initial()),
);
const env = EnvironmentId.make("target");
const grant = (permissions: ReadonlyArray<AuthEnvironmentScope>): AuthSessionState => ({
  authenticated: true,
  auth: {
    policy: "remote-reachable",
    bootstrapMethods: [],
    sessionMethods: [],
    sessionCookieName: "test",
  },
  scopes: permissions.filter((scope) => scope !== AuthSourceControlWriteScope),
  permissions,
});
const runtime = Atom.runtime(
  Layer.succeed(EnvironmentRegistry, {
    run: (_id: EnvironmentId, effect: Effect.Effect<unknown>) => effect,
  } as unknown as EnvironmentRegistry["Service"]),
);

describe("Issue write permissions on the client", () => {
  it.effect.each([ISSUE_WS_METHODS.issuesComment, ISSUE_WS_METHODS.issuesSetState])(
    "%s needs the source-control write grant, which operate alone does not give",
    (method) =>
      Effect.scoped(
        Effect.gen(function* () {
          const permissions = createCommandPermissions(runtime, method);
          const registry = AtomRegistry.make();
          registry.mount(sessions(env));
          yield* Effect.addFinalizer(() => Effect.sync(() => registry.dispose()));

          registry.set(
            sessions(env),
            AsyncResult.success(grant([AuthOrchestrationReadScope, AuthOrchestrationOperateScope])),
          );
          expect(registry.get(permissions.permissionAtom(env))).toBe(false);
          expect(yield* permissions.authorize(registry, env).pipe(Effect.flip)).toMatchObject({
            _tag: "EnvironmentAuthorizationError",
            requiredPermission: AuthSourceControlWriteScope,
          });

          registry.set(
            sessions(env),
            AsyncResult.success(grant([AuthOrchestrationReadScope, AuthSourceControlWriteScope])),
          );
          expect(registry.get(permissions.permissionAtom(env))).toBe(true);
          yield* permissions.authorize(registry, env);
        }),
      ),
  );
});
