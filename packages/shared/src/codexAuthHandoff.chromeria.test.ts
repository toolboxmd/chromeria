// Chromeria (toolboxmd fork): the V2 desktop's own chromeria-v2 scheme through the
// shared ChatGPT handoff and return helpers.
import { describe, expect, it } from "vite-plus/test";
import { EnvironmentId, ProviderInstanceId } from "@t3tools/contracts";
import {
  codexAuthDeliveryUrl,
  codexAuthHandoffUrl,
  readCodexAuthDelivery,
  readCodexAuthHandoff,
} from "./codexAuthHandoff.ts";
import { providerAuthReturnUrl } from "./providerAuthReturnUrl.ts";

const authorizationUrl = new URL("https://auth.openai.com/api/accounts/authorize");
authorizationUrl.search = new URLSearchParams({
  client_id: "dynamic_agent_client",
  response_type: "code",
  redirect_uri: "http://127.0.0.1:54213/auth/callback",
  state: "a".repeat(43),
  code_challenge_method: "S256",
  code_challenge: "b".repeat(43),
}).toString();
const input = {
  authorizationUrl: authorizationUrl.toString(),
  returnUrl: "chromeria-v2://app/settings/providers?instanceId=work-codex",
  environmentId: EnvironmentId.make("remote-environment"),
  instanceId: ProviderInstanceId.make("work-codex"),
  flowId: "flow-one",
};
const callbackUrl = `http://127.0.0.1:54213/auth/callback?state=${"a".repeat(43)}&code=one-time-code`;

describe("Chromeria V2 ChatGPT handoff", () => {
  it("sanitizes V2 desktop return destinations like the default app's", () => {
    expect(providerAuthReturnUrl("chromeria-v2://app/welcome?code=secret#agents:machine-id")).toBe(
      "chromeria-v2://app/welcome#agents:machine-id",
    );
    expect(
      providerAuthReturnUrl("chromeria-v2://app/settings/providers?instanceId=work&code=secret"),
    ).toBe("chromeria-v2://app/settings/providers?instanceId=work");
    for (const url of [
      "chromeria-v2://attacker/welcome",
      "chromeria-v2://app:123/welcome",
      "chromeria-v2://app/auth/callback",
      "chromeria-v2://app/welcome/../evil",
      "chromeria-v3://app/welcome",
    ]) {
      expect(providerAuthReturnUrl(url)).toBeUndefined();
    }
  });

  it("round-trips a V2 handoff and delivers the code to the V2 return destination", () => {
    const link = codexAuthHandoffUrl(input, false, "chromeria-v2");
    expect(link.startsWith("chromeria-v2://auth/codex?request=")).toBe(true);
    expect(readCodexAuthHandoff(link, false, "chromeria-v2")).toEqual(input);

    const delivery = codexAuthDeliveryUrl(input, callbackUrl);
    expect(delivery.startsWith(`${input.returnUrl}#codex-auth=`)).toBe(true);
    expect(readCodexAuthDelivery(delivery)).toEqual({
      callbackUrl,
      environmentId: input.environmentId,
      instanceId: input.instanceId,
      flowId: input.flowId,
      returnHash: "",
      returnUrl: input.returnUrl,
    });
  });

  it("keeps each app to handoffs addressed to its own scheme", () => {
    const v2 = codexAuthHandoffUrl(input, false, "chromeria-v2");
    expect(readCodexAuthHandoff(v2, false)).toBeUndefined();
    expect(readCodexAuthHandoff(v2, true)).toBeUndefined();
    expect(readCodexAuthHandoff(codexAuthHandoffUrl(input), false, "chromeria-v2")).toBeUndefined();
    expect(
      readCodexAuthHandoff(codexAuthHandoffUrl(input, true), true, "chromeria-v2"),
    ).toBeUndefined();
    expect(
      readCodexAuthHandoff(
        codexAuthHandoffUrl(
          { ...input, returnUrl: "chromeria-v2://attacker/settings/providers" },
          false,
          "chromeria-v2",
        ),
        false,
        "chromeria-v2",
      ),
    ).toBeUndefined();
  });
});
