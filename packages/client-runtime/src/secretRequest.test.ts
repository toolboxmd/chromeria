import {
  SecretProviderId,
  SecretProviderKind,
  SecretRequestError,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  secretRequestAnswerInput,
  secretRequestDisplay,
  secretRequestFailureMessage,
  secretRequestLink,
  secretRequestSources,
} from "./secretRequest.ts";

const item = {
  id: TurnItemId.make("turn-item:secret-request:1"),
  threadId: ThreadId.make("thread-1"),
};

describe("secretRequestDisplay", () => {
  it("never offers the form for a request inherited from another thread", () => {
    expect(secretRequestDisplay({ secretStatus: "pending" }, "inherited")).toEqual({
      kind: "pending-elsewhere",
      label: "Waiting for an answer in the original thread",
    });
    expect(secretRequestDisplay({ secretStatus: "saved" }, "inherited")).toMatchObject({
      outcome: "saved",
    });
  });
});

describe("secretRequestAnswerInput", () => {
  const single = { ...item, label: "Webhook secret" };
  it("refuses a blank save and sends the value exactly as typed", () => {
    expect(
      secretRequestAnswerInput(single, { type: "save", values: new Map([["secret", "   "]]) }),
    ).toBeNull();
    expect(
      secretRequestAnswerInput(single, {
        type: "save",
        values: new Map([["secret", " whsec_1 "]]),
      }),
    ).toEqual({
      threadId: item.threadId,
      turnItemId: item.id,
      answer: { type: "save", secret: " whsec_1 " },
    });
    expect(secretRequestAnswerInput(single, { type: "decline" })).toEqual({
      threadId: item.threadId,
      turnItemId: item.id,
      answer: { type: "decline" },
    });
  });

  it("saves every field of a multi-field card together, or not at all", () => {
    const card = {
      ...single,
      fields: [
        { name: "client_id", label: "Client ID" },
        { name: "client_secret", label: "Client secret" },
      ],
    };
    expect(
      secretRequestAnswerInput(card, { type: "save", values: new Map([["client_id", "id-1"]]) }),
    ).toBeNull();
    expect(
      secretRequestAnswerInput(card, {
        type: "save",
        values: new Map([
          ["client_id", " id-1 "],
          ["client_secret", "cs-2"],
          ["stray", "x"],
        ]),
      }),
    ).toEqual({
      threadId: item.threadId,
      turnItemId: item.id,
      answer: { type: "save", secrets: { client_id: " id-1 ", client_secret: "cs-2" } },
    });
  });

  it("reads fields named like Object properties from their own values", () => {
    const card = {
      ...single,
      fields: [
        { name: "constructor", label: "Constructor" },
        { name: "tostring", label: "To string" },
      ],
    };
    expect(secretRequestAnswerInput(card, { type: "save", values: new Map() })).toBeNull();
    expect(
      secretRequestAnswerInput(card, {
        type: "save",
        values: new Map([
          ["constructor", "c-1"],
          ["tostring", "t-2"],
        ]),
      })?.answer,
    ).toEqual({ type: "save", secrets: { constructor: "c-1", tostring: "t-2" } });
  });
});

describe("secretRequestLink", () => {
  it.each([
    [
      "Open this link and sign in: https://a.dev/x?s=1. Then paste the code.",
      "https://a.dev/x?s=1",
    ],
    ["Open [https://a.dev/x?s=1](https://a.dev/x?s=1) and sign in.", "https://a.dev/x?s=1"],
    ["Sign in [here](https://a.dev/login?state=abc(def)).", "https://a.dev/login?state=abc(def)"],
    ["Sign in at https://a.dev/login?state=abc(def).", "https://a.dev/login?state=abc(def)"],
    ["(Sign in at https://a.dev/login.)", "https://a.dev/login"],
    ["Open **https://a.dev/x?s=1** and sign in.", "https://a.dev/x?s=1"],
    ["Open https://a.dev/x?state=u7o_ and sign in.", "https://a.dev/x?state=u7o_"],
    ["**Sign in at https://a.dev/device**", "https://a.dev/device"],
    ["_Open https://a.dev/device_.", "https://a.dev/device"],
    ["_https://a.dev/x?state=u7o_&client_id=cli_", "https://a.dev/x?state=u7o_&client_id=cli"],
    ["Use client_id acme at https://a.dev/x?state=u7o_", "https://a.dev/x?state=u7o_"],
    ["_Sign in_ at https://a.dev/x?state=u7o_", "https://a.dev/x?state=u7o_"],
    ["_Required_: Open https://a.dev/x?state=u7o_", "https://a.dev/x?state=u7o_"],
    ["(**https://a.dev/login**)", "https://a.dev/login"],
    ["Open ***https://a.dev/device***", "https://a.dev/device"],
    ["Open **_https://a.dev/device_**", "https://a.dev/device"],
    [
      "Open [https://a.dev/x?s=u7o_](https://a.dev/x?s=u7o_) and sign in.",
      "https://a.dev/x?s=u7o_",
    ],
    ["Open `https://a.dev/x?s=1.` and sign in.", "https://a.dev/x?s=1."],
    ["Open https://a.dev/x?s=1, then if needed https://a.dev/x?s=1 again.", "https://a.dev/x?s=1"],
    ["Open https://a.dev or https://b.dev.", undefined],
    ["Open https://a.dev (also written https://a.dev/).", "https://a.dev/"],
    ["Paste the code the CLI printed.", undefined],
  ])("finds the one link in a reason: %s", (reason, href) => {
    expect(secretRequestLink({ reason })?.href).toBe(href);
  });

  it("prefers the agent's url over a link in the reason", () => {
    expect(
      secretRequestLink({ url: "https://auth.acme.dev/a", reason: "See https://docs.acme.dev" })
        ?.host,
    ).toBe("auth.acme.dev");
  });
});

describe("secretRequestFailureMessage", () => {
  it("passes through server errors but hides anything that could echo the payload", () => {
    expect(
      secretRequestFailureMessage(new SecretRequestError({ reason: "already_answered" })),
    ).toBe("This secret request was already answered.");
    expect(secretRequestFailureMessage(new Error('Expected string, got "whsec_1"'))).toBe(
      "Could not answer the request. Try again.",
    );
    expect(secretRequestFailureMessage(undefined)).toBe("Could not answer the request. Try again.");
  });
});

describe("secretRequestSources", () => {
  const onePassword = SecretProviderId.make("1password");
  const providers = {
    [onePassword]: {
      kind: SecretProviderKind.make("one_password"),
      label: "1Password",
      config: { account: "my" },
    },
    [SecretProviderId.make("later")]: {
      kind: SecretProviderKind.make("from_a_newer_build"),
      label: "Later",
      config: {},
    },
  };

  it("offers configured providers only when the server accepts reference answers", () => {
    expect(secretRequestSources({ secretRequestReferences: true }, providers)).toMatchObject([
      { id: onePassword, label: "1Password" },
    ]);
    expect(secretRequestSources({}, providers)).toEqual([]);
    expect(secretRequestSources({ secretRequestReferences: true }, {})).toEqual([]);
  });

  it("answers with a reference only in the provider's syntax", () => {
    const [source] = secretRequestSources({ secretRequestReferences: true }, providers);
    if (source === undefined) throw new Error("expected a source");
    const item = { id: TurnItemId.make("item-1"), threadId: ThreadId.make("thread-1") };
    expect(
      secretRequestAnswerInput(item, {
        type: "reference",
        source,
        reference: " op://vault/item/field ",
      })?.answer,
    ).toEqual({
      type: "reference",
      reference: { provider: onePassword, reference: "op://vault/item/field" },
    });
    expect(
      secretRequestAnswerInput(item, { type: "reference", source, reference: "vault/item" }),
    ).toBeNull();
    for (const fields of [
      [{ name: "token", label: "Token" }],
      [
        { name: "id", label: "ID" },
        { name: "token", label: "Token" },
      ],
    ]) {
      expect(
        secretRequestAnswerInput(
          { ...item, fields },
          {
            type: "reference",
            source,
            reference: "op://vault/item/field",
          },
        ),
      ).toBeNull();
    }
  });
});
