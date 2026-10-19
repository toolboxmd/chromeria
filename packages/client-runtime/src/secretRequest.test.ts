import { SecretRequestError, ThreadId, TurnItemId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  secretRequestAnswerInput,
  secretRequestDisplay,
  secretRequestFailureMessage,
  type SecretRequestItem,
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
