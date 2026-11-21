import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { SecretRequestAnswerInput, secretRequestPageUrl } from "./secretRequest.ts";

describe("secretRequestPageUrl", () => {
  it("opens only http and https pages", () => {
    expect(secretRequestPageUrl("https://accounts.google.com/o/oauth2/auth?x=1")?.host).toBe(
      "accounts.google.com",
    );
    expect(secretRequestPageUrl("javascript:alert(1)")).toBeNull();
    expect(secretRequestPageUrl("https://?state=x")).toBeNull();
    expect(secretRequestPageUrl("not a url")).toBeNull();
  });
});

describe("SecretRequestAnswerInput", () => {
  const decode = Schema.decodeUnknownSync(SecretRequestAnswerInput);
  const encode = Schema.encodeSync(SecretRequestAnswerInput);
  const ids = { threadId: "thread-1", turnItemId: "item-1" };

  it("reads an upstream client's fields answer as secrets", () => {
    const input = decode({
      ...ids,
      answer: { type: "save", values: { client_id: "id", client_secret: " pass word " } },
    });
    expect(input.answer).toEqual({
      type: "save",
      secrets: { client_id: "id", client_secret: " pass word " },
    });
    expect(encode(input).answer).toEqual(input.answer);
  });

  it("rejects an upstream fields answer the fork contract would reject", () => {
    expect(() =>
      decode({ ...ids, answer: { type: "save", values: { client_id: " " } } }),
    ).toThrow();
    expect(() =>
      decode({ ...ids, answer: { type: "save", values: { "Bad Name": "x" } } }),
    ).toThrow();
  });
});
