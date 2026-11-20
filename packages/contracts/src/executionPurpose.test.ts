import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import {
  PurposeIntent,
  parseTrackingReferenceUrl,
  purposeScopesEqual,
} from "./executionPurpose.ts";
import { OrchestrationV2Command } from "./orchestrationV2.ts";

const decodeCommand = Schema.decodeUnknownExit(OrchestrationV2Command);
const isPurposeIntent = Schema.is(PurposeIntent);

const reference = {
  host: "github.com",
  repository: "example/repo",
  kind: "issue",
  number: 11,
} as const;

describe("purpose request boundary", () => {
  it("keeps Issue and PR identities distinct, validates URLs and normalizes case", () => {
    expect(parseTrackingReferenceUrl("https://GitHub.com/Example/Repo/issues/11")).toEqual(
      reference,
    );
    expect(parseTrackingReferenceUrl("https://github.com/example/repo/pull/11")).toEqual({
      ...reference,
      kind: "pr",
    });
    for (const url of [
      "https://github.com/example/repo/issues/0",
      "https://github.com/example/repo/issues/9007199254740992",
      "https://github.com/example%20name/repo/issues/11",
      "text example/repo#11",
    ]) {
      expect(parseTrackingReferenceUrl(url)).toBeNull();
    }
    expect(
      purposeScopesEqual(
        { type: "single", reference },
        { type: "single", reference: { ...reference, kind: "pr" } },
      ),
    ).toBe(false);
  });

  it("rejects malformed, contradictory and unsupported envelopes at the public command schema", () => {
    const command = {
      type: "message.dispatch",
      commandId: "purpose-boundary",
      threadId: "purpose-thread",
      messageId: "purpose-message",
      text: "fixture",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
      dispatchMode: { type: "start_immediately" },
    };
    expect(decodeCommand(command)._tag === "Success").toBe(true);
    expect(
      decodeCommand({
        ...command,
        purposeIntent: { version: 1, intent: { type: "reference", reference } },
      })._tag === "Success",
    ).toBe(true);
    for (const purposeIntent of [
      { version: 1 },
      { intent: { type: "unscoped" } },
      { version: 2, intent: { type: "unscoped" } },
      { version: 1, intent: { type: "target", reference } },
      { version: 1, intent: { type: "reference", reference: { ...reference, number: -1 } } },
    ]) {
      expect(decodeCommand({ ...command, purposeIntent })._tag === "Success").toBe(false);
    }
    expect(
      isPurposeIntent({ version: 1, intent: { type: "selection", expectedRevision: -1 } }),
    ).toBe(false);
  });

  it("requires a valid versioned declaration to add an Issue-to-PR delivery", () => {
    const link = {
      type: "thread.pull-request.link",
      commandId: "delivery-boundary",
      threadId: "purpose-thread",
      host: "github.com",
      repository: "delivery/repo",
      number: 42,
      url: "https://github.com/delivery/repo/pull/42",
      source: "agent",
    };
    expect(decodeCommand(link)._tag).toBe("Success");
    expect(
      decodeCommand({ ...link, purposeDelivery: { version: 1, purpose: reference } })._tag,
    ).toBe("Success");
    for (const purposeDelivery of [
      { version: 1 },
      { version: 2, purpose: reference },
      { version: 1, purpose: { ...reference, kind: "task" } },
    ]) {
      expect(decodeCommand({ ...link, purposeDelivery })._tag).toBe("Failure");
    }
  });
});
