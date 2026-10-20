import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { describe, expect, it } from "vite-plus/test";

import { AssetResource } from "./assets.ts";

const decodeAssetResource = Schema.decodeUnknownSync(AssetResource);
describe("qualified attachment assets", () => {
  const reference = {
    environmentId: "destination-b",
    attachmentId: "thread-b-550e8400-e29b-41d4-a716-446655440000",
    owner: "1".repeat(64),
    digest: "2".repeat(64),
    name: "screen.png",
    mimeType: "image/png",
    sizeBytes: 68,
  };
  const resource = {
    _tag: "peer-attachment",
    threadId: "thread-b",
    reference,
    disposition: "inline",
  };
  const isResource = Schema.is(AssetResource);

  it("retains the destination context and verified reference without a host path or URL", () => {
    expect(decodeAssetResource(resource)).toEqual(resource);
  });

  it("rejects incomplete or altered reference manifests at the public boundary", () => {
    for (const change of [
      { environmentId: "" },
      { attachmentId: "../private/file" },
      { owner: "not-an-owner" },
      { digest: "not-a-digest" },
      { sizeBytes: 0 },
      { sizeBytes: 50 * 1024 * 1024 + 1 },
    ]) {
      expect(isResource({ ...resource, reference: { ...reference, ...change } })).toBe(false);
    }
    expect(isResource({ ...resource, threadId: "" })).toBe(false);
    expect(isResource({ ...resource, disposition: "execute" })).toBe(false);
  });

  it("preserves existing local attachment wire shapes", () => {
    for (const local of [
      { _tag: "attachment", attachmentId: "existing-image" },
      {
        _tag: "attachment",
        attachmentId: "existing-file.pdf",
        fileName: "report.pdf",
        mimeType: "application/pdf",
        disposition: "inline",
      },
    ]) {
      expect(decodeAssetResource(local)).toEqual(local);
    }
  });
});

describe("native browser download assets", () => {
  const download = {
    _tag: "native-browser-download",
    resource: {
      type: "preview",
      environmentId: "b",
      threadId: "thread-b",
      tabId: "tab-b",
      serverEpoch: "epoch-b",
    },
    downloadId: "550e8400-e29b-41d4-a716-446655440000",
    name: "report.txt",
    mimeType: "application/octet-stream",
    sizeBytes: 1024 * 1024 * 1024,
    digest: "a".repeat(64),
  };
  it("retains the native 1 GiB boundary without raising the attachment publication limit", () => {
    expect(decodeAssetResource(download)).toEqual(download);
    expect(Schema.is(AssetResource)({ ...download, sizeBytes: download.sizeBytes + 1 })).toBe(
      false,
    );
    expect(Schema.is(AssetResource)({ ...download, sizeBytes: 0 })).toBe(true);
    expect(
      Schema.is(AssetResource)({
        _tag: "peer-attachment",
        threadId: "thread-b",
        reference: {
          environmentId: "b",
          attachmentId: "thread-b-550e8400-e29b-41d4-a716-446655440000",
          owner: "b".repeat(64),
          name: download.name,
          mimeType: download.mimeType,
          digest: download.digest,
          sizeBytes: 50 * 1024 * 1024 + 1,
        },
      }),
    ).toBe(false);
  });
  it("refuses paths, URLs, unsafe names and malformed native resource identity", () => {
    for (const changed of [
      { path: "/private/file" },
      { url: "http://private/download" },
      { name: "../report" },
      { name: "report\r\nheader" },
      { digest: "wrong" },
      { downloadId: "../file" },
      { resource: { ...download.resource, serverEpoch: "" } },
    ]) {
      expect(
        Option.isNone(Schema.decodeUnknownOption(AssetResource)({ ...download, ...changed })),
        JSON.stringify(changed),
      ).toBe(true);
    }
  });
});
