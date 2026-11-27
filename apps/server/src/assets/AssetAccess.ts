import * as Digest from "effect/crypto/Digest";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import { loadCaller, assertLiveCaller } from "../mcp/threadAccess.ts";
import { CurrentRequest } from "../mcp/McpRequestReceipts.ts";
import type { AssetResource } from "@t3tools/contracts";
import {
  AssetAttachmentNotFoundError,
  AssetSourceControlMediaUrlValidationError,
  AssetPreviewTypeValidationError,
  AssetProjectFaviconInspectionError,
  AssetProjectFaviconNotFoundError,
  AssetProjectFaviconResolutionError,
  AssetSigningKeyLoadError,
  AssetWorkspaceAssetInspectionError,
  AssetWorkspaceAssetNotFoundError,
  AssetWorkspaceContextNotFoundError,
  AssetWorkspacePathValidationError,
  AssetWorkspaceResolutionError,
  AssetWorkspaceRootNormalizationError,
  SOURCE_CONTROL_MEDIA_ASSET,
  ThreadId,
  PeerAttachmentAssetResource,
  NativeBrowserDownloadAssetResource,
  type QualifiedAssetResource,
  AuthSessionId,
  OrchestratorMcpFailure,
  NativeAppIconReference,
  TurnItemId,
  EnvironmentThemeId,
} from "@t3tools/contracts";
import {
  hostPreviewMimeTypeFromExtension,
  isWorkspaceAudioPreviewPath,
  isWorkspacePreviewEntryPath,
  isWorkspaceVideoPreviewPath,
  WORKSPACE_BROWSER_PREVIEW_EXTENSIONS,
} from "@t3tools/shared/filePreview";
import {
  IMAGE_DIMENSIONS_HEADER_BYTES,
  readImageDimensions,
  type ImageDimensions,
} from "@t3tools/shared/imageDimensions";
import { MCP_APP_MAX_HTML_BYTES } from "@t3tools/shared/mcpApp";
import {
  isProjectFaviconPath,
  PROJECT_FAVICON_FALLBACK_MARKER,
} from "@t3tools/shared/projectFavicon";
import { isNetworkPath } from "@t3tools/shared/path";
import { MAX_TOOL_OUTPUT_IMAGE_BASE64_LENGTH, toolOutputImages } from "@t3tools/shared/toolOutput";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { base64UrlDecodeUtf8, base64UrlEncode } from "../auth/utils.ts";
import * as AssetSigner from "./AssetSigner.ts";
import { makeIsServerInternalPath } from "../auth/serverInternalPaths.ts";
import { parseAttachmentFileExtension, resolveAttachmentPathById } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import { findThemeBackground } from "../environmentTheme.ts";
import * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import * as PeerAuthority from "../peer/PeerAuthority.ts";
import * as PeerMcpClient from "../peer/PeerMcpClient.ts";
import * as PeerLinks from "../peer/PeerLinks.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ServerBrowser from "../preview/ServerBrowser.ts";
import * as AttachmentStaging from "./AttachmentStaging.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as ProjectFaviconResolver from "../project/ProjectFaviconResolver.ts";
import { isWithinRoot } from "../workspace/KnownWorkspaces.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { toHostDrivePath } from "../workspace/hostDrivePath.ts";
import * as NativeAppIconResolver from "./NativeAppIconResolver.ts";
import { sourceControlDefinitions } from "../sourceControl/sourceControlDefinitions.ts";
import { extractIcnsPng } from "./icns.ts";
import {
  mediaFileRevision,
  openMediaFile,
  readMediaFileHeader,
  realPathWithoutNativeResolver,
  type OpenMediaFile,
} from "./MediaFile.ts";
import * as WorkspacePreviewRevisions from "./WorkspacePreviewRevisions.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";

export const ASSET_ROUTE_PREFIX = "/api/assets";

const ASSET_TOKEN_TTL_MS = 60 * 60 * 1000;
const PROJECT_FAVICON_TOKEN_BUCKET_MS = 30 * 60 * 1000;
const HTML_ATTACHMENT_TOKEN_BUCKET_MS = 30 * 60 * 1000;
const PROJECT_FAVICON_VERSION_PREFIX = "v";
const PROJECT_FAVICON_ICNS_MIN_SIZE = 96;
const INLINE_VIDEO_MIME_TYPE_PATTERN = /^video\/[\w!#$&^.+-]+$/i;
// Use server-assigned attachment extensions, never client MIME types.
const INLINE_PREVIEW_MIME_TYPES: Record<string, string> = {
  svg: "image/svg+xml",
  pdf: "application/pdf",
  html: "text/html",
  htm: "text/html",
};
const inlinePreviewMimeTypeForExtension = (extension: string) =>
  INLINE_PREVIEW_MIME_TYPES[extension] ??
  hostPreviewMimeTypeFromExtension(`.${extension}`) ??
  undefined;
const PREVIEW_ASSET_EXTENSIONS = new Set([
  ...WORKSPACE_BROWSER_PREVIEW_EXTENSIONS,
  // Keep HTML dependency grants independent of newly supported file viewers.
  ".avif",
  ".gif",
  ".ico",
  ".jpeg",
  ".jpg",
  ".png",
  ".svg",
  ".webp",
  ".m4v",
  ".mp4",
  ".ogv",
  ".webm",
  ".aac",
  ".flac",
  ".m4a",
  ".mp3",
  ".oga",
  ".ogg",
  ".opus",
  ".wav",
  ".css",
  ".js",
  ".mjs",
  ".otf",
  ".ttf",
  ".woff",
  ".woff2",
]);

const isPreviewDependencyPath = (filePath: string, path: Path.Path) =>
  filePath.length > 0 &&
  !filePath.includes("\0") &&
  !filePath.split(/[\\/]/).some((segment) => segment.startsWith(".")) &&
  PREVIEW_ASSET_EXTENSIONS.has(path.extname(filePath).toLowerCase());

const AssetClaimsSchema = Schema.Union([
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("native-browser-download"),
    resource: NativeBrowserDownloadAssetResource,
    sessionId: AuthSessionId,
    generation: Schema.String,
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("peer-attachment"),
    resource: PeerAttachmentAssetResource,
    sessionId: AuthSessionId,
    generation: Schema.String,
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("workspace-file"),
    workspaceRoot: Schema.String,
    baseRelativePath: Schema.String,
    /** The opened page, which the files it requests are tracked under; absent on older URLs. */
    entryFileName: Schema.optionalKey(Schema.String),
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("workspace-file-exact"),
    workspaceRoot: Schema.String,
    relativePath: Schema.String,
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("media-file-exact"),
    filePath: Schema.String,
    download: Schema.optionalKey(Schema.Boolean),
    device: Schema.String,
    inode: Schema.String,
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("attachment"),
    attachmentId: Schema.String,
    download: Schema.optionalKey(Schema.Boolean),
    fileName: Schema.optionalKey(Schema.String),
    mimeType: Schema.optionalKey(Schema.String),
    renderIntent: Schema.optionalKey(Schema.Literal("mcp-app")),
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("project-favicon"),
    workspaceRoot: Schema.String,
    relativePath: Schema.NullOr(Schema.String),
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("project-favicon-external"),
    filePath: Schema.String,
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("native-app-icon"),
    app: NativeAppIconReference,
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("environment-theme-background"),
    themeId: EnvironmentThemeId,
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal("tool-output-image"),
    threadId: ThreadId,
    itemId: TurnItemId,
    index: Schema.Number,
    expiresAt: Schema.Number,
  }),
  Schema.Struct({
    version: Schema.Literal(1),
    kind: Schema.Literal(SOURCE_CONTROL_MEDIA_ASSET),
    /** A host's `fetchUrl`, narrowed at mint time; the signature is what keeps it there. */
    url: Schema.String,
    cwd: Schema.String,
    repository: Schema.optionalKey(Schema.String),
    expiresAt: Schema.Number,
  }),
]);
type AssetClaims = typeof AssetClaimsSchema.Type;

const AssetClaimsJson = Schema.fromJsonString(AssetClaimsSchema);
const decodeAssetClaims = Schema.decodeUnknownOption(AssetClaimsJson);
const encodeAssetClaims = Schema.encodeSync(AssetClaimsJson);

export type ResolvedAsset =
  | {
      readonly kind: "native-browser-download";
      readonly resource: NativeBrowserDownloadAssetResource;
      readonly sessionId: AuthSessionId;
      readonly generation: string;
    }
  | {
      readonly kind: "peer-attachment";
      readonly resource: PeerAttachmentAssetResource;
      readonly sessionId: AuthSessionId;
      readonly generation: string;
    }
  | {
      readonly kind: "file";
      readonly path: string;
      readonly download?: boolean;
      readonly fileName?: string;
      readonly mimeType?: string;
      readonly file?: OpenMediaFile;
      readonly renderIntent?: "mcp-app" | "file-preview";
    }
  | {
      readonly kind: "bytes";
      readonly bytes: Uint8Array;
      readonly mimeType: string;
    }
  | {
      readonly kind: "source-control-media";
      readonly url: string;
      readonly cwd: string;
      readonly repository?: string;
      readonly expiresAt: number;
    };

function decodeClaims(encodedPayload: string): AssetClaims | null {
  try {
    return Option.getOrNull(decodeAssetClaims(base64UrlDecodeUtf8(encodedPayload)));
  } catch {
    return null;
  }
}

function decodeRelativePath(value: string): string | null {
  try {
    return decodeURIComponent(value);
  } catch {
    return null;
  }
}

export const readMcpAppHtml = Effect.fn("AssetAccess.readMcpAppHtml")(function* (filePath: string) {
  const fileSystem = yield* FileSystem.FileSystem;
  // Allow space for stored CSP tags, but bound reads if the attachment grows after minting.
  const limit = MCP_APP_MAX_HTML_BYTES + 128 * 1024;
  const chunks = yield* Stream.runCollect(fileSystem.stream(filePath, { bytesToRead: limit + 1 }));
  const size = chunks.reduce((total, chunk) => total + chunk.byteLength, 0);
  if (size > limit) return null;
  const bytes = new Uint8Array(size);
  let position = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, position);
    position += chunk.byteLength;
  }
  return bytes;
});

const readToolOutputImage = Effect.fn("AssetAccess.readToolOutputImage")(function* (input: {
  readonly threadId: ThreadId;
  readonly itemId: TurnItemId;
  readonly index: number;
}) {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const item = yield* orchestrator.getTurnItem(input);
  const image =
    item?.type === "dynamic_tool" ? toolOutputImages(item.output)[input.index] : undefined;
  return image?.data === undefined || image.data.length > MAX_TOOL_OUTPUT_IMAGE_BASE64_LENGTH
    ? null
    : { mimeType: image.mimeType, bytes: Buffer.from(image.data, "base64") };
});

// Browsers cannot draw .icns; serve its embedded PNG.
const projectFaviconAsset = Effect.fn("AssetAccess.projectFaviconAsset")(function* (
  filePath: string,
) {
  // Checked again here: a link can be repointed after its URL was issued.
  if (!isProjectFaviconPath(filePath)) return null;
  if (!filePath.toLowerCase().endsWith(".icns")) {
    return { kind: "file", path: filePath } satisfies ResolvedAsset;
  }
  const fileSystem = yield* FileSystem.FileSystem;
  const bytes = yield* fileSystem.readFile(filePath).pipe(
    Effect.tapError((cause) =>
      Effect.logError("Failed to read project icon image.", { filePath, cause }),
    ),
    Effect.orElseSucceed(() => null),
  );
  const png = bytes ? extractIcnsPng(bytes, PROJECT_FAVICON_ICNS_MIN_SIZE) : null;
  return png
    ? ({ kind: "bytes", bytes: png, mimeType: "image/png" } satisfies ResolvedAsset)
    : null;
});

const resolveCanonicalFile = Effect.fn("AssetAccess.resolveCanonicalFile")(function* (
  filePath: string,
) {
  const fileSystem = yield* FileSystem.FileSystem;
  const nativeCanonicalFile = yield* ProjectFaviconResolver.optionOnNotFound(
    fileSystem.realPath(filePath),
  );
  // Windows' native realpath returns ENOENT on drives without a mount-manager name
  // (some USB and RAM-disk drivers) although the file is readable.
  const canonicalFile = Option.isSome(nativeCanonicalFile)
    ? nativeCanonicalFile.value
    : yield* realPathWithoutNativeResolver(filePath);
  if (canonicalFile === null) return null;

  const info = yield* ProjectFaviconResolver.optionOnNotFound(fileSystem.stat(canonicalFile));
  if (Option.isNone(info) || info.value.type !== "File") return null;
  const isServerInternalPath = yield* makeIsServerInternalPath;
  return (yield* isServerInternalPath(canonicalFile)) ? null : canonicalFile;
});

const resolveCanonicalWorkspaceFile = Effect.fn("AssetAccess.resolveCanonicalWorkspaceFile")(
  function* (input: {
    readonly workspaceRoot: string;
    readonly relativePath: string;
    /** The HTML entry's folder, relative to the root, when serving one of its dependencies. */
    readonly previewBaseRelativePath?: string;
    /** The request is for the opened page itself, which may be a link to elsewhere in the root. */
    readonly previewEntry?: boolean;
  }) {
    const fileSystem = yield* FileSystem.FileSystem;
    const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
    const resolved = yield* workspacePaths.resolveRelativePathWithinRoot(input).pipe(
      Effect.asSome,
      Effect.catchTags({
        WorkspacePathOutsideRootError: () => Effect.succeedNone,
      }),
    );
    if (Option.isNone(resolved)) return null;

    const [canonicalRoot, canonicalFile] = yield* Effect.all([
      ProjectFaviconResolver.optionOnNotFound(fileSystem.realPath(input.workspaceRoot)),
      ProjectFaviconResolver.optionOnNotFound(fileSystem.realPath(resolved.value.absolutePath)),
    ]);
    if (Option.isNone(canonicalRoot) || Option.isNone(canonicalFile)) return null;

    const path = yield* Path.Path;
    const relative = path.relative(canonicalRoot.value, canonicalFile.value);
    if (relative === "" || relative.startsWith("..") || path.isAbsolute(relative)) return null;

    if (input.previewBaseRelativePath !== undefined) {
      // The opened entry's own folder may be hidden; nothing below it may.
      const canonicalBase = yield* ProjectFaviconResolver.optionOnNotFound(
        fileSystem.realPath(path.join(canonicalRoot.value, input.previewBaseRelativePath)),
      );
      const belowBase = Option.isSome(canonicalBase)
        ? path.relative(canonicalBase.value, canonicalFile.value)
        : "..";
      const outsideBase = belowBase.startsWith("..") || path.isAbsolute(belowBase);
      // Only the opened page may resolve outside its folder; the files it loads may not.
      if (outsideBase && input.previewEntry !== true) return null;
      if (!isPreviewDependencyPath(outsideBase ? relative : belowBase, path)) return null;
    }

    const info = yield* ProjectFaviconResolver.optionOnNotFound(
      fileSystem.stat(canonicalFile.value),
    );
    if (Option.isNone(info) || info.value.type !== "File") return null;
    const isServerInternalPath = yield* makeIsServerInternalPath;
    return (yield* isServerInternalPath(canonicalFile.value)) ? null : canonicalFile.value;
  },
);

const resolveCanonicalWorkspaceFileForRequest = (input: {
  readonly workspaceRoot: string;
  readonly relativePath: string;
  readonly previewBaseRelativePath?: string;
  readonly previewEntry?: boolean;
}) =>
  resolveCanonicalWorkspaceFile(input).pipe(
    Effect.tapError((cause) =>
      Effect.logError("Failed to resolve canonical asset path.", {
        workspaceRoot: input.workspaceRoot,
        relativePath: input.relativePath,
        cause,
      }),
    ),
    Effect.orElseSucceed(() => null),
  );

const HEADER_IMAGE_EXTENSIONS = new Set([".png", ".jpg", ".jpeg", ".gif", ".webp"]);

const readImageDimensionsFromOpenFile = (filePath: string, file: OpenMediaFile) =>
  readMediaFileHeader(filePath, file, IMAGE_DIMENSIONS_HEADER_BYTES).pipe(
    Effect.map(readImageDimensions),
    Effect.orElseSucceed((): ImageDimensions | null => null),
  );

// Non-blocking open prevents swapped-in FIFOs from hanging the request.
const readImageDimensionsFromHeader = (filePath: string) =>
  openMediaFile(filePath).pipe(
    Effect.flatMap((file) =>
      file === null ? Effect.succeed(null) : readImageDimensionsFromOpenFile(filePath, file),
    ),
    Effect.scoped,
    Effect.orElseSucceed((): ImageDimensions | null => null),
  );

const finalizeAbsoluteMediaFileAsset = Effect.fn("AssetAccess.finalizeAbsoluteMediaFileAsset")(
  function* (input: {
    readonly requestedPath: string;
    readonly expectedCanonicalPath?: string;
    readonly workspaceRoot: string | undefined;
    readonly resource: AssetResource;
    readonly expiresAt: number;
  }) {
    const path = yield* Path.Path;
    // Resolving a network path makes Windows sign in to its host with the
    // user's NTLM credentials, so only the thread's own project may be on a share.
    // Windows reads a `\\?\` path as written, `..` included, so the lookup gets
    // the resolved path this check approved.
    const requestedPath = path.resolve(input.requestedPath);
    if (
      isNetworkPath(input.requestedPath) &&
      (input.workspaceRoot === undefined ||
        !isWithinRoot(path, requestedPath, path.resolve(input.workspaceRoot)))
    ) {
      return yield* new AssetWorkspaceAssetNotFoundError({
        resource: input.resource,
        checkedPath: input.requestedPath,
      });
    }
    const canonicalFile = yield* resolveCanonicalFile(requestedPath).pipe(
      Effect.mapError(
        (cause) => new AssetWorkspaceAssetInspectionError({ resource: input.resource, cause }),
      ),
    );
    // The workspace caller pins its containment-checked path; never follow a
    // replacement symlink to a different file during this second resolution.
    if (
      !canonicalFile ||
      (input.expectedCanonicalPath !== undefined && canonicalFile !== input.expectedCanonicalPath)
    ) {
      return yield* new AssetWorkspaceAssetNotFoundError({
        resource: input.resource,
        checkedPath: input.requestedPath,
      });
    }
    const download =
      (input.resource._tag === "media-file" || input.resource._tag === "draft-workspace-file") &&
      input.resource.disposition === "attachment";
    if (!download && hostPreviewMimeTypeFromExtension(path.extname(canonicalFile)) === null) {
      return yield* new AssetPreviewTypeValidationError({ resource: input.resource });
    }
    const wantsDimensions = HEADER_IMAGE_EXTENSIONS.has(path.extname(canonicalFile).toLowerCase());
    const opened = yield* openMediaFile(canonicalFile).pipe(
      Effect.flatMap((file) =>
        file === null
          ? Effect.succeed(null)
          : Effect.map(
              wantsDimensions
                ? readImageDimensionsFromOpenFile(canonicalFile, file)
                : Effect.succeed(null),
              (dimensions) => ({
                identity: { device: file.info.dev.toString(), inode: file.info.ino.toString() },
                fileRevision: mediaFileRevision(file.info),
                dimensions,
              }),
            ),
      ),
      Effect.scoped,
      Effect.mapError(
        (cause) => new AssetWorkspaceAssetInspectionError({ resource: input.resource, cause }),
      ),
    );
    if (!opened) {
      return yield* new AssetWorkspaceAssetNotFoundError({
        resource: input.resource,
        checkedPath: input.requestedPath,
      });
    }
    return {
      claims: {
        version: 1 as const,
        kind: "media-file-exact" as const,
        filePath: canonicalFile,
        ...(download ? { download: true } : {}),
        ...opened.identity,
        expiresAt: input.expiresAt,
      },
      fileName: path.basename(canonicalFile),
      fileRevision: opened.fileRevision,
      imageDimensions: opened.dimensions,
    };
  },
);

const finalizeWorkspaceFileAsset = Effect.fn("AssetAccess.finalizeWorkspaceFileAsset")(
  function* (input: {
    readonly workspaceRoot: string;
    readonly requestedPath: string;
    readonly resource: AssetResource;
    readonly expiresAt: number;
  }) {
    const path = yield* Path.Path;
    const fileSystem = yield* FileSystem.FileSystem;
    const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
    const relativePath = path.isAbsolute(input.requestedPath)
      ? path.relative(input.workspaceRoot, input.requestedPath)
      : input.requestedPath;
    const resolved = yield* workspacePaths
      .resolveRelativePathWithinRoot({ workspaceRoot: input.workspaceRoot, relativePath })
      .pipe(
        Effect.mapError(
          (cause) =>
            new AssetWorkspacePathValidationError({
              resource: input.resource,
              cause,
            }),
        ),
      );
    const isDraftMedia =
      input.resource._tag === "draft-workspace-file" &&
      (input.resource.disposition === "attachment" ||
        isWorkspaceAudioPreviewPath(resolved.relativePath) ||
        isWorkspaceVideoPreviewPath(resolved.relativePath));
    if (!isDraftMedia && !isWorkspacePreviewEntryPath(resolved.relativePath)) {
      return yield* new AssetPreviewTypeValidationError({
        resource: input.resource,
      });
    }
    const canonicalFile = yield* resolveCanonicalWorkspaceFile({
      workspaceRoot: input.workspaceRoot,
      relativePath: resolved.relativePath,
    }).pipe(
      Effect.mapError(
        (cause) =>
          new AssetWorkspaceAssetInspectionError({
            resource: input.resource,
            cause,
          }),
      ),
    );
    if (!canonicalFile) {
      // A link out of the root gets the single-file grant an absolute path would.
      const linkedFile = yield* resolveCanonicalFile(resolved.absolutePath).pipe(
        Effect.mapError(
          (cause) => new AssetWorkspaceAssetInspectionError({ resource: input.resource, cause }),
        ),
      );
      if (linkedFile) {
        return yield* finalizeAbsoluteMediaFileAsset({
          ...input,
          requestedPath: linkedFile,
          expectedCanonicalPath: linkedFile,
        });
      }
      return yield* new AssetWorkspaceAssetNotFoundError({
        resource: input.resource,
      });
    }
    if (isDraftMedia) {
      return yield* finalizeAbsoluteMediaFileAsset({
        ...input,
        requestedPath: canonicalFile,
        expectedCanonicalPath: canonicalFile,
      });
    }
    // The response type follows the file a link points at, so that file must be previewable too.
    if (!isWorkspacePreviewEntryPath(canonicalFile)) {
      return yield* new AssetPreviewTypeValidationError({
        resource: input.resource,
      });
    }
    const canonicalWorkspaceRoot = yield* fileSystem.realPath(input.workspaceRoot).pipe(
      Effect.mapError(
        (cause) =>
          new AssetWorkspaceResolutionError({
            resource: input.resource,
            cause,
          }),
      ),
    );
    const imageDimensions = HEADER_IMAGE_EXTENSIONS.has(
      path.extname(resolved.relativePath).toLowerCase(),
    )
      ? yield* readImageDimensionsFromHeader(canonicalFile)
      : null;
    if (!/\.html?$/i.test(resolved.relativePath)) {
      return {
        claims: {
          version: 1 as const,
          kind: "workspace-file-exact" as const,
          workspaceRoot: canonicalWorkspaceRoot,
          relativePath: resolved.relativePath,
          expiresAt: input.expiresAt,
        },
        fileName: path.basename(resolved.relativePath),
        imageDimensions,
        fileRevision: undefined,
      };
    }
    const revisions = yield* WorkspacePreviewRevisions.WorkspacePreviewRevisions;
    return {
      claims: {
        version: 1 as const,
        kind: "workspace-file" as const,
        workspaceRoot: canonicalWorkspaceRoot,
        baseRelativePath: path.dirname(resolved.relativePath),
        entryFileName: path.basename(resolved.relativePath),
        expiresAt: input.expiresAt,
      },
      fileName: path.basename(resolved.relativePath),
      imageDimensions,
      fileRevision: yield* revisions.revision({
        workspaceRoot: canonicalWorkspaceRoot,
        entryRelativePath: resolved.relativePath,
      }),
    };
  },
);

/** Acquisition is an MCP mutation with its original actor, never a native viewer ticket. */
export const acquireAttachment = Effect.fn("AssetAccess.acquireAttachment")(function* (
  resource: QualifiedAssetResource,
  key: string,
) {
  if (
    resource._tag === "native-browser-download" &&
    (resource.sizeBytes < 1 || resource.sizeBytes > 50 * 1024 * 1024)
  )
    return yield* new OrchestratorMcpFailure({
      code: "invalid_request",
      message:
        "Source attachment acquisition supports 1 byte through 50 MiB. Fetch this native download through the authenticated source client instead.",
    });
  const original = yield* Invocation.McpInvocationContext;
  const scope = yield* Invocation.requireThreadScope(original, "Attachment acquisition");
  const request = yield* CurrentRequest;
  const refused = () =>
    new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "The attachment is unavailable to its original acquisition caller.",
    });
  if (request === undefined) return yield* refused();
  const threads = yield* ThreadManagement.ThreadManagementService;
  const staging = yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging);
  const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const peers = yield* Effect.serviceOption(PeerMcpClient.PeerMcpClient);
  const links = yield* Effect.serviceOption(PeerLinks.PeerLinks);
  if (Option.isNone(staging) || Option.isNone(authority)) return yield* refused();
  const live = loadCaller().pipe(
    Effect.tap(assertLiveCaller),
    Effect.provideService(Invocation.McpInvocationContext, scope),
    Effect.provideService(ThreadManagement.ThreadManagementService, threads),
  );
  const caller = yield* live;
  const current = live.pipe(Effect.asVoid);
  let admitted: {
    bytes: Uint8Array;
    producer: "device" | "preview";
    authorize: Effect.Effect<void, OrchestratorMcpFailure>;
  };
  if (resource._tag === "native-browser-download") {
    let access: {
      authorize: Effect.Effect<void, OrchestratorMcpFailure>;
      readBlock: (offset: number) => Effect.Effect<Uint8Array, OrchestratorMcpFailure>;
    };
    if (resource.resource.environmentId === scope.environmentId) {
      const browser = yield* Effect.serviceOption(ServerBrowser.ServerBrowser);
      if (
        Option.isNone(browser) ||
        browser.value.inspectDownload === undefined ||
        browser.value.readDownloadBlock === undefined
      )
        return yield* refused();
      const authorize = current.pipe(
        Effect.andThen(
          authority.value.recheckInvocation(scope, "preview", resource.resource.threadId),
        ),
        Effect.asVoid,
      );
      const read = browser.value.readDownloadBlock;
      access = {
        authorize: browser.value.inspectDownload(resource, authorize),
        readBlock: (offset) => read(resource, offset, authorize),
      };
    } else {
      if (
        Option.isNone(peers) ||
        peers.value.readNativeBrowserDownload === undefined ||
        Option.isNone(links)
      )
        return yield* refused();
      const link = yield* links.value
        .get(resource.resource.environmentId)
        .pipe(Effect.mapError(refused));
      if (Option.isNone(link)) return yield* refused();
      const envelope = yield* authority.value.prepare(
        scope,
        resource.resource.environmentId,
        link.value.launchIdentity,
        "attachment_acquire",
        resource,
        caller.limits,
        undefined,
        true,
        true,
      );
      const authorize = current.pipe(
        Effect.andThen(authority.value.checkRecord(envelope.recordId)),
      );
      access = yield* peers.value.readNativeBrowserDownload({
        resource,
        authority: envelope,
        authorize,
      });
    }
    const bytes = new Uint8Array(resource.sizeBytes);
    yield* nativeDownloadStream(access, resource.sizeBytes).pipe(
      Stream.runFold(
        () => 0,
        (offset, chunk) => {
          bytes.set(chunk, offset);
          return offset + chunk.byteLength;
        },
      ),
    );
    if (
      (yield* (yield* Digest.Digest)
        .digest(Digest.sha256, bytes)
        .pipe(Effect.map(Hex.encode), Effect.mapError(refused))) !== resource.digest
    )
      return yield* refused();
    admitted = { bytes, producer: "preview", authorize: access.authorize };
  } else if (resource.reference.environmentId === scope.environmentId) {
    const authorize = (capability: "device" | "preview") =>
      current.pipe(
        Effect.andThen(authority.value.recheckInvocation(scope, capability, resource.threadId)),
        Effect.asVoid,
      );
    const producer = yield* staging.value.authorizeReference(resource, authorize);
    const bytes = yield* staging.value.readReference(resource, authorize);
    admitted = {
      bytes,
      producer,
      authorize: staging.value.authorizeReference(resource, authorize).pipe(Effect.asVoid),
    };
  } else {
    if (Option.isNone(peers) || peers.value.readAttachment === undefined || Option.isNone(links))
      return yield* refused();
    const link = yield* links.value
      .get(resource.reference.environmentId)
      .pipe(Effect.mapError(refused));
    if (Option.isNone(link)) return yield* refused();
    const envelope = yield* authority.value.prepare(
      scope,
      resource.reference.environmentId,
      link.value.launchIdentity,
      "attachment_acquire",
      resource,
      caller.limits,
      undefined,
      true,
      true,
    );
    const authorize = current.pipe(Effect.andThen(authority.value.checkRecord(envelope.recordId)));
    admitted = yield* peers.value.readAttachment({ resource, authority: envelope, authorize });
  }
  const authorize = admitted.authorize.pipe(
    Effect.andThen(current),
    Effect.andThen(
      authority.value.recheckInvocation(scope, admitted.producer, scope.thread.threadId),
    ),
    Effect.asVoid,
  );
  yield* authorize;
  const acquired = yield* staging.value.acquireReference(
    scope,
    resource._tag === "peer-attachment" ? resource.reference : resource,
    admitted.bytes,
    `acquire:${key}`,
    admitted.producer,
    authorize,
  );
  yield* authorize;
  return {
    acceptance: "native-accepted" as const,
    resource: {
      _tag: "peer-attachment" as const,
      threadId: scope.thread.threadId,
      reference: acquired.reference,
    },
    attachment: acquired.attachment,
    path: acquired.path,
  };
});

/** Bounded native download blocks remain at the browser owner, not the attachment store. */
export const qualifiedNativeDownloadReader = Effect.gen(function* () {
  const sessions = yield* Effect.serviceOption(AuthSessions.AuthSessionRepository);
  const environment = yield* Effect.serviceOption(ServerEnvironment.ServerEnvironment);
  const browser = yield* Effect.serviceOption(ServerBrowser.ServerBrowser);
  const links = yield* Effect.serviceOption(PeerLinks.PeerLinks);
  const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const peers = yield* Effect.serviceOption(PeerMcpClient.PeerMcpClient);
  return Effect.fn("AssetAccess.readQualifiedNativeDownload")(function* (
    session: EnvironmentAuth.AuthenticatedSession,
    resource: NativeBrowserDownloadAssetResource,
    expectedGeneration?: string,
  ) {
    const refused = () =>
      new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "The native browser download is unavailable to its current source reader.",
      });
    if (Option.isNone(sessions) || Option.isNone(environment)) return yield* refused();
    const current = (yield* EnvironmentAuth.nativeSessionAuthorization(
      session,
      "orchestration:read",
    ).pipe(
      Effect.provideService(AuthSessions.AuthSessionRepository, sessions.value),
      Effect.mapError(refused),
    )).pipe(Effect.mapError(refused));
    yield* current;
    const here = yield* environment.value.getEnvironmentId;
    if (resource.resource.environmentId === here) {
      if (
        (expectedGeneration !== undefined && expectedGeneration !== "local") ||
        Option.isNone(browser) ||
        browser.value.inspectDownload === undefined ||
        browser.value.readDownloadBlock === undefined
      )
        return yield* refused();
      const authorize = browser.value.inspectDownload(resource, current);
      yield* authorize;
      const read = browser.value.readDownloadBlock;
      return {
        generation: "local",
        authorize,
        readBlock: (offset: number) => read(resource, offset, current),
      };
    }
    if (
      Option.isNone(links) ||
      Option.isNone(authority) ||
      Option.isNone(peers) ||
      peers.value.readNativeBrowserDownload === undefined
    )
      return yield* refused();
    const link = yield* links.value
      .get(resource.resource.environmentId)
      .pipe(Effect.mapError(refused));
    if (
      Option.isNone(link) ||
      (expectedGeneration !== undefined && link.value.launchIdentity !== expectedGeneration)
    )
      return yield* refused();
    const envelope = yield* authority.value.prepareAttachmentRead(
      session.sessionId,
      resource.resource.environmentId,
      link.value.launchIdentity,
      resource,
    );
    const recheck = current.pipe(Effect.andThen(authority.value.checkRecord(envelope.recordId)));
    const admitted = yield* peers.value.readNativeBrowserDownload({
      resource,
      authority: envelope,
      authorize: recheck,
    });
    return { ...admitted, generation: link.value.launchIdentity };
  });
});

/** One upstream block per downstream pull; no read-ahead or whole-file allocation. */
export const nativeDownloadStream = (
  admitted: {
    readonly authorize: Effect.Effect<void, OrchestratorMcpFailure>;
    readonly readBlock: (offset: number) => Effect.Effect<Uint8Array, OrchestratorMcpFailure>;
  },
  sizeBytes: number,
  offset = 0,
  length = sizeBytes,
) =>
  Stream.unfold(offset, (position) =>
    Effect.gen(function* () {
      if (position >= offset + length) return undefined;
      yield* admitted.authorize;
      const blockOffset =
        Math.floor(position / ServerBrowser.DOWNLOAD_BLOCK_BYTES) *
        ServerBrowser.DOWNLOAD_BLOCK_BYTES;
      const block = yield* admitted.readBlock(blockOffset);
      if (
        block.byteLength !== Math.min(ServerBrowser.DOWNLOAD_BLOCK_BYTES, sizeBytes - blockOffset)
      )
        return yield* new OrchestratorMcpFailure({
          code: "invalid_request",
          message: "The native download block has an invalid byte count.",
        });
      yield* admitted.authorize;
      const end = Math.min(offset + length, blockOffset + block.byteLength);
      return [block.subarray(position - blockOffset, end - blockOffset), end] as const;
    }),
  );

export const qualifiedAttachmentReader = Effect.gen(function* () {
  const sessions = yield* Effect.serviceOption(AuthSessions.AuthSessionRepository);
  const environment = yield* Effect.serviceOption(ServerEnvironment.ServerEnvironment);
  const staging = yield* Effect.serviceOption(AttachmentStaging.AttachmentStaging);
  const links = yield* Effect.serviceOption(PeerLinks.PeerLinks);
  const authority = yield* Effect.serviceOption(PeerAuthority.PeerAuthority);
  const peers = yield* Effect.serviceOption(PeerMcpClient.PeerMcpClient);
  return Effect.fn("AssetAccess.readQualifiedAttachment")(function* (
    session: EnvironmentAuth.AuthenticatedSession,
    resource: PeerAttachmentAssetResource,
    expectedGeneration?: string,
  ) {
    const refused = () =>
      new OrchestratorMcpFailure({
        code: "capability_denied",
        message: "The qualified attachment is unavailable to its current reader.",
      });
    if (Option.isNone(sessions))
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "Qualified attachment native session storage is unavailable.",
      });
    if (Option.isNone(environment))
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "Qualified attachment environment identity is unavailable.",
      });
    const authorize = yield* EnvironmentAuth.nativeSessionAuthorization(
      session,
      "orchestration:read",
    ).pipe(
      Effect.provideService(AuthSessions.AuthSessionRepository, sessions.value),
      Effect.mapError(refused),
    );
    const current = authorize.pipe(Effect.mapError(refused));
    yield* current;
    const here = yield* environment.value.getEnvironmentId;
    if (resource.reference.environmentId === here) {
      if (expectedGeneration !== undefined && expectedGeneration !== "local")
        return yield* refused();
      if (Option.isNone(staging)) return yield* refused();
      const bytes = yield* staging.value.readReference(resource, () => current);
      return {
        bytes,
        generation: "local",
        authorize: staging.value.authorizeReference(resource, () => current).pipe(Effect.asVoid),
      };
    }
    if (
      Option.isNone(links) ||
      Option.isNone(authority) ||
      Option.isNone(peers) ||
      peers.value.readAttachment === undefined
    )
      return yield* new OrchestratorMcpFailure({
        code: "invalid_request",
        message: "Qualified attachment transport owners are unavailable.",
      });
    const link = yield* links.value
      .get(resource.reference.environmentId)
      .pipe(Effect.mapError(refused));
    if (
      Option.isNone(link) ||
      (expectedGeneration !== undefined && expectedGeneration !== link.value.launchIdentity)
    )
      return yield* refused();
    const envelope = yield* authority.value.prepareAttachmentRead(
      session.sessionId,
      resource.reference.environmentId,
      link.value.launchIdentity,
      resource,
    );
    const recheck = current.pipe(Effect.andThen(authority.value.checkRecord(envelope.recordId)));
    const admitted = yield* peers.value.readAttachment({
      resource,
      authority: envelope,
      authorize: recheck,
    });
    return { ...admitted, generation: link.value.launchIdentity };
  });
});

export const issueAssetUrl = Effect.fn("AssetAccess.issueAssetUrl")(function* (input: {
  readonly resource: AssetResource;
  readonly session?: EnvironmentAuth.AuthenticatedSession;
  readonly qualifiedReader?: Effect.Success<typeof qualifiedAttachmentReader>;
  readonly downloadReader?: Effect.Success<typeof qualifiedNativeDownloadReader>;
  readonly workspaceRoot?: string;
  readonly projectFaviconPath?: string;
  /** Where a cloned project's checkout stands, while the clone tracker still knows it. */
  readonly projectCheckout?: "pending" | "cloned";
}) {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
  let expiresAt = (yield* Clock.currentTimeMillis) + ASSET_TOKEN_TTL_MS;
  let claims: AssetClaims;
  let fileName: string;
  let sourcePath: string | undefined;
  let imageDimensions: ImageDimensions | null = null;
  let fileRevision: string | undefined;

  switch (input.resource._tag) {
    case "native-browser-download": {
      if (input.session === undefined)
        return yield* new AssetAttachmentNotFoundError({ resource: input.resource });
      const reader = input.downloadReader ?? (yield* qualifiedNativeDownloadReader);
      const admitted = yield* reader(input.session, input.resource).pipe(
        Effect.mapError(() => new AssetAttachmentNotFoundError({ resource: input.resource })),
      );
      yield* admitted.authorize.pipe(
        Effect.mapError(() => new AssetAttachmentNotFoundError({ resource: input.resource })),
      );
      claims = {
        version: 1,
        kind: "native-browser-download",
        resource: input.resource,
        sessionId: input.session.sessionId,
        generation: admitted.generation,
        expiresAt,
      };
      fileName = "artifact";
      break;
    }
    case "peer-attachment": {
      if (input.session === undefined)
        return yield* new AssetAttachmentNotFoundError({ resource: input.resource });
      const reader = input.qualifiedReader ?? (yield* qualifiedAttachmentReader);
      const admitted = yield* reader(input.session, input.resource).pipe(
        Effect.mapError(() => new AssetAttachmentNotFoundError({ resource: input.resource })),
      );
      claims = {
        version: 1,
        kind: "peer-attachment",
        resource: input.resource,
        sessionId: input.session.sessionId,
        generation: admitted.generation,
        expiresAt,
      };
      fileName = "artifact";
      break;
    }
    case "media-file": {
      let requestedPath = expandHomePath(
        yield* toHostDrivePath(input.resource.path),
        yield* HostProcess.HomeDirectory,
      );
      if (!path.isAbsolute(requestedPath)) {
        if (!input.workspaceRoot) {
          return yield* new AssetWorkspaceContextNotFoundError({ resource: input.resource });
        }
        const workspaceRoot = yield* workspacePaths
          .normalizeWorkspaceRoot(input.workspaceRoot)
          .pipe(
            Effect.mapError(
              (cause) =>
                new AssetWorkspaceRootNormalizationError({ resource: input.resource, cause }),
            ),
          );
        requestedPath = path.resolve(workspaceRoot, requestedPath);
      }
      const finalized = yield* finalizeAbsoluteMediaFileAsset({
        requestedPath,
        workspaceRoot: input.workspaceRoot,
        resource: input.resource,
        expiresAt,
      });
      claims = finalized.claims;
      fileName = finalized.fileName;
      imageDimensions = finalized.imageDimensions;
      fileRevision = finalized.fileRevision;
      break;
    }
    case "workspace-file": {
      if (!input.workspaceRoot) {
        return yield* new AssetWorkspaceContextNotFoundError({
          resource: input.resource,
        });
      }
      const workspaceRoot = yield* workspacePaths.normalizeWorkspaceRoot(input.workspaceRoot).pipe(
        Effect.mapError(
          (cause) =>
            new AssetWorkspaceRootNormalizationError({
              resource: input.resource,
              cause,
            }),
        ),
      );
      const finalized = yield* finalizeWorkspaceFileAsset({
        workspaceRoot,
        requestedPath: input.resource.path,
        resource: input.resource,
        expiresAt,
      });
      claims = finalized.claims;
      fileName = finalized.fileName;
      imageDimensions = finalized.imageDimensions;
      fileRevision = finalized.fileRevision;
      break;
    }
    case "draft-workspace-file": {
      const draftWorkspaceRoot = input.workspaceRoot ?? input.resource.cwd;
      const hostPath =
        input.resource.path.startsWith("~/") || input.resource.path.startsWith("~\\")
          ? expandHomePath(input.resource.path, yield* HostProcess.HomeDirectory)
          : yield* toHostDrivePath(input.resource.path);
      if (path.isAbsolute(hostPath)) {
        const finalized = yield* finalizeAbsoluteMediaFileAsset({
          requestedPath: hostPath,
          workspaceRoot: draftWorkspaceRoot,
          resource: input.resource,
          expiresAt,
        });
        claims = finalized.claims;
        fileName = finalized.fileName;
        imageDimensions = finalized.imageDimensions;
        fileRevision = finalized.fileRevision;
        break;
      }
      const workspaceRoot = yield* workspacePaths.normalizeWorkspaceRoot(draftWorkspaceRoot).pipe(
        Effect.mapError(
          (cause) =>
            new AssetWorkspaceRootNormalizationError({
              resource: input.resource,
              cause,
            }),
        ),
      );
      const finalized = yield* finalizeWorkspaceFileAsset({
        workspaceRoot,
        requestedPath: input.resource.path,
        resource: input.resource,
        expiresAt,
      });
      claims = finalized.claims;
      fileName = finalized.fileName;
      imageDimensions = finalized.imageDimensions;
      fileRevision = finalized.fileRevision;
      break;
    }
    case "attachment": {
      const config = yield* ServerConfig.ServerConfig;
      const attachmentPath = resolveAttachmentPathById({
        attachmentsDir: config.attachmentsDir,
        attachmentId: input.resource.attachmentId,
      });
      if (!attachmentPath) {
        return yield* new AssetAttachmentNotFoundError({
          resource: input.resource,
        });
      }
      const extension = parseAttachmentFileExtension(input.resource.attachmentId);
      const isGenericFile = extension !== null;
      const videoMimeType = input.resource.mimeType?.split(";", 1)[0]?.trim() ?? "";
      const isVideo = INLINE_VIDEO_MIME_TYPE_PATTERN.test(videoMimeType);
      const inlinePreviewMimeType =
        input.resource.disposition === "inline" && extension !== null
          ? inlinePreviewMimeTypeForExtension(extension)
          : undefined;
      if (!isGenericFile) {
        imageDimensions = yield* fileSystem.realPath(attachmentPath).pipe(
          Effect.flatMap(readImageDimensionsFromHeader),
          Effect.orElseSucceed(() => null),
        );
      }
      const download = isGenericFile && !isVideo && inlinePreviewMimeType === undefined;
      // A client-named type is kept only where it cannot make the bytes render as a document.
      const mimeType =
        inlinePreviewMimeType ??
        (isVideo ? videoMimeType : download ? input.resource.mimeType : undefined);
      claims = {
        version: 1,
        kind: "attachment",
        attachmentId: input.resource.attachmentId,
        ...(download ? { download: true } : {}),
        ...(input.resource.fileName !== undefined ? { fileName: input.resource.fileName } : {}),
        ...(mimeType !== undefined ? { mimeType } : {}),
        ...(input.resource.renderIntent === "mcp-app" && inlinePreviewMimeType === "text/html"
          ? { renderIntent: "mcp-app" as const }
          : {}),
        expiresAt,
      };
      fileName = input.resource.fileName ?? path.basename(attachmentPath);
      break;
    }
    case "project-favicon": {
      const workspaceRoot = yield* workspacePaths.normalizeWorkspaceRoot(input.resource.cwd).pipe(
        Effect.mapError(
          (cause) =>
            new AssetWorkspaceRootNormalizationError({
              resource: input.resource,
              cause,
            }),
        ),
      );
      const faviconResolver = yield* ProjectFaviconResolver.ProjectFaviconResolver;
      // A lookup in a half-cloned checkout would cache a miss that outlives the clone.
      // Once it lands, a miss cached before (by enrichment) is stale too.
      if (input.projectCheckout === "cloned") yield* faviconResolver.invalidate(workspaceRoot);
      const faviconPath =
        input.projectCheckout === "pending"
          ? null
          : yield* faviconResolver
              .resolvePath(workspaceRoot, input.projectFaviconPath ?? undefined)
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new AssetProjectFaviconResolutionError({
                      resource: input.resource,
                      cause,
                    }),
                ),
              );
      const isExternalOverride =
        faviconPath !== null &&
        input.projectFaviconPath !== undefined &&
        path.isAbsolute(input.projectFaviconPath) &&
        path.normalize(faviconPath) === path.normalize(input.projectFaviconPath);
      const relativePath =
        faviconPath && !isExternalOverride ? path.relative(workspaceRoot, faviconPath) : null;
      const sourceFaviconPath = isExternalOverride ? faviconPath : relativePath;
      if (sourceFaviconPath && !isProjectFaviconPath(sourceFaviconPath)) {
        return yield* new AssetPreviewTypeValidationError({ resource: input.resource });
      }
      sourcePath = sourceFaviconPath ?? undefined;
      const canonicalFaviconPath = sourceFaviconPath
        ? yield* (
            isExternalOverride
              ? resolveCanonicalFile(sourceFaviconPath)
              : resolveCanonicalWorkspaceFile({ workspaceRoot, relativePath: sourceFaviconPath })
          ).pipe(
            Effect.mapError(
              (cause) =>
                new AssetProjectFaviconInspectionError({
                  resource: input.resource,
                  cause,
                }),
            ),
          )
        : null;
      if (sourceFaviconPath && !canonicalFaviconPath) {
        return yield* new AssetProjectFaviconNotFoundError({
          resource: input.resource,
        });
      }
      // A favicon link must resolve to an image file.
      if (canonicalFaviconPath && !isProjectFaviconPath(canonicalFaviconPath)) {
        return yield* new AssetPreviewTypeValidationError({ resource: input.resource });
      }
      claims =
        isExternalOverride && canonicalFaviconPath
          ? {
              version: 1,
              kind: "project-favicon-external",
              filePath: canonicalFaviconPath,
              expiresAt,
            }
          : {
              version: 1,
              kind: "project-favicon",
              workspaceRoot: yield* fileSystem.realPath(workspaceRoot).pipe(
                Effect.mapError(
                  (cause) =>
                    new AssetWorkspaceResolutionError({
                      resource: input.resource,
                      cause,
                    }),
                ),
              ),
              relativePath,
              expiresAt,
            };
      if (sourceFaviconPath && canonicalFaviconPath) {
        const digest = yield* Digest.Digest;
        const faviconBytes = yield* fileSystem.readFile(canonicalFaviconPath).pipe(
          Effect.mapError(
            (cause) =>
              new AssetProjectFaviconInspectionError({
                resource: input.resource,
                cause,
              }),
          ),
        );
        const revision = yield* digest.digest(Digest.sha256, faviconBytes).pipe(
          Effect.map(Hex.encode),
          Effect.mapError(
            (cause) =>
              new AssetProjectFaviconInspectionError({
                resource: input.resource,
                cause,
              }),
          ),
        );
        fileName = `${PROJECT_FAVICON_VERSION_PREFIX}${revision}-${path.basename(sourceFaviconPath)}`;
      } else {
        fileName = PROJECT_FAVICON_FALLBACK_MARKER;
      }
      break;
    }
    case "tool-output-image": {
      const image = yield* readToolOutputImage(input.resource).pipe(
        Effect.mapError(
          (cause) => new AssetWorkspaceAssetInspectionError({ resource: input.resource, cause }),
        ),
      );
      if (image === null) {
        return yield* new AssetWorkspaceAssetNotFoundError({ resource: input.resource });
      }
      claims = {
        version: 1,
        kind: "tool-output-image",
        threadId: input.resource.threadId,
        itemId: input.resource.itemId,
        index: input.resource.index,
        expiresAt,
      };
      fileName = `image-${input.resource.index + 1}.${image.mimeType.slice("image/".length)}`;
      imageDimensions = readImageDimensions(image.bytes);
      break;
    }
    case "environment-theme-background": {
      // Published themes go to every client, so any client may read their backgrounds.
      claims = {
        version: 1,
        kind: "environment-theme-background",
        themeId: input.resource.themeId,
        expiresAt,
      };
      fileName = `${input.resource.themeId}-background`;
      break;
    }
    case "native-app-icon": {
      claims = {
        version: 1,
        kind: "native-app-icon",
        app: input.resource.app,
        expiresAt,
      };
      fileName = "native-app-icon.png";
      break;
    }
    case SOURCE_CONTROL_MEDIA_ASSET: {
      const media = sourceControlDefinitions.mediaUrl(input.resource.url);
      if (media === null) {
        return yield* new AssetSourceControlMediaUrlValidationError({});
      }
      const { _tag, ...resource } = input.resource;
      claims = {
        version: 1,
        kind: SOURCE_CONTROL_MEDIA_ASSET,
        ...resource,
        url: media.fetchUrl,
        expiresAt,
      };
      fileName = media.fileName;
      break;
    }
  }

  if (claims.kind === "project-favicon" || claims.kind === "project-favicon-external") {
    const issuedAt = yield* Clock.currentTimeMillis;
    expiresAt =
      (Math.floor(issuedAt / PROJECT_FAVICON_TOKEN_BUCKET_MS) + 2) *
      PROJECT_FAVICON_TOKEN_BUCKET_MS;
    claims = { ...claims, expiresAt };
  }
  if (claims.kind === "attachment" && claims.mimeType === "text/html") {
    // Stable URLs let immutable HTML reloads hit the cache; image retries need fresh URLs.
    expiresAt =
      Math.ceil(expiresAt / HTML_ATTACHMENT_TOKEN_BUCKET_MS) * HTML_ATTACHMENT_TOKEN_BUCKET_MS;
    claims = { ...claims, expiresAt };
  }
  const encodedPayload = base64UrlEncode(encodeAssetClaims(claims));
  const signer = yield* AssetSigner.AssetSigner;
  const signature = yield* signer
    .sign(encodedPayload)
    .pipe(
      Effect.mapError((cause) => new AssetSigningKeyLoadError({ resource: input.resource, cause })),
    );
  const token = `${encodedPayload}.${signature}`;
  return {
    relativeUrl: `${ASSET_ROUTE_PREFIX}/${token}/${encodeURIComponent(fileName)}`,
    expiresAt,
    ...(sourcePath !== undefined ? { sourcePath } : {}),
    ...(imageDimensions !== null ? { imageDimensions } : {}),
    ...(fileRevision !== undefined ? { fileRevision } : {}),
  };
});

export const resolveAsset = Effect.fn("AssetAccess.resolveAsset")(function* (
  token: string,
  relativePath: string,
) {
  const [encodedPayload, signature] = token.split(".");
  if (!encodedPayload || !signature) return null;

  const signer = yield* AssetSigner.AssetSigner;
  const verified = yield* signer.verify(encodedPayload, signature).pipe(
    Effect.tapError((cause) => Effect.logError("Failed to load the asset signing key.", { cause })),
    Effect.orElseSucceed(() => false),
  );
  if (!verified) return null;

  const claims = decodeClaims(encodedPayload);
  if (!claims || claims.expiresAt <= (yield* Clock.currentTimeMillis)) return null;

  if (claims.kind === "native-browser-download") {
    if (relativePath !== "artifact") return null;
    return {
      kind: claims.kind,
      resource: claims.resource,
      sessionId: claims.sessionId,
      generation: claims.generation,
    } satisfies ResolvedAsset;
  }
  if (claims.kind === "peer-attachment") {
    if (relativePath !== "artifact") return null;
    return {
      kind: claims.kind,
      resource: claims.resource,
      sessionId: claims.sessionId,
      generation: claims.generation,
    } satisfies ResolvedAsset;
  }
  if (claims.kind === "attachment") {
    const config = yield* ServerConfig.ServerConfig;
    const attachmentPath = resolveAttachmentPathById({
      attachmentsDir: config.attachmentsDir,
      attachmentId: claims.attachmentId,
    });
    if (!attachmentPath) return null;
    const fileSystem = yield* FileSystem.FileSystem;
    const info = yield* ProjectFaviconResolver.optionOnNotFound(
      fileSystem.stat(attachmentPath),
    ).pipe(
      Effect.tapError((cause) =>
        Effect.logError("Failed to inspect attachment asset.", {
          attachmentId: claims.attachmentId,
          path: attachmentPath,
          cause,
        }),
      ),
      Effect.orElseSucceed(() => Option.none()),
    );
    return Option.isSome(info) && info.value.type === "File"
      ? ({
          kind: "file",
          path: attachmentPath,
          ...(claims.download ? { download: true } : {}),
          ...(claims.fileName !== undefined ? { fileName: claims.fileName } : {}),
          ...(claims.mimeType !== undefined ? { mimeType: claims.mimeType } : {}),
          ...(claims.renderIntent === "mcp-app" ? { renderIntent: "mcp-app" as const } : {}),
        } satisfies ResolvedAsset)
      : null;
  }

  if (claims.kind === "project-favicon") {
    if (claims.relativePath === null) return null;
    const faviconPath = yield* resolveCanonicalWorkspaceFileForRequest({
      workspaceRoot: claims.workspaceRoot,
      relativePath: claims.relativePath,
    });
    return faviconPath ? yield* projectFaviconAsset(faviconPath) : null;
  }

  if (claims.kind === "project-favicon-external") {
    const faviconPath = yield* resolveCanonicalFile(claims.filePath).pipe(
      Effect.tapError((cause) =>
        Effect.logError("Failed to resolve canonical asset path.", {
          filePath: claims.filePath,
          cause,
        }),
      ),
      Effect.orElseSucceed(() => null),
    );
    return faviconPath === claims.filePath ? yield* projectFaviconAsset(faviconPath) : null;
  }

  if (claims.kind === SOURCE_CONTROL_MEDIA_ASSET) {
    const { version: _version, ...media } = claims;
    return { ...media, kind: "source-control-media" } satisfies ResolvedAsset;
  }

  if (claims.kind === "tool-output-image") {
    const image = yield* readToolOutputImage(claims).pipe(
      Effect.tapError((cause) =>
        Effect.logError("Failed to read tool output image.", {
          threadId: claims.threadId,
          itemId: claims.itemId,
          cause,
        }),
      ),
      Effect.orElseSucceed(() => null),
    );
    return image ? ({ kind: "bytes", ...image } satisfies ResolvedAsset) : null;
  }

  if (claims.kind === "environment-theme-background") {
    const config = yield* ServerConfig.ServerConfig;
    const fileSystem = yield* FileSystem.FileSystem;
    // The themes directory may be a symlink; openMediaFile refuses any path that is not canonical.
    const themesDir = yield* fileSystem
      .realPath(config.environmentThemesDir)
      .pipe(Effect.orElseSucceed(() => null));
    const background = themesDir === null ? null : findThemeBackground(themesDir, claims.themeId);
    if (background === null) return null;
    const file = yield* openMediaFile(background.path).pipe(
      Effect.tapError((cause) =>
        Effect.logError("Failed to open theme background.", { path: background.path, cause }),
      ),
      Effect.orElseSucceed(() => null),
    );
    return file ? ({ kind: "file", path: background.path, file } satisfies ResolvedAsset) : null;
  }

  if (claims.kind === "native-app-icon") {
    const nativeAppIconResolver = yield* NativeAppIconResolver.NativeAppIconResolver;
    const iconPath = yield* nativeAppIconResolver.resolve(claims.app);
    return iconPath ? ({ kind: "file", path: iconPath } satisfies ResolvedAsset) : null;
  }

  const decodedPath = decodeRelativePath(relativePath);
  if (decodedPath === null) return null;
  const path = yield* Path.Path;
  if (claims.kind === "media-file-exact") {
    if (decodedPath !== path.basename(claims.filePath)) return null;
    const canonicalFile = yield* resolveCanonicalFile(claims.filePath).pipe(
      Effect.tapError((cause) =>
        Effect.logError("Failed to resolve canonical media path.", {
          filePath: claims.filePath,
          cause,
        }),
      ),
      Effect.orElseSucceed(() => null),
    );
    if (canonicalFile !== claims.filePath) return null;
    const mimeType = claims.download
      ? "application/octet-stream"
      : hostPreviewMimeTypeFromExtension(path.extname(canonicalFile));
    if (!mimeType) return null;
    const file = yield* openMediaFile(canonicalFile, claims).pipe(
      Effect.tapError((cause) =>
        Effect.logError("Failed to open canonical media file.", { filePath: canonicalFile, cause }),
      ),
      Effect.orElseSucceed(() => null),
    );
    return file
      ? ({
          kind: "file",
          path: canonicalFile,
          mimeType,
          file,
          ...(claims.download ? { download: true, fileName: path.basename(canonicalFile) } : {}),
        } satisfies ResolvedAsset)
      : null;
  }
  if (claims.kind === "workspace-file-exact") {
    if (decodedPath !== path.basename(claims.relativePath)) return null;
    const exactWorkspaceFile = yield* resolveCanonicalWorkspaceFileForRequest({
      workspaceRoot: claims.workspaceRoot,
      relativePath: claims.relativePath,
    });
    // Checked again here: a link can be repointed after its URL was issued.
    return exactWorkspaceFile && isWorkspacePreviewEntryPath(exactWorkspaceFile)
      ? ({ kind: "file", path: exactWorkspaceFile } satisfies ResolvedAsset)
      : null;
  }
  if (!isPreviewDependencyPath(decodedPath, path)) return null;
  const joinedRelativePath =
    claims.baseRelativePath === "." ? decodedPath : path.join(claims.baseRelativePath, decodedPath);
  if (claims.entryFileName !== undefined) {
    // Before the file is resolved and read, so the recorded state is never newer than the response.
    const revisions = yield* WorkspacePreviewRevisions.WorkspacePreviewRevisions;
    yield* revisions.observe(
      {
        workspaceRoot: claims.workspaceRoot,
        entryRelativePath: path.join(claims.baseRelativePath, claims.entryFileName),
      },
      joinedRelativePath,
    );
  }
  const workspaceFile = yield* resolveCanonicalWorkspaceFileForRequest({
    workspaceRoot: claims.workspaceRoot,
    relativePath: joinedRelativePath,
    previewBaseRelativePath: claims.baseRelativePath,
    previewEntry: decodedPath === claims.entryFileName,
  });
  return workspaceFile
    ? ({ kind: "file", path: workspaceFile, renderIntent: "file-preview" } satisfies ResolvedAsset)
    : null;
});
