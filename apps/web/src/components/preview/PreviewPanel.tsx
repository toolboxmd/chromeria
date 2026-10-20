import {
  openServerPictureInPicture,
  closeServerPictureInPicture,
  serverPictureInPictureKey,
  useServerPictureInPictureKey,
  supportsServerPictureInPicture,
} from "~/browser/serverPictureInPicture";
import { usePreviewMiniPlayerStore } from "~/previewMiniPlayerStore";

import {
  AuthPreviewOperateScope,
  type PreviewAnnotationPayload,
  type ScopedThreadRef,
  type PreviewViewerResource,
} from "@t3tools/contracts";

import { useAtomValue } from "@effect/atom-react";
import { previewEnvironment } from "~/state/preview";
import { useRef, useState } from "react";
import { ServerBrowserSurface, type ServerBrowserHandle } from "~/browser/ServerBrowserSurface";
import { Input } from "~/components/ui/input";
import { Button } from "~/components/ui/button";
import { browserResourceSurfaceId, useRightPanelStore } from "~/rightPanelStore";
import type { ComposerImageAttachment } from "~/composerDraftStore";
import { usePreviewAvailable } from "~/browser/previewRuntime";
import { useEnvironmentScope } from "~/state/session";

import { PreviewPanelShell, type PreviewPanelMode } from "./PreviewPanelShell";
import { PreviewView } from "./PreviewView";

interface Props {
  mode: PreviewPanelMode;
  threadRef: ScopedThreadRef;
  tabId?: string | null;
  resource?: PreviewViewerResource;
  configuredUrls?: ReadonlyArray<string> | undefined;
  visible: boolean;
  onSendAnnotation?: (
    annotation: PreviewAnnotationPayload,
    image: ComposerImageAttachment | null,
  ) => void;
}

export function PreviewPanel(props: Props) {
  return props.resource === undefined ? (
    <LocalPreviewPanel {...props} />
  ) : (
    <QualifiedPreviewPanel {...props} resource={props.resource} />
  );
}
function QualifiedPreviewPanel(props: Props & { resource: PreviewViewerResource }) {
  const handle = useRef<ServerBrowserHandle | null>(null);
  const canControl = useAtomValue(
    previewEnvironment.viewerOpen.permissionAtom(props.threadRef.environmentId, {
      resource: props.resource,
      operation: "control",
    }),
  );
  const [address, setAddress] = useState("");
  const [viewError, setViewError] = useState<string | null>(null);
  const pictureInPicture =
    useServerPictureInPictureKey() ===
    serverPictureInPictureKey(
      props.resource.threadId,
      props.resource.tabId,
      props.threadRef.environmentId,
      props.resource,
    );
  return (
    <PreviewPanelShell mode={props.mode}>
      <form
        className="flex items-center gap-2 border-b px-3 py-2"
        onSubmit={(event) => {
          event.preventDefault();
          if (canControl) handle.current?.navigate(address);
        }}
      >
        <Input
          aria-label="Browser address"
          disabled={!canControl}
          value={address}
          onChange={(event) => setAddress(event.target.value)}
          placeholder="Enter a URL"
        />
        <Button size="xs" variant="outline" type="submit" disabled={!canControl}>
          Go
        </Button>
        <Button
          size="xs"
          variant="outline"
          type="button"
          onClick={() => {
            usePreviewMiniPlayerStore.getState().open(props.threadRef, {
              kind: "browser",
              tabId: props.resource.tabId,
              resource: props.resource,
            });
            useRightPanelStore
              .getState()
              .closeSurface(props.threadRef, browserResourceSurfaceId(props.resource));
          }}
        >
          Float view
        </Button>
        <Button
          size="xs"
          variant="outline"
          type="button"
          disabled={!supportsServerPictureInPicture()}
          onClick={() => {
            if (pictureInPicture) closeServerPictureInPicture();
            else
              void openServerPictureInPicture({
                environmentId: props.threadRef.environmentId,
                threadId: props.resource.threadId,
                tabId: props.resource.tabId,
                resource: props.resource,
                seed: handle.current?.canvas() ?? null,
              }).catch((error: unknown) =>
                setViewError(
                  error instanceof Error ? error.message : "Picture in picture is unavailable.",
                ),
              );
          }}
        >
          {pictureInPicture ? "Close picture in picture" : "Picture in picture"}
        </Button>
        <Button
          size="xs"
          variant="ghost"
          type="button"
          onClick={() =>
            useRightPanelStore
              .getState()
              .closeSurface(props.threadRef, browserResourceSurfaceId(props.resource))
          }
        >
          Close view
        </Button>
      </form>
      {viewError ? (
        <p role="alert" className="px-3 py-2 text-xs text-destructive">
          {viewError}
        </p>
      ) : null}
      <ServerBrowserSurface
        ref={handle}
        environmentId={props.threadRef.environmentId}
        resource={props.resource}
        threadId={props.resource.threadId}
        tabId={props.resource.tabId}
        visible={props.visible}
        onPopup={(tabId, resource) => {
          if (resource !== undefined)
            useRightPanelStore.getState().openBrowser(props.threadRef, tabId, resource);
        }}
      />
    </PreviewPanelShell>
  );
}
function LocalPreviewPanel({
  mode,
  threadRef,
  tabId,
  configuredUrls,
  visible,
  onSendAnnotation,
}: Props) {
  const available = usePreviewAvailable(threadRef.environmentId);
  const canOperatePreview = useEnvironmentScope(threadRef.environmentId, AuthPreviewOperateScope);
  if (!canOperatePreview || !available) {
    return (
      <PreviewPanelShell mode={mode}>
        <div className="flex min-h-0 flex-1 flex-col items-center justify-center gap-3 p-8 text-center">
          <p className="max-w-sm text-sm text-muted-foreground">
            {canOperatePreview
              ? "Preview is only available in the T3 Code desktop app."
              : "Pair this client again with preview access to control browser previews."}
          </p>
        </div>
      </PreviewPanelShell>
    );
  }

  return (
    <PreviewPanelShell mode={mode}>
      <PreviewView
        threadRef={threadRef}
        {...(tabId !== undefined ? { tabId } : {})}
        configuredUrls={configuredUrls}
        visible={visible}
        {...(onSendAnnotation ? { onSendAnnotation } : {})}
      />
    </PreviewPanelShell>
  );
}
