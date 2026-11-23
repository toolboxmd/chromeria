import { createContext, useMemo, type ReactNode } from "react";
import { View } from "react-native";

import { promachosBubbleFragments } from "./promachosBubbles";

/** Whether the thread on screen is a Promachos chat, whose replies render as bubbles. */
export const PromachosChatContext = createContext(false);

/** One assistant reply as Promachos bubbles; `renderText` renders each bubble's Markdown. */
export function PromachosBubbles(props: {
  readonly markdown: string;
  readonly renderText: (markdown: string) => ReactNode;
}) {
  const fragments = useMemo(() => promachosBubbleFragments(props.markdown), [props.markdown]);
  return (
    <View className="items-start gap-1">
      {fragments.map((fragment) =>
        fragment.kind === "break" ? (
          <View key={fragment.offset} className="my-2 h-px self-stretch bg-separator" />
        ) : (
          <View
            key={fragment.offset}
            className={
              fragment.kind === "bubble"
                ? "min-w-0 max-w-[85%] rounded-2xl bg-card px-4 py-2.5"
                : "self-stretch"
            }
          >
            {props.renderText(fragment.markdown)}
          </View>
        ),
      )}
    </View>
  );
}
