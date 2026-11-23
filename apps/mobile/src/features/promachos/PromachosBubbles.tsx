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
    <View className="items-start gap-2">
      {fragments.map((fragment) =>
        fragment.kind === "break" ? (
          <View key={fragment.offset} className="my-2 h-px self-stretch bg-separator" />
        ) : (
          <View
            key={fragment.offset}
            className={
              fragment.kind === "outside"
                ? "self-stretch"
                : fragment.card
                  ? "min-w-0 max-w-[82%]"
                  : "min-w-0 max-w-[82%] rounded-[18px] bg-promachos-inbound px-3.5 py-2.5"
            }
          >
            {props.renderText(fragment.markdown)}
          </View>
        ),
      )}
    </View>
  );
}
