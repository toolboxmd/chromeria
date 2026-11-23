import { useGT } from "gt-react-native";

import { uuidv4 } from "../../lib/uuid";
import { useSelectWorkspaceDetail } from "../layout/workspace/useSelectWorkspaceDetail";
import { usePromachosHome } from "./promachosHome";

/** The compose button's action on the Promachos tab: a new conversation in his home. */
export function usePromachosCompose() {
  const gt = useGT();
  const { home } = usePromachosHome();
  const selectDetail = useSelectWorkspaceDetail();
  if (home === null) return null;
  return {
    label: gt("New conversation"),
    onPress: () =>
      selectDetail({
        name: "Thread",
        params: {
          draft: {
            requestId: uuidv4(),
            environmentId: String(home.environmentId),
            projectId: String(home.projectId),
            promachos: "1",
          },
        },
      }),
  } as const;
}
