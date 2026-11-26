import { createFileRoute } from "@tanstack/react-router";

import { redirectToScheduledTasksPage } from "../components/settings/scheduledTasksPage";

export const Route = createFileRoute("/_chat/settings/scheduled-tasks")({
  beforeLoad: redirectToScheduledTasksPage,
});
