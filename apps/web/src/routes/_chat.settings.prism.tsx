import { createFileRoute } from "@tanstack/react-router";
import { PrismSettings } from "../components/settings/PrismSettings";

export const Route = createFileRoute("/_chat/settings/prism")({ component: PrismSettings });
