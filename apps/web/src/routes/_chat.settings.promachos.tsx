import { createFileRoute } from "@tanstack/react-router";
import { PromachosSettings } from "../components/settings/PromachosSettings";

export const Route = createFileRoute("/_chat/settings/promachos")({ component: PromachosSettings });
