import { createFileRoute } from "@tanstack/react-router";
import { SpectrumSettings } from "../components/settings/SpectrumSettings";

export const Route = createFileRoute("/_chat/settings/spectrum")({ component: SpectrumSettings });
