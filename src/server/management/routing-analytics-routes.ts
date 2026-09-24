/**
 * Routing analytics API (RI-03): `GET /api/routing-analytics`.
 *
 * Returns source-backed reliability/latency/cost metrics over the
 * request-history index. Read-only; never changes routing behavior.
 */

import {
  ANALYTICS_API_DEFAULT_ROWS,
  ANALYTICS_MAX_ROWS,
  computeRoutingAnalytics,
} from "../../routing/analytics";
import { USAGE_RANGES, rangeWindow, type UsageRange } from "../../usage/summary";
import { jsonResponse } from "../auth-cors";
import type { ManagementContext } from "./context";

function parseQueryInt(raw: string | null): number | undefined | "invalid" {
  if (raw === null) return undefined;
  const trimmed = raw.trim();
  if (trimmed.length === 0) return "invalid";
  const value = Number(trimmed);
  return Number.isInteger(value) ? value : "invalid";
}

export async function handleRoutingAnalyticsRoutes(ctx: ManagementContext): Promise<Response | null> {
  const { url, req, config } = ctx;
  if (url.pathname !== "/api/routing-analytics" || req.method !== "GET") return null;

  const fromParsed = parseQueryInt(url.searchParams.get("from"));
  if (fromParsed === "invalid") {
    return jsonResponse({ error: { code: "invalid_from", message: "from must be an integer timestamp" } }, 400, req, config);
  }
  const toParsed = parseQueryInt(url.searchParams.get("to"));
  if (toParsed === "invalid") {
    return jsonResponse({ error: { code: "invalid_to", message: "to must be an integer timestamp" } }, 400, req, config);
  }
  // `range` is the dashboard's calendar shorthand, resolved through the SAME helper the usage
  // surface uses so both agree on where a local day starts. Explicit from/to wins: a caller that
  // states an exact window means it. An unrecognized value is refused rather than silently read
  // as the default window, which would answer a different question than the one asked.
  const rangeRaw = url.searchParams.get("range")?.trim();
  if (rangeRaw !== undefined && rangeRaw.length > 0
    && !(USAGE_RANGES as readonly string[]).includes(rangeRaw)) {
    return jsonResponse(
      { error: { code: "invalid_range", message: `range must be one of ${USAGE_RANGES.join(", ")}` } },
      400, req, config,
    );
  }
  const rangeSince = rangeRaw && fromParsed === undefined
    ? rangeWindow(rangeRaw as UsageRange, Date.now()).since ?? undefined
    : undefined;
  const from = fromParsed ?? rangeSince;
  const to = toParsed;
  if (from !== undefined && to !== undefined && from > to) {
    return jsonResponse({ error: { code: "invalid_range", message: "from must not be after to" } }, 400, req, config);
  }
  const limitParsed = parseQueryInt(url.searchParams.get("limit"));
  if (limitParsed === "invalid") {
    return jsonResponse(
      { error: { code: "invalid_limit", message: "limit must be an integer" } },
      400,
      req,
      config,
    );
  }
  const maxRows = limitParsed ?? ANALYTICS_API_DEFAULT_ROWS;
  if (maxRows < 1 || maxRows > ANALYTICS_MAX_ROWS) {
    return jsonResponse(
      {
        error: {
          code: "invalid_limit",
          message: `limit must be between 1 and ${ANALYTICS_MAX_ROWS}`,
        },
      },
      400,
      req,
      config,
    );
  }

  const result = await computeRoutingAnalytics({
    provider: url.searchParams.get("provider")?.trim() || undefined,
    model: url.searchParams.get("model")?.trim() || undefined,
    profileId: url.searchParams.get("profileId")?.trim() || undefined,
    surface: ((): "all" | "claude" | "codex" | "grok" | undefined => {
      const raw = url.searchParams.get("surface")?.trim();
      return raw === "all" || raw === "claude" || raw === "codex" || raw === "grok" ? raw : undefined;
    })(),
    from,
    to,
  }, { maxRows });
  return jsonResponse(result, 200, req, config);
}
