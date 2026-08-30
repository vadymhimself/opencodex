import { IconAlert, IconCheck, IconRefresh } from "../icons";
import type { QuotaWatchData } from "./dashboard-shared";
import type { useDashboardData } from "./use-dashboard-data";

type Dash = ReturnType<typeof useDashboardData>;

type AlertKey = keyof QuotaWatchData["alerts"];

function formatTokens(value: number, locale: string): string {
  return new Intl.NumberFormat(locale, { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="stat">
      <div className="label">{label}</div>
      <div className="value mono">{value}</div>
    </div>
  );
}

function hasAlerts(data: QuotaWatchData): boolean {
  return Object.values(data.alerts).some(alert => alert.length > 0);
}

export function DashboardQuotaObservability({ d }: { d: Dash }) {
  const { quotaWatch, quotaWatchLoading, refreshQuotaWatch, locale, t } = d;
  if (!quotaWatch) {
    return (
      <section className="panel" aria-label={t("dash.quota.title")}>
        <div className="font-semibold">{t("dash.quota.title")}</div>
        <div className="muted text-control" style={{ marginTop: 8 }}>
          {quotaWatchLoading ? t("dash.quota.loading") : t("dash.quota.unavailable")}
        </div>
      </section>
    );
  }

  const signalRows: Array<[AlertKey, string]> = [
    ["rawInput", t("dash.quota.signal.rawInput")],
    ["cacheWrite", t("dash.quota.signal.cacheWrite")],
    ["cacheRead", t("dash.quota.signal.cacheRead")],
    ["retry", t("dash.quota.signal.retry")],
  ];
  const breached = signalRows.filter(([key]) => quotaWatch.alerts[key].length > 0);
  const coverage = quotaWatch.coverage.rawInput === null
    ? "—"
    : t("dash.coverage", { pct: `${Math.round(quotaWatch.coverage.rawInput * 100)}%` });

  return (
    <section className="panel" aria-label={t("dash.quota.title")}>
      <div className="injection-head">
        <div className="font-semibold">{t("dash.quota.title")}</div>
        <button
          type="button"
          className="btn btn-ghost btn-sm"
          onClick={() => { void refreshQuotaWatch(); }}
          aria-label={t("dash.quota.refresh")}
          title={t("dash.quota.refresh")}
        >
          <IconRefresh width={14} height={14} aria-hidden="true" />
          {t("dash.quota.refresh")}
        </button>
      </div>

      {hasAlerts(quotaWatch) ? (
        <div className="notice notice-err" role="alert" style={{ marginTop: 12 }}>
          <IconAlert />
          <span>{t("dash.quota.alert")}</span>
        </div>
      ) : (
        <div className="notice notice-ok" role="status" style={{ marginTop: 12 }}>
          <IconCheck />
          <span>{t("dash.quota.healthy")}</span>
        </div>
      )}

      {breached.length > 0 && (
        <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginTop: 10 }}>
          {breached.map(([key, label]) => (
            <span key={key} className="badge" style={{ background: "var(--red-soft)", color: "var(--red)" }}>
              {label}: {quotaWatch.alerts[key].map(item => `${item.provider}/${item.model}`).join(", ")}
            </span>
          ))}
        </div>
      )}

      <div className="stat-row" style={{ marginTop: 14 }}>
        <Stat label={t("dash.quota.rawInput")} value={`${formatTokens(quotaWatch.rawInputTokens, locale)} (${coverage})`} />
        <Stat
          label={t("dash.quota.cache")}
          value={quotaWatch.cacheUtilization === null ? "—" : `${Math.round(quotaWatch.cacheUtilization * 100)}%`}
        />
        <Stat label={t("dash.quota.rate")} value={quotaWatch.requestRatePerMinute.toFixed(1)} />
        <Stat label={t("dash.quota.sends")} value={String(quotaWatch.physicalSends)} />
        <Stat label={t("dash.quota.retries")} value={String(quotaWatch.retrySends)} />
      </div>

      <div className="muted text-control" style={{ marginTop: 10 }}>
        {t("dash.quota.updated", {
          at: quotaWatch.lastActivityAt === null ? "—" : new Date(quotaWatch.lastActivityAt).toLocaleTimeString(locale),
        })}
      </div>
    </section>
  );
}
