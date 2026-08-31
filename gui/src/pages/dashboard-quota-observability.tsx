import { formatTokens } from "../format-tokens";
import { formatProviderDisplayName } from "../provider-icons";
import type { Locale, TFn } from "../i18n/shared";
import { modelLabel } from "../model-display";
import { Notice } from "../ui";
import type { RoutingAnalyticsResult } from "./dashboard-shared";

type Alert = RoutingAnalyticsResult["redAlerts"][number];

function percentage(value: number | null): string {
  return value === null ? "—" : `${Math.round(value * 100)}%`;
}

function rate(value: number | null, locale: Locale): string {
  return value === null ? "—" : new Intl.NumberFormat(locale, { maximumFractionDigits: 1 }).format(value);
}

function renderableInstant(value: number, locale: Locale): string | null {
  const at = new Date(value);
  return Number.isFinite(at.getTime()) ? at.toLocaleString(locale) : null;
}

function alertLabel(t: TFn, kind: Alert["kind"]): string {
  return t(`usage.quota.alert.${kind}`);
}

function alertValue(alert: Alert, locale: Locale): string {
  if (alert.value === undefined) return "—";
  if (alert.kind === "low-cache-read-share-after-warmup") return percentage(alert.value);
  if (alert.kind === "consecutive-high-raw-input"
    || alert.kind === "high-cache-write-after-warmup"
    || alert.kind === "falling-cache-read") {
    return formatTokens(alert.value, locale);
  }
  return new Intl.NumberFormat(locale).format(alert.value);
}

function routeLabel(provider: string, model: string, accountRef: string | undefined, t: TFn): string {
  const route = `${formatProviderDisplayName(provider, t)} / ${modelLabel(model)}`;
  return accountRef ? `${route} / ${accountRef}` : route;
}

function alertIdentity(alert: Alert): string {
  return alert.conversationId ?? alert.requestId;
}

function QuotaStaleNotice({
  data,
  error,
  locale,
  t,
}: {
  data: RoutingAnalyticsResult | null;
  error: boolean;
  locale: Locale;
  t: TFn;
}) {
  if (!data || !error) return null;
  const generatedAt = data.generatedAt;
  const time = typeof generatedAt === "number" ? renderableInstant(generatedAt, locale) : null;
  return (
    <Notice tone="warn">
      {time ? t("usage.quota.staleAt", { time }) : t("integrations.error.stale")}
    </Notice>
  );
}

function QuotaState({
  data,
  loading,
  error,
  t,
}: {
  data: RoutingAnalyticsResult | null;
  loading: boolean;
  error: boolean;
  t: TFn;
}) {
  if (!data) {
    return <Notice tone="warn">{loading ? t("usage.quota.loading") : error ? t("usage.quota.loadError") : t("usage.quota.status.noData")}</Notice>;
  }
  if (error) return null;
  if (data.totalRequests === 0) return <Notice tone="warn">{t("usage.quota.status.noData")}</Notice>;
  if (data.redAlerts.length > 0) {
    return <Notice tone="err">{t("usage.quota.status.alerts", { count: data.redAlerts.length })}</Notice>;
  }

  const coverage = data.physicalUsageCoverage;
  if (coverage.totalAttempts === 0) {
    return <Notice tone="warn">{t("usage.quota.status.noAttempts")}</Notice>;
  }
  if (coverage.measuredAttempts === 0 && coverage.unsupportedAttempts > 0 && coverage.unreportedAttempts === 0) {
    return <Notice tone="warn">{t("usage.quota.status.unsupportedTelemetry", { count: coverage.unsupportedAttempts })}</Notice>;
  }
  if (coverage.measuredAttempts === 0 && coverage.unreportedAttempts > 0 && coverage.unsupportedAttempts === 0) {
    return <Notice tone="warn">{t("usage.quota.status.unreportedTelemetry", { count: coverage.unreportedAttempts })}</Notice>;
  }
  if (coverage.measuredAttempts < coverage.totalAttempts) {
    return <Notice tone="warn">{t("usage.quota.status.partialTelemetry", {
      measured: coverage.measuredAttempts,
      total: coverage.totalAttempts,
      unsupported: coverage.unsupportedAttempts,
      unreported: coverage.unreportedAttempts,
    })}</Notice>;
  }
  if (data.redAlertsPartial) return <Notice tone="warn">{t("usage.quota.partial")}</Notice>;
  if (data.warmedRoutes < data.sequentialRoutes) return <Notice tone="warn">{t("usage.quota.status.warmingUp")}</Notice>;
  return <Notice tone="ok">{t("usage.quota.status.clear")}</Notice>;
}

function QuotaCards({
  data,
  locale,
  t,
}: {
  data: RoutingAnalyticsResult;
  locale: Locale;
  t: TFn;
}) {
  const usage = data.attemptUsage;
  const cards = [
    [t("usage.quota.card.inclusiveInput"), formatTokens(usage.inclusiveInputTokens, locale)],
    [t("usage.quota.card.rawInput"), `${formatTokens(usage.rawInputTokens, locale)} · ${percentage(usage.rawInputShare)}`],
    [t("usage.quota.card.cacheReads"), `${formatTokens(usage.cacheReadInputTokens, locale)} · ${percentage(usage.cacheReadShare)}`],
    [t("usage.quota.card.cacheWrites"), `${formatTokens(usage.cacheWriteInputTokens, locale)} · ${percentage(usage.cacheWriteShare)}`],
    [t("usage.quota.card.physicalSends"), new Intl.NumberFormat(locale).format(data.physicalSends)],
    [t("usage.quota.alert.repeated-send"), new Intl.NumberFormat(locale).format(data.repeatedSendAttempts)],
    [t("usage.quota.card.requestRate"), rate(data.requestRatePerHour, locale)],
    [t("usage.quota.card.coverage"), percentage(data.physicalUsageCoverage.ratio)],
    [t("usage.quota.card.recoveries"), new Intl.NumberFormat(locale).format(data.recoveryEvents)],
  ];
  return (
    <div className="usage-cards usage-cards-3x2" role="group" aria-label={t("usage.quota.title")}>
      {cards.map(([label, value]) => (
        <div className="stat" key={label}>
          <div className="muted">{label}</div>
          <div className="stat-value">{value}</div>
        </div>
      ))}
    </div>
  );
}

export function DashboardQuotaObservability({
  data,
  loading,
  error,
  locale,
  t,
}: {
  data: RoutingAnalyticsResult | null;
  loading: boolean;
  error: boolean;
  locale: Locale;
  t: TFn;
}) {
  return (
    <section className="panel" aria-labelledby="dashboard-quota-title">
      <div className="panel-head">
        <div>
          <h3 id="dashboard-quota-title" className="panel-title">{t("usage.quota.title")}</h3>
          <p className="muted text-control">{t("usage.quota.subtitle")}</p>
        </div>
      </div>
      <QuotaStaleNotice data={data} error={error} locale={locale} t={t} />
      <QuotaState data={data} loading={loading} error={error} t={t} />
      {data && <QuotaCards data={data} locale={locale} t={t} />}
      {data && data.redAlerts.length > 0 && (
        <div className="tbl-wrap">
          <table className="tbl" aria-label={t("usage.quota.alerts")}>
            <thead>
              <tr>
                <th>{t("usage.quota.col.alert")}</th>
                <th>{t("usage.quota.col.route")}</th>
                <th>{t("usage.quota.col.session")}</th>
              </tr>
            </thead>
            <tbody>
              {data.redAlerts.slice(0, 3).map((alert, index) => {
                const identity = alertIdentity(alert);
                return (
                  <tr key={`${alert.requestId}/${alert.attemptOrdinal}/${alert.kind}/${index}`}>
                    <td>{alertLabel(t, alert.kind)}</td>
                    <td className="mono">{routeLabel(alert.provider, alert.model, alert.accountRef, t)}</td>
                    <td className="mono">{identity}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      {data && data.redAlerts.length > 3 && (
        <p className="muted text-caption">{t("usage.quota.alertsPreview", { shown: 3, total: data.redAlerts.length })}</p>
      )}
      {data?.redAlertsPartial && data.redAlerts.length > 0 && (
        <p className="muted text-caption">{t("usage.quota.partial")}</p>
      )}
    </section>
  );
}

export function UsageQuotaObservability({
  data,
  loading,
  error,
  locale,
  t,
}: {
  data: RoutingAnalyticsResult | null;
  loading: boolean;
  error: boolean;
  locale: Locale;
  t: TFn;
}) {
  return (
    <section className="usw-section" aria-labelledby="usage-quota-title">
      <h3 id="usage-quota-title" className="h-section">{t("usage.quota.title")}</h3>
      <p className="muted text-control">{t("usage.quota.subtitle")}</p>
      <QuotaStaleNotice data={data} error={error} locale={locale} t={t} />
      <QuotaState data={data} loading={loading} error={error} t={t} />
      {data && (
        <>
          <QuotaCards data={data} locale={locale} t={t} />
          <h4 className="panel-title">{t("usage.quota.routes")}</h4>
          <div className="tbl-wrap">
            <table className="tbl" aria-label={t("usage.quota.routes")}>
              <thead>
                <tr>
                  <th>{t("usage.quota.col.route")}</th>
                  <th className="num">{t("usage.quota.col.requests")}</th>
                  <th className="num">{t("usage.quota.col.attempts")}</th>
                  <th className="num">{t("usage.quota.col.sends")}</th>
                  <th className="num">{t("usage.quota.alert.repeated-send")}</th>
                  <th className="num">{t("usage.quota.col.raw")}</th>
                  <th className="num">{t("usage.quota.col.read")}</th>
                  <th className="num">{t("usage.quota.col.write")}</th>
                  <th className="num">{t("usage.quota.col.recoveries")}</th>
                  <th className="num">{t("usage.quota.col.failovers")}</th>
                  <th className="num">{t("usage.quota.col.rate")}</th>
                  <th className="num">{t("usage.quota.col.coverage")}</th>
                </tr>
              </thead>
              <tbody>
                {data.physicalBreakdown.map(row => (
                  <tr key={`${row.provider}/${row.model}/${row.accountRef ?? ""}`}>
                    <td className="mono">{routeLabel(row.provider, row.model, row.accountRef, t)}</td>
                    <td className="num">{row.requests}</td>
                    <td className="num">{row.physicalAttempts}</td>
                    <td className="num">{row.physicalSends}</td>
                    <td className="num">{row.repeatedSendAttempts}</td>
                    <td className="num mono">{formatTokens(row.attemptUsage.rawInputTokens, locale)} · {percentage(row.attemptUsage.rawInputShare)}</td>
                    <td className="num mono">{formatTokens(row.attemptUsage.cacheReadInputTokens, locale)} · {percentage(row.attemptUsage.cacheReadShare)}</td>
                    <td className="num mono">{formatTokens(row.attemptUsage.cacheWriteInputTokens, locale)} · {percentage(row.attemptUsage.cacheWriteShare)}</td>
                    <td className="num">{row.recoveryEvents}</td>
                    <td className="num">{row.comboFailoverRequests}</td>
                    <td className="num">{rate(row.requestRatePerHour, locale)}</td>
                    <td className="num">{percentage(row.usageCoverage.ratio)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <h4 className="panel-title">{t("usage.quota.alerts")}</h4>
          {data.redAlerts.length === 0 ? (
            <p className="muted text-control">{t("usage.quota.alertsEmpty")}</p>
          ) : (
            <div className="tbl-wrap">
              <table className="tbl" aria-label={t("usage.quota.alerts")}>
                <thead>
                  <tr>
                    <th>{t("usage.quota.col.alert")}</th>
                    <th>{t("usage.quota.col.route")}</th>
                    <th>{t("usage.quota.col.session")}</th>
                    <th>{t("usage.quota.col.time")}</th>
                    <th className="num">{t("usage.quota.col.value")}</th>
                  </tr>
                </thead>
                <tbody>
                  {data.redAlerts.map((alert, index) => {
                    const identity = alertIdentity(alert);
                    return (
                      <tr key={`${alert.requestId}/${alert.attemptOrdinal}/${alert.kind}/${index}`}>
                        <td>{alertLabel(t, alert.kind)}</td>
                        <td className="mono">{routeLabel(alert.provider, alert.model, alert.accountRef, t)}</td>
                        <td className="mono">{identity}</td>
                        <td>{renderableInstant(alert.timestamp, locale) ?? "—"}</td>
                        <td className="num mono">{alertValue(alert, locale)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
          {data.redAlertsPartial && data.redAlerts.length > 0 && (
            <p className="muted text-caption">{t("usage.quota.partial")}</p>
          )}
        </>
      )}
    </section>
  );
}
