import MemoryObservabilityCard from "../components/MemoryObservabilityCard";
import { DashboardQuotaObservability } from "./dashboard-quota-observability";
import type { useDashboardData } from "./use-dashboard-data";
import {
  DashboardEffortCapPanel,
  DashboardInjectionPanel,
  DashboardMaintenancePanel,
  DashboardSidecarPanels,
} from "./dashboard-overview-sections";

type Dash = ReturnType<typeof useDashboardData>;

export function DashboardOverviewPanels(props: Dash) {
  return (
    <>
      <DashboardQuotaObservability
        data={props.quotaAnalytics}
        loading={props.quotaAnalyticsLoading}
        error={props.quotaAnalyticsError}
        locale={props.locale}
        t={props.t}
      />
      <DashboardEffortCapPanel apiBase={props.apiBase} d={props} />
      <div className="dash-overview-tools">
        <DashboardInjectionPanel apiBase={props.apiBase} d={props} />
        <DashboardMaintenancePanel d={props} />
      </div>
      <DashboardSidecarPanels d={props} />
      <MemoryObservabilityCard apiBase={props.apiBase} />
    </>
  );
}
