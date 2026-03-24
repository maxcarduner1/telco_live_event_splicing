import React from 'react';

function formatCurrency(n) {
  if (n == null) return '--';
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `$${(n / 1_000).toFixed(0)}k`;
  return `$${n.toLocaleString()}`;
}

const KPI_CONFIGS = [
  { key: 'accounts_at_risk',   label: 'B2B Accounts at Risk', icon: '🏢', format: v => v ?? '--', alert: v => v > 0 },
  { key: 'active_towers',      label: 'Active Towers',        icon: '📡', format: v => v ?? '--' },
  { key: 'towers_congested',   label: 'Towers Congested',     icon: '🔴', format: v => v ?? '--', alert: v => v > 0 },
  { key: 'proposals_sent',     label: 'Upsell Proposals',     icon: '📋', format: v => v?.toLocaleString() ?? '--' },
  { key: 'proposals_accepted', label: 'Accepted',             icon: '✅', format: v => v?.toLocaleString() ?? '--' },
  { key: 'upsell_arr',         label: 'Upsell ARR',           icon: '💰', format: v => v != null ? formatCurrency(v) : '--' },
  { key: 'arr_protected',      label: 'ARR Protected',        icon: '🛡️', format: v => v != null ? formatCurrency(v) : '--' },
  { key: 'burst_slices',       label: 'Burst Slices Active',  icon: '⚡', format: v => v ?? '--' },
];

export default function KPIBar({ kpis }) {
  return (
    <div className="kpi-bar">
      {KPI_CONFIGS.map(({ key, label, icon, format, alert }) => {
        const val = kpis?.[key];
        const isAlert = alert && alert(val);
        return (
          <div key={key} className={`kpi-card ${isAlert ? 'kpi-alert' : ''}`}>
            <div className="kpi-icon">{icon}</div>
            <div className="kpi-value">{format(val)}</div>
            <div className="kpi-label">{label}</div>
          </div>
        );
      })}
    </div>
  );
}
