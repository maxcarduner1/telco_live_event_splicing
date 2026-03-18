import React from 'react';

function formatNumber(n) {
  if (n == null) return '--';
  if (n >= 1000000) return `$${(n / 1000000).toFixed(1)}M`;
  if (n >= 1000) return `$${(n / 1000).toFixed(0)}k`;
  return n.toLocaleString();
}

const KPI_CONFIGS = [
  { key: 'customers_near', label: 'Customers Near Venue', icon: '👥', format: v => v?.toLocaleString() ?? '--' },
  { key: 'active_towers', label: 'Active Towers', icon: '📡', format: v => v ?? '--' },
  { key: 'towers_congested', label: 'Towers in Congestion', icon: '🔴', format: v => v ?? '--', alert: v => v > 0 },
  { key: 'offers_sent', label: 'Offers Sent', icon: '📱', format: v => v?.toLocaleString() ?? '--' },
  { key: 'converted', label: 'Converted', icon: '🏆', format: v => v?.toLocaleString() ?? '--' },
  { key: 'projected_arr', label: 'Projected ARR', icon: '💰', format: v => v != null ? formatNumber(v) : '--' },
  { key: 'active_slices', label: 'Premium Slices', icon: '⚡', format: v => v ?? '--' },
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
