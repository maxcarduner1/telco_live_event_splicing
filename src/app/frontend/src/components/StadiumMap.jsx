import React, { useMemo, useState } from 'react';

// Coordinate transform: lat/lon -> SVG pixels
const MAP_BOUNDS = {
  minLat: 47.5925, maxLat: 47.5975,
  minLon: -122.3350, maxLon: -122.3285,
};
const SVG_W = 900;
const SVG_H = 640;
const PAD = 40;

function latLonToSvg(lat, lon) {
  const x = PAD + ((lon - MAP_BOUNDS.minLon) / (MAP_BOUNDS.maxLon - MAP_BOUNDS.minLon)) * (SVG_W - 2 * PAD);
  const y = PAD + ((MAP_BOUNDS.maxLat - lat) / (MAP_BOUNDS.maxLat - MAP_BOUNDS.minLat)) * (SVG_H - 2 * PAD);
  return { x, y };
}

// B2B customer type colors
const CUSTOMER_TYPE_COLORS = {
  broadcaster:       '#FFAB00',   // gold — media/broadcast
  venue_operator:    '#00AEEF',   // blue — venue ops
  public_safety:     '#FF4444',   // red — emergency/security
  payment_processor: '#00C853',   // green — payments
  team_sponsor:      '#B45CFF',   // purple — teams/sponsors
};
// Segment alias fallback
const SEGMENT_COLORS = {
  media_rights:       '#FFAB00',
  venue_ops:          '#00AEEF',
  critical_services:  '#FF4444',
  payments_ticketing: '#00C853',
  teams_sponsors:     '#B45CFF',
};

function getCustomerColor(c) {
  return CUSTOMER_TYPE_COLORS[c.customer_type]
      || SEGMENT_COLORS[c.customer_segment]
      || '#919191';
}

function getTowerColor(congestionScore) {
  if (congestionScore > 0.7) return '#FF4444';
  if (congestionScore > 0.4) return '#FF9500';
  return '#00C853';
}

const stadiumCenter = latLonToSvg(47.5952, -122.3316);

export default function StadiumMap({ towers, customers }) {
  const [hoveredTower, setHoveredTower] = useState(null);
  const [hoveredCustomer, setHoveredCustomer] = useState(null);

  const towerPositions = useMemo(() =>
    towers.map(t => ({ ...t, ...latLonToSvg(t.latitude, t.longitude) })),
    [towers]
  );

  const customerPositions = useMemo(() =>
    customers.map(c => ({ ...c, ...latLonToSvg(c.latitude, c.longitude) })),
    [customers]
  );

  return (
    <div className="stadium-map-container">
      <svg viewBox={`0 0 ${SVG_W} ${SVG_H}`} className="stadium-svg" preserveAspectRatio="xMidYMid meet">
        <defs>
          <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="3" result="blur" />
            <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
          </filter>
          <filter id="offerGlow" x="-100%" y="-100%" width="300%" height="300%">
            <feGaussianBlur stdDeviation="5" result="blur" />
            <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
          </filter>
          <pattern id="grid" width="30" height="30" patternUnits="userSpaceOnUse">
            <path d="M 30 0 L 0 0 0 30" fill="none" stroke="#1a3040" strokeWidth="0.5" opacity="0.4" />
          </pattern>
          <radialGradient id="stadiumGlow" cx="50%" cy="50%">
            <stop offset="0%" stopColor="#00AEEF" stopOpacity="0.12" />
            <stop offset="100%" stopColor="#00AEEF" stopOpacity="0" />
          </radialGradient>
        </defs>

        {/* Background */}
        <rect width={SVG_W} height={SVG_H} fill="#0E1117" />
        <rect width={SVG_W} height={SVG_H} fill="url(#grid)" />
        <ellipse cx={stadiumCenter.x} cy={stadiumCenter.y} rx={180} ry={130} fill="url(#stadiumGlow)" />

        {/* Streets */}
        <g stroke="#1E3A4D" strokeWidth="2" opacity="0.6">
          <line x1={PAD} y1={SVG_H * 0.25} x2={SVG_W - PAD} y2={SVG_H * 0.25} />
          <line x1={PAD} y1={SVG_H * 0.75} x2={SVG_W - PAD} y2={SVG_H * 0.75} />
          <line x1={SVG_W * 0.2} y1={PAD} x2={SVG_W * 0.2} y2={SVG_H - PAD} />
          <line x1={SVG_W * 0.8} y1={PAD} x2={SVG_W * 0.8} y2={SVG_H - PAD} />
          <line x1={PAD} y1={PAD} x2={stadiumCenter.x - 60} y2={stadiumCenter.y - 40} />
          <line x1={SVG_W - PAD} y1={PAD} x2={stadiumCenter.x + 60} y2={stadiumCenter.y - 40} />
          <line x1={PAD} y1={SVG_H - PAD} x2={stadiumCenter.x - 60} y2={stadiumCenter.y + 40} />
          <line x1={SVG_W - PAD} y1={SVG_H - PAD} x2={stadiumCenter.x + 60} y2={stadiumCenter.y + 40} />
        </g>
        <text x={SVG_W * 0.5} y={SVG_H * 0.25 - 6} textAnchor="middle" fill="#2a5060" fontSize="9" fontFamily="monospace">S ROYAL BROUGHAM WAY</text>
        <text x={SVG_W * 0.5} y={SVG_H * 0.75 - 6} textAnchor="middle" fill="#2a5060" fontSize="9" fontFamily="monospace">S ATLANTIC ST</text>
        <text x={SVG_W * 0.2 + 6} y={SVG_H * 0.15} fill="#2a5060" fontSize="9" fontFamily="monospace" transform={`rotate(-90 ${SVG_W * 0.2 + 6} ${SVG_H * 0.15})`}>1ST AVE S</text>
        <text x={SVG_W * 0.8 + 6} y={SVG_H * 0.15} fill="#2a5060" fontSize="9" fontFamily="monospace" transform={`rotate(-90 ${SVG_W * 0.8 + 6} ${SVG_H * 0.15})`}>OCCIDENTAL AVE S</text>

        {/* Stadium */}
        <rect x={stadiumCenter.x - 80} y={stadiumCenter.y - 50} width={160} height={100} rx={20} ry={20} fill="#0a2030" stroke="#00AEEF" strokeWidth="2" opacity="0.8" />
        <rect x={stadiumCenter.x - 60} y={stadiumCenter.y - 35} width={120} height={70} rx={4} fill="#0d3520" stroke="#1a6040" strokeWidth="1" opacity="0.7" />
        <line x1={stadiumCenter.x} y1={stadiumCenter.y - 35} x2={stadiumCenter.x} y2={stadiumCenter.y + 35} stroke="#1a6040" strokeWidth="1" />
        <circle cx={stadiumCenter.x} cy={stadiumCenter.y} r={15} fill="none" stroke="#1a6040" strokeWidth="1" />
        <text x={stadiumCenter.x} y={stadiumCenter.y + 60} textAnchor="middle" fill="#00AEEF" fontSize="12" fontWeight="bold" fontFamily="system-ui">LUMEN FIELD</text>

        {/* Tower coverage rings */}
        {towerPositions.map(t => (
          <circle key={`cov-${t.tower_id}`} cx={t.x} cy={t.y} r={55}
            fill="none" stroke={getTowerColor(t.congestion_score)}
            strokeWidth="0.5" opacity="0.15" strokeDasharray="4 4" />
        ))}

        {/* B2B customer dots */}
        {customerPositions.map((c, i) => {
          const color = getCustomerColor(c);
          const isHovered = hoveredCustomer === c.customer_id;
          const dotR = c.show_proposal ? 6 : c.breach_risk_level === 'critical' ? 5 : 3.5;
          return (
            <g key={c.customer_id}
               onMouseEnter={() => setHoveredCustomer(c.customer_id)}
               onMouseLeave={() => setHoveredCustomer(null)}
               style={{ cursor: 'pointer' }}>
              {/* Proposal pulse ring */}
              {c.show_proposal && (
                <>
                  <circle cx={c.x} cy={c.y} r={10} fill="none" stroke={color} strokeWidth="1.5" className="pulse-ring" />
                  <circle cx={c.x} cy={c.y} r={17} fill="none" stroke={color} strokeWidth="1" className="pulse-ring-outer" />
                </>
              )}
              {/* Customer dot */}
              <circle cx={c.x} cy={c.y} r={dotR} fill={color}
                opacity={c.show_proposal ? 1 : 0.75}
                className={c.show_proposal ? 'customer-dot-offer' : 'customer-dot'} />
              {/* Proposal label — first few high-score accounts */}
              {c.show_proposal && i < 8 && (
                <text x={c.x + 10} y={c.y - 8} fill="#FFAB00" fontSize="8" fontWeight="bold" fontFamily="system-ui" className="offer-label">
                  Upsell Sent!
                </text>
              )}
              {/* Hover tooltip */}
              {isHovered && (
                <g>
                  <rect x={c.x + 12} y={c.y - 58} width={170} height={88} rx={6} fill="#1B2838" stroke="#2a5060" strokeWidth="1" opacity="0.97" />
                  <text x={c.x + 20} y={c.y - 42} fill={color} fontSize="10" fontWeight="bold" fontFamily="system-ui">{c.company_name}</text>
                  <text x={c.x + 20} y={c.y - 28} fill="#8899aa" fontSize="9" fontFamily="monospace">{c.customer_type?.replace('_', ' ')}</text>
                  <text x={c.x + 20} y={c.y - 16} fill="#8899aa" fontSize="9" fontFamily="monospace">
                    BW: {c.current_bandwidth_mbps?.toFixed(0)}/{c.contracted_bandwidth_mbps?.toFixed(0)} Mbps
                  </text>
                  <text x={c.x + 20} y={c.y - 4} fill={c.utilization_pct >= 90 ? '#FF4444' : c.utilization_pct >= 85 ? '#FF9500' : '#00C853'} fontSize="9" fontFamily="monospace" fontWeight="bold">
                    Util: {c.utilization_pct?.toFixed(1)}% [{c.breach_risk_level?.toUpperCase()}]
                  </text>
                  <text x={c.x + 20} y={c.y + 8} fill="#FFAB00" fontSize="9" fontFamily="monospace">
                    +${c.monthly_revenue_opportunity?.toFixed(0)}/mo upsell opp
                  </text>
                </g>
              )}
            </g>
          );
        })}

        {/* Tower icons */}
        {towerPositions.map(t => {
          const color = getTowerColor(t.congestion_score);
          const isFlashing = t.congestion_predicted_15min;
          const isHovered = hoveredTower === t.tower_id;
          return (
            <g key={t.tower_id}
               onMouseEnter={() => setHoveredTower(t.tower_id)}
               onMouseLeave={() => setHoveredTower(null)}
               style={{ cursor: 'pointer' }}>
              {isFlashing && <circle cx={t.x} cy={t.y} r={22} fill={color} opacity="0.15" className="tower-flash" />}
              <polygon points={hexPoints(t.x, t.y, isHovered ? 14 : 12)} fill="#0E1117" stroke={color}
                strokeWidth={isFlashing ? 2.5 : 2} filter="url(#glow)" className={isFlashing ? 'tower-icon-flash' : ''} />
              <line x1={t.x} y1={t.y - 5} x2={t.x} y2={t.y - 10} stroke={color} strokeWidth="1.5" />
              <line x1={t.x - 3} y1={t.y - 8} x2={t.x + 3} y2={t.y - 8} stroke={color} strokeWidth="1" />
              <text x={t.x} y={t.y + 22} textAnchor="middle" fill={color} fontSize="8" fontFamily="monospace" fontWeight="bold">
                {t.tower_id.replace('SEA-LF-', 'LF-')}
              </text>
              {isFlashing && (
                <text x={t.x} y={t.y + 32} textAnchor="middle" fill="#FF4444" fontSize="7" fontWeight="bold" fontFamily="system-ui" className="slice-label">
                  Burst Provisioned
                </text>
              )}
              {isHovered && (
                <g>
                  <rect x={t.x + 18} y={t.y - 50} width={155} height={72} rx={6} fill="#1B2838" stroke="#2a5060" strokeWidth="1" opacity="0.95" />
                  <text x={t.x + 26} y={t.y - 34} fill="#00AEEF" fontSize="10" fontWeight="bold" fontFamily="system-ui">{t.tower_id}</text>
                  <text x={t.x + 26} y={t.y - 20} fill="#8899aa" fontSize="9" fontFamily="monospace">BW: {t.bandwidth_utilization_pct?.toFixed(0)}% | Conn: {t.active_connections}</text>
                  <text x={t.x + 26} y={t.y - 8} fill="#8899aa" fontSize="9" fontFamily="monospace">Latency: {t.latency_ms?.toFixed(1)}ms</text>
                  <text x={t.x + 26} y={t.y + 4} fill={color} fontSize="9" fontFamily="monospace" fontWeight="bold">Congestion: {t.congestion_score?.toFixed(2)}</text>
                  <text x={t.x + 26} y={t.y + 16} fill={isFlashing ? '#FF4444' : '#00C853'} fontSize="9" fontFamily="monospace">
                    {isFlashing ? 'CONGESTION PREDICTED' : 'Normal'}
                  </text>
                </g>
              )}
            </g>
          );
        })}

        {/* Legend */}
        <g transform={`translate(${SVG_W - 185}, ${SVG_H - 115})`}>
          <rect x={-8} y={-14} width={180} height={110} rx={6} fill="#0E1117" stroke="#1E3A4D" strokeWidth="1" opacity="0.9" />
          <text x={0} y={0} fill="#5a7a8a" fontSize="9" fontWeight="bold" fontFamily="system-ui">BUSINESS CUSTOMER TYPES</text>
          {[
            ['broadcaster',       'Broadcaster / Media'],
            ['venue_operator',    'Venue Operator'],
            ['public_safety',     'Security / Public Safety'],
            ['payment_processor', 'Payments / Ticketing'],
            ['team_sponsor',      'Teams / Sponsors'],
          ].map(([type, label], i) => (
            <g key={type} transform={`translate(0, ${14 + i * 16})`}>
              <circle cx={6} cy={0} r={4} fill={CUSTOMER_TYPE_COLORS[type]} />
              <text x={16} y={3} fill="#8899aa" fontSize="9" fontFamily="system-ui">{label}</text>
            </g>
          ))}
        </g>
      </svg>
    </div>
  );
}

function hexPoints(cx, cy, r) {
  const pts = [];
  for (let i = 0; i < 6; i++) {
    const angle = (Math.PI / 3) * i - Math.PI / 6;
    pts.push(`${cx + r * Math.cos(angle)},${cy + r * Math.sin(angle)}`);
  }
  return pts.join(' ');
}
