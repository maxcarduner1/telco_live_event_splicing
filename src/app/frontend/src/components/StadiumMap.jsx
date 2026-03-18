import React, { useMemo, useState } from 'react';

// Coordinate transform: lat/lon -> SVG pixels
// Map bounds around Lumen Field
const MAP_BOUNDS = {
  minLat: 47.5925,
  maxLat: 47.5975,
  minLon: -122.3350,
  maxLon: -122.3285,
};
const SVG_W = 900;
const SVG_H = 640;
const PAD = 40;

function latLonToSvg(lat, lon) {
  const x = PAD + ((lon - MAP_BOUNDS.minLon) / (MAP_BOUNDS.maxLon - MAP_BOUNDS.minLon)) * (SVG_W - 2 * PAD);
  const y = PAD + ((MAP_BOUNDS.maxLat - lat) / (MAP_BOUNDS.maxLat - MAP_BOUNDS.minLat)) * (SVG_H - 2 * PAD);
  return { x, y };
}

const SEGMENT_COLORS = {
  high_value_influencer: '#FFAB00',
  high_value: '#0070C0',
  influencer: '#00A972',
  standard: '#919191',
  premium: '#B0B0B0',
};

function getTowerColor(congestionScore) {
  if (congestionScore > 0.7) return '#FF4444';
  if (congestionScore > 0.4) return '#FF9500';
  return '#00C853';
}

// Stadium center in SVG
const stadiumCenter = latLonToSvg(47.5952, -122.3316);

export default function StadiumMap({ towers, customers }) {
  const [hoveredTower, setHoveredTower] = useState(null);

  // Pre-calculate positions
  const towerPositions = useMemo(() =>
    towers.map(t => ({
      ...t,
      ...latLonToSvg(t.latitude, t.longitude),
    })),
    [towers]
  );

  const customerPositions = useMemo(() =>
    customers.map(c => ({
      ...c,
      ...latLonToSvg(c.latitude, c.longitude),
    })),
    [customers]
  );

  return (
    <div className="stadium-map-container">
      <svg
        viewBox={`0 0 ${SVG_W} ${SVG_H}`}
        className="stadium-svg"
        preserveAspectRatio="xMidYMid meet"
      >
        <defs>
          {/* Glow filter for towers */}
          <filter id="glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="3" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          {/* Pulse animation for offers */}
          <filter id="offerGlow" x="-100%" y="-100%" width="300%" height="300%">
            <feGaussianBlur stdDeviation="5" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          {/* Grid pattern */}
          <pattern id="grid" width="30" height="30" patternUnits="userSpaceOnUse">
            <path d="M 30 0 L 0 0 0 30" fill="none" stroke="#1a3040" strokeWidth="0.5" opacity="0.4" />
          </pattern>
          {/* Radial gradient for stadium */}
          <radialGradient id="stadiumGlow" cx="50%" cy="50%">
            <stop offset="0%" stopColor="#00AEEF" stopOpacity="0.12" />
            <stop offset="100%" stopColor="#00AEEF" stopOpacity="0" />
          </radialGradient>
        </defs>

        {/* Background */}
        <rect width={SVG_W} height={SVG_H} fill="#0E1117" />
        <rect width={SVG_W} height={SVG_H} fill="url(#grid)" />

        {/* Stadium glow */}
        <ellipse
          cx={stadiumCenter.x}
          cy={stadiumCenter.y}
          rx={180}
          ry={130}
          fill="url(#stadiumGlow)"
        />

        {/* Streets */}
        <g stroke="#1E3A4D" strokeWidth="2" opacity="0.6">
          {/* Horizontal streets */}
          <line x1={PAD} y1={SVG_H * 0.25} x2={SVG_W - PAD} y2={SVG_H * 0.25} />
          <line x1={PAD} y1={SVG_H * 0.75} x2={SVG_W - PAD} y2={SVG_H * 0.75} />
          {/* Vertical streets */}
          <line x1={SVG_W * 0.2} y1={PAD} x2={SVG_W * 0.2} y2={SVG_H - PAD} />
          <line x1={SVG_W * 0.8} y1={PAD} x2={SVG_W * 0.8} y2={SVG_H - PAD} />
          {/* Diagonal approach roads */}
          <line x1={PAD} y1={PAD} x2={stadiumCenter.x - 60} y2={stadiumCenter.y - 40} />
          <line x1={SVG_W - PAD} y1={PAD} x2={stadiumCenter.x + 60} y2={stadiumCenter.y - 40} />
          <line x1={PAD} y1={SVG_H - PAD} x2={stadiumCenter.x - 60} y2={stadiumCenter.y + 40} />
          <line x1={SVG_W - PAD} y1={SVG_H - PAD} x2={stadiumCenter.x + 60} y2={stadiumCenter.y + 40} />
        </g>

        {/* Street labels */}
        <text x={SVG_W * 0.5} y={SVG_H * 0.25 - 6} textAnchor="middle" fill="#2a5060" fontSize="9" fontFamily="monospace">S ROYAL BROUGHAM WAY</text>
        <text x={SVG_W * 0.5} y={SVG_H * 0.75 - 6} textAnchor="middle" fill="#2a5060" fontSize="9" fontFamily="monospace">S ATLANTIC ST</text>
        <text x={SVG_W * 0.2 + 6} y={SVG_H * 0.15} fill="#2a5060" fontSize="9" fontFamily="monospace" transform={`rotate(-90 ${SVG_W * 0.2 + 6} ${SVG_H * 0.15})`}>1ST AVE S</text>
        <text x={SVG_W * 0.8 + 6} y={SVG_H * 0.15} fill="#2a5060" fontSize="9" fontFamily="monospace" transform={`rotate(-90 ${SVG_W * 0.8 + 6} ${SVG_H * 0.15})`}>OCCIDENTAL AVE S</text>

        {/* Stadium shape - rounded rectangle with field markings */}
        <rect
          x={stadiumCenter.x - 80}
          y={stadiumCenter.y - 50}
          width={160}
          height={100}
          rx={20}
          ry={20}
          fill="#0a2030"
          stroke="#00AEEF"
          strokeWidth="2"
          opacity="0.8"
        />
        {/* Field */}
        <rect
          x={stadiumCenter.x - 60}
          y={stadiumCenter.y - 35}
          width={120}
          height={70}
          rx={4}
          fill="#0d3520"
          stroke="#1a6040"
          strokeWidth="1"
          opacity="0.7"
        />
        {/* Center line */}
        <line
          x1={stadiumCenter.x}
          y1={stadiumCenter.y - 35}
          x2={stadiumCenter.x}
          y2={stadiumCenter.y + 35}
          stroke="#1a6040"
          strokeWidth="1"
        />
        {/* Center circle */}
        <circle cx={stadiumCenter.x} cy={stadiumCenter.y} r={15} fill="none" stroke="#1a6040" strokeWidth="1" />
        {/* Stadium label */}
        <text
          x={stadiumCenter.x}
          y={stadiumCenter.y + 60}
          textAnchor="middle"
          fill="#00AEEF"
          fontSize="12"
          fontWeight="bold"
          fontFamily="system-ui"
        >
          LUMEN FIELD
        </text>

        {/* Tower coverage rings */}
        {towerPositions.map((t) => (
          <circle
            key={`cov-${t.tower_id}`}
            cx={t.x}
            cy={t.y}
            r={55}
            fill="none"
            stroke={getTowerColor(t.congestion_score)}
            strokeWidth="0.5"
            opacity="0.15"
            strokeDasharray="4 4"
          />
        ))}

        {/* Customer dots */}
        {customerPositions.map((c, i) => (
          <g key={c.customer_id}>
            {/* Offer pulse ring */}
            {c.show_offer && (
              <>
                <circle
                  cx={c.x}
                  cy={c.y}
                  r={8}
                  fill="none"
                  stroke={SEGMENT_COLORS[c.customer_segment] || '#919191'}
                  strokeWidth="1.5"
                  className="pulse-ring"
                />
                <circle
                  cx={c.x}
                  cy={c.y}
                  r={14}
                  fill="none"
                  stroke={SEGMENT_COLORS[c.customer_segment] || '#919191'}
                  strokeWidth="1"
                  className="pulse-ring-outer"
                />
              </>
            )}
            {/* Customer dot */}
            <circle
              cx={c.x}
              cy={c.y}
              r={c.show_offer ? 4 : 2.5}
              fill={SEGMENT_COLORS[c.customer_segment] || '#919191'}
              opacity={c.show_offer ? 1 : 0.7}
              className={c.show_offer ? 'customer-dot-offer' : 'customer-dot'}
            />
            {/* Offer label - only first few to avoid clutter */}
            {c.show_offer && i < 12 && (
              <text
                x={c.x + 10}
                y={c.y - 6}
                fill="#FFAB00"
                fontSize="8"
                fontWeight="bold"
                fontFamily="system-ui"
                className="offer-label"
              >
                Offer Sent!
              </text>
            )}
          </g>
        ))}

        {/* Tower icons */}
        {towerPositions.map((t) => {
          const color = getTowerColor(t.congestion_score);
          const isFlashing = t.congestion_predicted_15min;
          const isHovered = hoveredTower === t.tower_id;
          return (
            <g
              key={t.tower_id}
              onMouseEnter={() => setHoveredTower(t.tower_id)}
              onMouseLeave={() => setHoveredTower(null)}
              style={{ cursor: 'pointer' }}
            >
              {/* Tower range indicator */}
              {isFlashing && (
                <circle
                  cx={t.x}
                  cy={t.y}
                  r={22}
                  fill={color}
                  opacity="0.15"
                  className="tower-flash"
                />
              )}
              {/* Hexagon tower icon */}
              <polygon
                points={hexPoints(t.x, t.y, isHovered ? 14 : 12)}
                fill="#0E1117"
                stroke={color}
                strokeWidth={isFlashing ? 2.5 : 2}
                filter="url(#glow)"
                className={isFlashing ? 'tower-icon-flash' : ''}
              />
              {/* Antenna lines */}
              <line x1={t.x} y1={t.y - 5} x2={t.x} y2={t.y - 10} stroke={color} strokeWidth="1.5" />
              <line x1={t.x - 3} y1={t.y - 8} x2={t.x + 3} y2={t.y - 8} stroke={color} strokeWidth="1" />
              {/* Tower ID label */}
              <text
                x={t.x}
                y={t.y + 22}
                textAnchor="middle"
                fill={color}
                fontSize="8"
                fontFamily="monospace"
                fontWeight="bold"
              >
                {t.tower_id.replace('SEA-LF-', 'LF-')}
              </text>
              {/* Congestion prediction label */}
              {isFlashing && (
                <text
                  x={t.x}
                  y={t.y + 32}
                  textAnchor="middle"
                  fill="#FF4444"
                  fontSize="7"
                  fontWeight="bold"
                  fontFamily="system-ui"
                  className="slice-label"
                >
                  Slice Provisioned
                </text>
              )}
              {/* Hover tooltip */}
              {isHovered && (
                <g>
                  <rect
                    x={t.x + 18}
                    y={t.y - 50}
                    width={150}
                    height={72}
                    rx={6}
                    fill="#1B2838"
                    stroke="#2a5060"
                    strokeWidth="1"
                    opacity="0.95"
                  />
                  <text x={t.x + 26} y={t.y - 34} fill="#00AEEF" fontSize="10" fontWeight="bold" fontFamily="system-ui">
                    {t.tower_id}
                  </text>
                  <text x={t.x + 26} y={t.y - 20} fill="#8899aa" fontSize="9" fontFamily="monospace">
                    BW: {t.bandwidth_utilization_pct?.toFixed(0)}% | Conn: {t.active_connections}
                  </text>
                  <text x={t.x + 26} y={t.y - 8} fill="#8899aa" fontSize="9" fontFamily="monospace">
                    Latency: {t.latency_ms?.toFixed(1)}ms
                  </text>
                  <text x={t.x + 26} y={t.y + 4} fill={color} fontSize="9" fontFamily="monospace" fontWeight="bold">
                    Congestion: {t.congestion_score?.toFixed(2)}
                  </text>
                  <text x={t.x + 26} y={t.y + 16} fill={isFlashing ? '#FF4444' : '#00C853'} fontSize="9" fontFamily="monospace">
                    {isFlashing ? 'PREDICTED CONGESTION' : 'Normal'}
                  </text>
                </g>
              )}
            </g>
          );
        })}

        {/* Legend */}
        <g transform={`translate(${SVG_W - 175}, ${SVG_H - 100})`}>
          <rect x={-8} y={-14} width={170} height={95} rx={6} fill="#0E1117" stroke="#1E3A4D" strokeWidth="1" opacity="0.9" />
          <text x={0} y={0} fill="#5a7a8a" fontSize="9" fontWeight="bold" fontFamily="system-ui">CUSTOMER SEGMENTS</text>
          {[
            ['high_value_influencer', 'High Value Influencer'],
            ['high_value', 'High Value'],
            ['influencer', 'Influencer'],
            ['standard', 'Standard / Premium'],
          ].map(([seg, label], i) => (
            <g key={seg} transform={`translate(0, ${14 + i * 16})`}>
              <circle cx={6} cy={0} r={4} fill={SEGMENT_COLORS[seg]} />
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
