import React, { useEffect, useRef } from 'react';

const ICONS = {
  offer: { emoji: '📱', color: '#00AEEF', label: 'OFFER' },
  congestion: { emoji: '⚡', color: '#FF4444', label: 'ALERT' },
  trophy: { emoji: '🏆', color: '#FFAB00', label: 'CONVERTED' },
};

export default function LiveFeed({ feed }) {
  const scrollRef = useRef(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = 0;
    }
  }, [feed]);

  return (
    <div className="live-feed">
      <div className="feed-header">
        <span className="feed-title">Live Event Feed</span>
        <span className="feed-count">{feed?.length || 0} events</span>
      </div>
      <div className="feed-scroll" ref={scrollRef}>
        {(!feed || feed.length === 0) ? (
          <div className="feed-empty">
            <div className="feed-empty-icon">📡</div>
            <div>Waiting for events...</div>
            <div className="feed-empty-hint">Start the simulation to see live data</div>
          </div>
        ) : (
          feed.map((item, i) => {
            const config = ICONS[item.type] || ICONS.offer;
            return (
              <div key={i} className={`feed-item feed-item-${item.type}`} style={{ animationDelay: `${i * 0.05}s` }}>
                <div className="feed-item-icon" style={{ background: `${config.color}15` }}>
                  {config.emoji}
                </div>
                <div className="feed-item-content">
                  <div className="feed-item-badge" style={{ color: config.color }}>
                    {config.label}
                  </div>
                  <div className="feed-item-message">{item.message}</div>
                  {item.timestamp && (
                    <div className="feed-item-time">
                      {formatTimestamp(item.timestamp)}
                    </div>
                  )}
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}

function formatTimestamp(ts) {
  if (!ts) return '';
  try {
    const d = new Date(ts);
    if (isNaN(d.getTime())) return ts;
    return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  } catch {
    return ts;
  }
}
