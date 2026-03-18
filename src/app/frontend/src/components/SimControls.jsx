import React from 'react';

const TOTAL_STEPS = 30;

const TIMELINE_MARKERS = [
  { step: 0, label: 'Pre-match' },
  { step: 5, label: 'Gates Open' },
  { step: 8, label: 'Kickoff' },
  { step: 10, label: '1st Half' },
  { step: 15, label: 'Halftime' },
  { step: 17, label: '2nd Half' },
  { step: 24, label: 'T+73 GOAL' },
  { step: 26, label: 'Full Time' },
];

export default function SimControls({ mode, simStep, simSpeed, timelineLabel, onStart, onPause, onReset, onLive, onSpeedChange }) {
  const progress = simStep >= 0 ? ((simStep + 1) / TOTAL_STEPS) * 100 : 0;

  return (
    <div className="sim-controls">
      <div className="controls-left">
        {/* Main action buttons */}
        {mode !== 'simulation' ? (
          <button className="btn btn-primary btn-play" onClick={onStart}>
            <span className="btn-icon">&#9654;</span>
            Simulate Live Event
          </button>
        ) : (
          <button className="btn btn-secondary" onClick={onPause}>
            <span className="btn-icon">&#9646;&#9646;</span>
            Pause
          </button>
        )}
        <button className="btn btn-secondary" onClick={onReset}>
          <span className="btn-icon">&#9632;</span>
          Reset
        </button>
        <button
          className={`btn ${mode === 'live' ? 'btn-live-active' : 'btn-live'}`}
          onClick={onLive}
        >
          <span className="live-dot" />
          Live Data
        </button>
      </div>

      {/* Timeline progress */}
      <div className="controls-center">
        <div className="timeline-bar">
          <div className="timeline-fill" style={{ width: `${progress}%` }} />
          {/* Timeline markers */}
          {TIMELINE_MARKERS.map(m => (
            <div
              key={m.step}
              className={`timeline-marker ${simStep >= m.step ? 'active' : ''}`}
              style={{ left: `${(m.step / TOTAL_STEPS) * 100}%` }}
            >
              <div className="marker-tick" />
              <div className="marker-label">{m.label}</div>
            </div>
          ))}
          {/* Current position */}
          {simStep >= 0 && (
            <div
              className="timeline-cursor"
              style={{ left: `${((simStep + 0.5) / TOTAL_STEPS) * 100}%` }}
            />
          )}
        </div>
        {timelineLabel && (
          <div className="timeline-current">{timelineLabel}</div>
        )}
      </div>

      {/* Speed controls */}
      <div className="controls-right">
        <span className="speed-label">Speed:</span>
        {[1, 2, 5].map(s => (
          <button
            key={s}
            className={`btn btn-speed ${simSpeed === s ? 'btn-speed-active' : ''}`}
            onClick={() => onSpeedChange(s)}
          >
            {s}x
          </button>
        ))}
        {simStep >= 0 && (
          <span className="step-counter">
            Step {simStep + 1}/{TOTAL_STEPS}
          </span>
        )}
      </div>
    </div>
  );
}
