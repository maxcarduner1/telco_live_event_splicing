import React, { useState, useEffect, useCallback, useRef } from 'react';
import KPIBar from './components/KPIBar.jsx';
import StadiumMap from './components/StadiumMap.jsx';
import LiveFeed from './components/LiveFeed.jsx';
import SimControls from './components/SimControls.jsx';

const POLL_INTERVAL = 3000;

export default function App() {
  const [mode, setMode] = useState('idle'); // 'idle' | 'live' | 'simulation'
  const [simStep, setSimStep] = useState(-1);
  const [simSpeed, setSimSpeed] = useState(1);
  const [simData, setSimData] = useState(null);
  const [kpis, setKpis] = useState(null);
  const [towers, setTowers] = useState([]);
  const [customers, setCustomers] = useState([]);
  const [feed, setFeed] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const simInterval = useRef(null);

  // Fetch live data
  const fetchLiveData = useCallback(async () => {
    try {
      const [kpiRes, towerRes, custRes, feedRes] = await Promise.all([
        fetch('/api/kpis').then(r => r.json()),
        fetch('/api/towers').then(r => r.json()),
        fetch('/api/customers').then(r => r.json()),
        fetch('/api/feed').then(r => r.json()),
      ]);
      if (!kpiRes.error) setKpis(kpiRes);
      if (towerRes.towers) setTowers(towerRes.towers);
      if (custRes.customers) setCustomers(custRes.customers);
      if (feedRes.feed) setFeed(feedRes.feed);
      setError(null);
    } catch (e) {
      setError('Connection error - retrying...');
    }
  }, []);

  // Fetch simulation step
  const fetchSimStep = useCallback(async (step) => {
    try {
      const res = await fetch(`/api/simulation/step/${step}`);
      const data = await res.json();
      setSimData(data);
      setTowers(data.towers || []);
      setCustomers(data.customers || []);
      setKpis(data.kpis || null);
      setFeed(data.feed || []);
      setError(null);
    } catch (e) {
      setError('Simulation fetch error');
    }
  }, []);

  // Live polling
  useEffect(() => {
    if (mode === 'live') {
      fetchLiveData();
      const id = setInterval(fetchLiveData, POLL_INTERVAL);
      return () => clearInterval(id);
    }
  }, [mode, fetchLiveData]);

  // Simulation auto-step
  useEffect(() => {
    if (mode === 'simulation' && simStep >= 0 && simStep < 30) {
      fetchSimStep(simStep);
    }
  }, [mode, simStep, fetchSimStep]);

  useEffect(() => {
    if (mode === 'simulation' && simStep >= 0 && simStep < 29) {
      const delay = 2000 / simSpeed;
      simInterval.current = setTimeout(() => {
        setSimStep(s => s + 1);
      }, delay);
      return () => clearTimeout(simInterval.current);
    }
  }, [mode, simStep, simSpeed]);

  // Start simulation
  const startSimulation = () => {
    setMode('simulation');
    setSimStep(0);
  };

  // Pause
  const pauseSimulation = () => {
    clearTimeout(simInterval.current);
    setMode('idle');
  };

  // Reset
  const resetSimulation = () => {
    clearTimeout(simInterval.current);
    setMode('idle');
    setSimStep(-1);
    setSimData(null);
    setKpis(null);
    setTowers([]);
    setCustomers([]);
    setFeed([]);
  };

  // Start live
  const startLive = () => {
    setMode('live');
    setSimStep(-1);
    setSimData(null);
  };

  // Load initial data on mount
  useEffect(() => {
    fetchLiveData();
  }, [fetchLiveData]);

  return (
    <div className="app-container">
      {/* Header */}
      <header className="app-header">
        <div className="header-left">
          <div className="logo-mark">
            <svg width="32" height="32" viewBox="0 0 32 32">
              <polygon points="16,2 28,9 28,23 16,30 4,23 4,9" fill="none" stroke="#00AEEF" strokeWidth="2"/>
              <circle cx="16" cy="16" r="4" fill="#00AEEF"/>
              <line x1="16" y1="2" x2="16" y2="12" stroke="#00AEEF" strokeWidth="1.5" opacity="0.5"/>
              <line x1="28" y1="9" x2="20" y2="14" stroke="#00AEEF" strokeWidth="1.5" opacity="0.5"/>
              <line x1="28" y1="23" x2="20" y2="18" stroke="#00AEEF" strokeWidth="1.5" opacity="0.5"/>
              <line x1="16" y1="30" x2="16" y2="20" stroke="#00AEEF" strokeWidth="1.5" opacity="0.5"/>
              <line x1="4" y1="23" x2="12" y2="18" stroke="#00AEEF" strokeWidth="1.5" opacity="0.5"/>
              <line x1="4" y1="9" x2="12" y2="14" stroke="#00AEEF" strokeWidth="1.5" opacity="0.5"/>
            </svg>
          </div>
          <div>
            <h1 className="header-title">TelcoMax Dynamic 5G Network Slicing</h1>
            <p className="header-subtitle">
              Lumen Field -- Live Event Intelligence
              {simData?.timeline_label && (
                <span className="timeline-badge">{simData.timeline_label}</span>
              )}
            </p>
          </div>
        </div>
        <div className="header-right">
          <div className={`status-indicator ${mode === 'idle' ? 'idle' : 'active'}`}>
            <span className="status-dot" />
            {mode === 'simulation' ? 'SIMULATING' : mode === 'live' ? 'LIVE' : 'READY'}
          </div>
          {error && <div className="error-badge">{error}</div>}
        </div>
      </header>

      {/* KPI Bar */}
      <KPIBar kpis={kpis} />

      {/* Main Content */}
      <div className="main-content">
        <div className="map-panel">
          <StadiumMap towers={towers} customers={customers} />
        </div>
        <div className="feed-panel">
          <LiveFeed feed={feed} />
        </div>
      </div>

      {/* Simulation Controls */}
      <SimControls
        mode={mode}
        simStep={simStep}
        simSpeed={simSpeed}
        timelineLabel={simData?.timeline_label || ''}
        onStart={startSimulation}
        onPause={pauseSimulation}
        onReset={resetSimulation}
        onLive={startLive}
        onSpeedChange={setSimSpeed}
      />
    </div>
  );
}
