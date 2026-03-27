import { useEffect, useMemo, useRef, useState } from 'react'
import { useDebugLog } from '../debug/DebugContext.jsx'

const INITIAL_VISIBLE = 10
const PAGE_SIZE = 10

function fmtDate(dateStr) {
  if (!dateStr) {
    return 'Unknown date'
  }
  const date = new Date(`${dateStr}T00:00:00`)
  return date.toLocaleDateString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  })
}

function buildEloUrl(mode, startDate, endDate) {
  const params = new URLSearchParams()
  params.set('mode', mode)
  params.set('maxPlayers', '5000')
  if (mode === 'date-range') {
    params.set('start', startDate)
    params.set('end', endDate)
  }
  return `/api/elo?${params.toString()}`
}

export default function HomePage() {
  const dlog = useDebugLog()
  const [mode, setMode] = useState('all-time')
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [search, setSearch] = useState('')
  const [eloData, setEloData] = useState([])
  const [missingRanges, setMissingRanges] = useState([])
  const [eloLoading, setEloLoading] = useState(false)
  const [eloError, setEloError] = useState('')
  const [visibleCount, setVisibleCount] = useState(INITIAL_VISIBLE)
  const [events, setEvents] = useState([])
  const [expanded, setExpanded] = useState({})
  const [eventsLoading, setEventsLoading] = useState(false)
  const [eventsError, setEventsError] = useState('')
  const [eventsRemaining, setEventsRemaining] = useState(0)
  const [dateRangeProgress, setDateRangeProgress] = useState({
    active: false,
    phase: '',
    processedEvents: 0,
    totalEvents: 0,
    progressPct: 0,
  })
  const [coverageResolving, setCoverageResolving] = useState(false)
  const [coverageDetail, setCoverageDetail] = useState('')
  const revealRef = useRef(null)

  async function refreshAllTimeElo() {
    dlog('info', 'Home', 'refreshAllTimeElo — fetching all-time ELO')
    setEloLoading(true)
    setEloError('')
    try {
      const res = await fetch(buildEloUrl('all-time', null, null))
      const data = await res.json()
      if (!res.ok) {
        throw new Error(data.error || 'Failed to load ELO rankings')
      }
      dlog('info', 'Home', `refreshAllTimeElo done — ${(data.rankings || []).length} players, ${(data.missingRanges || []).length} missing ranges`)
      setMode('all-time')
      setEloData(data.rankings || [])
      setMissingRanges(data.missingRanges || [])
      setVisibleCount(INITIAL_VISIBLE)
    } catch (err) {
      dlog('error', 'Home', `refreshAllTimeElo failed: ${err.message}`)
      setEloError(err instanceof Error ? err.message : 'Failed to load ELO rankings')
    } finally {
      setEloLoading(false)
    }
  }

  async function resolveCoverageGaps() {
    if (missingRanges.length === 0 || coverageResolving) {
      return
    }
    dlog('info', 'Home', `resolveCoverageGaps — ${missingRanges.length} ranges to resolve`)
    setCoverageResolving(true)
    setCoverageDetail('Starting…')
    setEloError('')
    try {
      const startRes = await fetch('/api/coverage/resolve/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ranges: missingRanges }),
      })
      const startData = await startRes.json()
      if (!startRes.ok) {
        throw new Error(startData.error || 'Failed to start coverage resolve')
      }
      const jobId = startData.jobId
      await new Promise((resolve, reject) => {
        const intervalId = window.setInterval(async () => {
          try {
            const stRes = await fetch(`/api/coverage/resolve/status?jobId=${encodeURIComponent(jobId)}`)
            const st = await stRes.json()
            if (!stRes.ok) {
              throw new Error(st.error || 'Coverage status failed')
            }
            const cur = st.currentWindow || 0
            const tot = st.totalWindows || 0
            const msg = tot ? `Scraping window ${cur}/${tot}${st.detail ? `: ${st.detail}` : ''}` : (st.detail || 'Working…')
            dlog('info', 'Home', `coverageResolve poll — ${msg}`)
            setCoverageDetail(msg)
            if (st.status === 'done') {
              window.clearInterval(intervalId)
              setCoverageDetail(
                st.remainingCount === 0
                  ? 'All listed ranges resolved (or confirmed empty).'
                  : `${st.remainingCount} possible gap(s) remain.`,
              )
              resolve()
            }
            if (st.status === 'error') {
              window.clearInterval(intervalId)
              reject(new Error(st.error || 'Coverage resolve failed'))
            }
          } catch (e) {
            window.clearInterval(intervalId)
            reject(e)
          }
        }, 1000)
      })
      await refreshAllTimeElo()
    } catch (err) {
      setEloError(err instanceof Error ? err.message : 'Coverage resolve failed')
    } finally {
      setCoverageResolving(false)
    }
  }

  useEffect(() => {
    let cancelled = false
    async function loadAllTimeElo() {
      dlog('info', 'Home', 'Mount — loading initial all-time ELO')
      setEloLoading(true)
      setEloError('')
      try {
        const res = await fetch(buildEloUrl('all-time', null, null))
        const data = await res.json()
        if (!res.ok) {
          throw new Error(data.error || 'Failed to load ELO rankings')
        }
        if (!cancelled) {
          dlog('info', 'Home', `Initial ELO loaded — ${(data.rankings || []).length} players`)
          setMode('all-time')
          setEloData(data.rankings || [])
          setMissingRanges(data.missingRanges || [])
          setVisibleCount(INITIAL_VISIBLE)
          setCoverageDetail('')
        }
      } catch (err) {
        if (!cancelled) {
          dlog('error', 'Home', `Initial ELO failed: ${err.message}`)
          setEloError(err instanceof Error ? err.message : 'Failed to load ELO rankings')
        }
      } finally {
        if (!cancelled) {
          setEloLoading(false)
        }
      }
    }
    loadAllTimeElo()
    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    let intervalId = null
    async function loadEvents() {
      dlog('info', 'Home', 'Mount — starting recent events job (last 30 days, 10 events)')
      setEventsLoading(true)
      setEventsError('')
      setEventsRemaining(10)
      try {
        const startRes = await fetch('/api/recent-events/start?days=30&limit=10&sampleRegistrants=8')
        const startData = await startRes.json()
        if (!startRes.ok) {
          throw new Error(startData.error || 'Failed to start recent events loading')
        }
        const jobId = startData.jobId
        dlog('info', 'Home', `Recent events job started — jobId=${jobId}`)
        intervalId = window.setInterval(async () => {
          if (cancelled) {
            return
          }
          const statusRes = await fetch(`/api/recent-events/status?jobId=${encodeURIComponent(jobId)}`)
          const statusData = await statusRes.json()
          if (!statusRes.ok) {
            throw new Error(statusData.error || 'Failed to fetch recent events status')
          }
          if (cancelled) {
            return
          }
          dlog('info', 'Home', `Recent events poll — status=${statusData.status}, ${(statusData.events||[]).length} events loaded, ${statusData.remaining ?? 0} remaining`)
          setEvents(statusData.events || [])
          setEventsRemaining(Number(statusData.remaining || 0))
          if (statusData.status === 'done' || statusData.status === 'error') {
            window.clearInterval(intervalId)
            intervalId = null
            setEventsLoading(false)
            dlog(statusData.status === 'done' ? 'info' : 'error', 'Home', `Recent events ${statusData.status}`)
            if (statusData.status === 'error') {
              setEventsError(statusData.error || 'Failed to load recent events')
            }
          }
        }, 900)
      } catch (err) {
        if (!cancelled) {
          setEventsError(err instanceof Error ? err.message : 'Failed to load recent events')
          setEventsLoading(false)
        }
      }
    }
    loadEvents()
    return () => {
      cancelled = true
      if (intervalId) {
        window.clearInterval(intervalId)
      }
    }
  }, [])

  useEffect(() => {
    const node = revealRef.current
    if (!node) {
      return undefined
    }
    const observer = new IntersectionObserver(
      (entries) => {
        const first = entries[0]
        if (first?.isIntersecting) {
          setVisibleCount((prev) => prev + PAGE_SIZE)
        }
      },
      { rootMargin: '100px' },
    )
    observer.observe(node)
    return () => observer.disconnect()
  }, [])

  async function showAllTime() {
    dlog('info', 'Home', 'showAllTime — switching to all-time ELO')
    setEloLoading(true)
    setEloError('')
    try {
      const res = await fetch(buildEloUrl('all-time', null, null))
      const data = await res.json()
      if (!res.ok) {
        throw new Error(data.error || 'Failed to load all-time rankings')
      }
      dlog('info', 'Home', `showAllTime done — ${(data.rankings || []).length} players`)
      setMode('all-time')
      setEloData(data.rankings || [])
      setMissingRanges(data.missingRanges || [])
      setVisibleCount(INITIAL_VISIBLE)
    } catch (err) {
      dlog('error', 'Home', `showAllTime failed: ${err.message}`)
      setEloError(err instanceof Error ? err.message : 'Failed to load all-time rankings')
    } finally {
      setEloLoading(false)
    }
  }

  async function showDateRange() {
    if (!startDate || !endDate) {
      setEloError('Choose both start and end date.')
      return
    }
    dlog('info', 'Home', `showDateRange — ${startDate} to ${endDate}`)
    setEloLoading(true)
    setDateRangeProgress({
      active: true,
      phase: 'queued',
      processedEvents: 0,
      totalEvents: 0,
      progressPct: 0,
    })
    setEloError('')
    let intervalId = null
    try {
      const startRes = await fetch(
        `/api/elo/date-range/start?start=${encodeURIComponent(startDate)}&end=${encodeURIComponent(endDate)}`,
      )
      const startPayload = await startRes.json()
      if (!startRes.ok) {
        throw new Error(startPayload.error || 'Failed to start date range ranking job')
      }
      const jobId = startPayload.jobId
      await new Promise((resolve, reject) => {
        intervalId = window.setInterval(async () => {
          try {
            const statusRes = await fetch(`/api/elo/date-range/status?jobId=${encodeURIComponent(jobId)}`)
            const statusPayload = await statusRes.json()
            if (!statusRes.ok) {
              throw new Error(statusPayload.error || 'Failed to fetch date range job status')
            }
            dlog('info', 'Home', `dateRange poll — phase=${statusPayload.phase || '?'}, ${statusPayload.processedEvents || 0}/${statusPayload.totalEvents || '?'} events, ${Math.round(statusPayload.progressPct || 0)}%`)
            setDateRangeProgress({
              active: statusPayload.status === 'running',
              phase: statusPayload.phase || '',
              processedEvents: Number(statusPayload.processedEvents || 0),
              totalEvents: Number(statusPayload.totalEvents || 0),
              progressPct: Number(statusPayload.progressPct || 0),
            })
            if (statusPayload.status === 'done') {
              window.clearInterval(intervalId)
              intervalId = null
              const data = statusPayload.result || {}
              setMode('date-range')
              setEloData(data.rankings || [])
              setMissingRanges([])
              setVisibleCount(INITIAL_VISIBLE)
              resolve()
            }
            if (statusPayload.status === 'error') {
              window.clearInterval(intervalId)
              intervalId = null
              reject(new Error(statusPayload.error || 'Date range job failed'))
            }
          } catch (pollError) {
            window.clearInterval(intervalId)
            intervalId = null
            reject(pollError)
          }
        }, 850)
      })
    } catch (err) {
      setEloError(err instanceof Error ? err.message : 'Failed to load date range rankings')
    } finally {
      if (intervalId) {
        window.clearInterval(intervalId)
      }
      setDateRangeProgress((prev) => ({ ...prev, active: false }))
      setEloLoading(false)
    }
  }

  const filteredElo = useMemo(() => {
    const q = search.trim().toLowerCase()
    if (!q) {
      return eloData
    }
    return eloData.filter((row) => row.player.toLowerCase().includes(q))
  }, [eloData, search])

  const shownElo = filteredElo.slice(0, visibleCount)

  function toggleCard(id) {
    setExpanded((prev) => ({ ...prev, [id]: !prev[id] }))
  }

  return (
    <main className="content-grid">
      <section className="panel">
        <h2 className="panel-title">ELO Rankings</h2>
        <div className="control-row">
          <label>
            Start
            <input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} />
          </label>
          <label>
            End
            <input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} />
          </label>
        </div>
        <div className="button-row">
          <button
            type="button"
            className={`elo-mode-btn ${mode === 'date-range' ? 'elo-mode-btn--active' : 'elo-mode-btn--inactive'}`}
            onClick={showDateRange}
            disabled={eloLoading}
          >
            Show Date Range
          </button>
          <button
            type="button"
            className={`elo-mode-btn ${mode === 'all-time' ? 'elo-mode-btn--active' : 'elo-mode-btn--inactive'}`}
            onClick={showAllTime}
            disabled={eloLoading}
          >
            Show All Time
          </button>
        </div>
        <input
          className="search-input"
          placeholder="Search gamertags (reactive as you type)"
          value={search}
          onChange={(e) => {
            setSearch(e.target.value)
            setVisibleCount(INITIAL_VISIBLE)
          }}
        />
        {dateRangeProgress.active ? (
          <div className="progress-wrap">
            <div className="progress-meta">
              <span>Date range load: {Math.round(dateRangeProgress.progressPct)}%</span>
              <span>
                {dateRangeProgress.processedEvents}/{dateRangeProgress.totalEvents || '?'} events
              </span>
            </div>
            <div className="progress-track">
              <div className="progress-fill" style={{ width: `${dateRangeProgress.progressPct}%` }} />
            </div>
          </div>
        ) : null}

        {mode === 'all-time' && missingRanges.length > 0 ? (
          <div className="warning">
            <div className="warning-header">
              <div>
                <strong>Warning:</strong> possible unscraped weeks (no cached tournaments and not yet
                confirmed empty by a full scrape).
              </div>
              <button
                type="button"
                className="resolve-btn"
                disabled={eloLoading || coverageResolving}
                onClick={resolveCoverageGaps}
              >
                {coverageResolving ? 'Resolving…' : 'Resolve'}
              </button>
            </div>
            <div className="warning-ranges">
              {missingRanges.slice(0, 3).map((range) => (
                <span key={`${range.start}-${range.end}`}>
                  {range.start} - {range.end}
                </span>
              ))}
              {missingRanges.length > 3 ? <span>... and more</span> : null}
            </div>
            {coverageDetail ? <p className="coverage-detail">{coverageDetail}</p> : null}
          </div>
        ) : null}

        {eloError ? <p className="error">{eloError}</p> : null}
        {eloLoading ? <p className="loading">Loading rankings...</p> : null}

        <div className="table-head">
          <span>Player</span>
          <span>ELO</span>
        </div>
        <div className="ranking-list">
          {shownElo.map((row, index) => (
            <div
              key={`${row.player}-${row.rank}`}
              className="ranking-row fade-in"
              style={{ animationDelay: `${Math.min(index, 10) * 22}ms` }}
            >
              <span className="player-label">
                {row.rank}. {row.player}
              </span>
              <span className="score">{row.elo.toFixed ? row.elo.toFixed(2) : row.elo}</span>
            </div>
          ))}
          {!eloLoading && shownElo.length === 0 ? <p className="empty">No matching players.</p> : null}
          <div ref={revealRef} className="reveal-anchor" />
        </div>
      </section>

      <section className="panel">
        <h2 className="panel-title">Recent Events</h2>
        <p className="panel-subtitle">Last 30 days, most recent 10 events.</p>
        {eventsError ? <p className="error">{eventsError}</p> : null}
        {eventsLoading ? (
          <p className="loading">
            Loading events... ({eventsRemaining} events still loading!)
          </p>
        ) : null}
        <div className="event-list">
          {events.map((eventCard) => {
            const isOpen = !!expanded[eventCard.id]
            return (
              <a
                key={eventCard.id}
                className={`event-card ${isOpen ? 'open' : ''}`}
                href={eventCard.eventLink}
                target="_blank"
                rel="noreferrer"
              >
                <div className="event-header">
                  <div className="event-title-wrap">
                    {eventCard.iconUrl ? (
                      <img src={eventCard.iconUrl} alt="" className="event-icon" />
                    ) : (
                      <div className="event-icon fallback">S</div>
                    )}
                    <h3>{eventCard.title}</h3>
                  </div>
                  <button
                    type="button"
                    className="expand-btn"
                    onClick={(e) => {
                      e.preventDefault()
                      toggleCard(eventCard.id)
                    }}
                  >
                    {isOpen ? '-' : '+'}
                  </button>
                </div>
                <p className="meta-line">
                  <span>Date:</span> {fmtDate(eventCard.date)}
                </p>
                <p className="meta-line">
                  <span>Winner:</span> {eventCard.winner}
                </p>

                {isOpen ? (
                  <div className="expanded">
                    <div className="top8">
                      <h4>Top 8</h4>
                      <div className="top8-list">
                        {eventCard.top8.map((row, idx) => (
                          <div key={`${eventCard.id}-${row.placement}-${row.name}-${idx}`}>
                            #{row.placement} {row.name}
                          </div>
                        ))}
                      </div>
                    </div>

                    <div className="registrants">
                      <h4>Other Entrants</h4>
                      <div className="registrant-grid">
                        {eventCard.randomRegistrants.map((name, idx) => (
                          <span key={`${eventCard.id}-${name}-${idx}`}>{name}</span>
                        ))}
                      </div>
                    </div>
                    <p className="entrant-count">Total entrants: {eventCard.entrantCount}</p>
                  </div>
                ) : null}
              </a>
            )
          })}
        </div>
      </section>
    </main>
  )
}
