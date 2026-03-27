import { useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { useDebugLog } from '../debug/DebugContext.jsx'

const CURRENT_YEAR = new Date().getFullYear()

const QUARTERS = [
  { label: `Q1: January 1 to March 31`, start: `${CURRENT_YEAR}-01-01`, end: `${CURRENT_YEAR}-03-31` },
  { label: `Q2: April 1 to June 30`, start: `${CURRENT_YEAR}-04-01`, end: `${CURRENT_YEAR}-06-30` },
  { label: `Q3: July 1 to September 30`, start: `${CURRENT_YEAR}-07-01`, end: `${CURRENT_YEAR}-09-30` },
  { label: `Q4: October 1 to December 31`, start: `${CURRENT_YEAR}-10-01`, end: `${CURRENT_YEAR}-12-31` },
]

const PHASE_LABELS = {
  queued: 'Starting...',
  clearing_cache: 'Clearing cache...',
  fetching: 'Fetching NorCal events from start.gg (Ultimate, 16+ entrants)...',
  saving: 'Saving to cache...',
  done: 'Done',
  error: 'Error',
}

export default function PRMakerPage() {
  const dlog = useDebugLog()
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [activeQuarter, setActiveQuarter] = useState(null)
  const [jobId, setJobId] = useState(null)
  const [jobStatus, setJobStatus] = useState(null)
  const [scrapeError, setScrapeError] = useState('')
  const [consoleOpen, setConsoleOpen] = useState(false)
  const consoleEndRef = useRef(null)

  function selectQuarter(idx) {
    const q = QUARTERS[idx]
    dlog('info', 'PRMaker/Scrape', `Selected quarter ${idx + 1}: ${q.start} — ${q.end}`)
    setStartDate(q.start)
    setEndDate(q.end)
    setActiveQuarter(idx)
  }

  function handleDateChange(which, value) {
    if (which === 'start') {
      setStartDate(value)
    } else {
      setEndDate(value)
    }
    setActiveQuarter(null)
  }

  async function startScrape(fresh) {
    if (!startDate || !endDate) {
      setScrapeError('Please select both a start and end date.')
      return
    }
    dlog('info', 'PRMaker/Scrape', `startScrape — ${fresh ? 'FRESH' : 'normal'} scrape for ${startDate} to ${endDate}`)
    setScrapeError('')
    setJobStatus(null)
    try {
      const url =
        `/api/pr-maker/scrape/start?start=${encodeURIComponent(startDate)}` +
        `&end=${encodeURIComponent(endDate)}` +
        `&fresh=${fresh ? 'true' : 'false'}`
      const res = await fetch(url)
      const data = await res.json()
      if (!res.ok) {
        throw new Error(data.error || 'Failed to start scrape')
      }
      dlog('info', 'PRMaker/Scrape', `Scrape job started — jobId=${data.jobId}`)
      setJobId(data.jobId)
    } catch (err) {
      dlog('error', 'PRMaker/Scrape', `startScrape failed: ${err.message}`)
      setScrapeError(err instanceof Error ? err.message : 'Failed to start scrape')
    }
  }

  useEffect(() => {
    if (!jobId) {
      return undefined
    }
    let cancelled = false
    const intervalId = window.setInterval(async () => {
      if (cancelled) {
        return
      }
      try {
        const res = await fetch(`/api/pr-maker/scrape/status?jobId=${encodeURIComponent(jobId)}`)
        const data = await res.json()
        if (!res.ok) {
          throw new Error(data.error || 'Failed to poll scrape status')
        }
        if (cancelled) {
          return
        }
        dlog('info', 'PRMaker/Scrape', `Poll — phase=${data.phase || '?'}, status=${data.status}, ${data.tournamentsTotal || 0} events found`)
        setJobStatus(data)
        if (data.status === 'done' || data.status === 'error') {
          window.clearInterval(intervalId)
          if (data.status === 'error') {
            setScrapeError(data.error || 'Scrape failed')
          }
        }
      } catch (err) {
        if (!cancelled) {
          setScrapeError(err instanceof Error ? err.message : 'Polling failed')
          window.clearInterval(intervalId)
        }
      }
    }, 800)
    return () => {
      cancelled = true
      window.clearInterval(intervalId)
    }
  }, [jobId])

  useEffect(() => {
    if (consoleOpen && consoleEndRef.current) {
      consoleEndRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [consoleOpen, jobStatus?.log?.length])

  const isRunning = jobStatus && jobStatus.status === 'running'
  const isDone = jobStatus && jobStatus.status === 'done'
  const phase = jobStatus?.phase || ''
  const phaseLabel = PHASE_LABELS[phase] || phase
  const logLines = jobStatus?.log || []
  const totalFound = jobStatus?.tournamentsTotal || 0

  return (
    <main className="pr-maker-page" aria-label="PR Maker">
      <div className="pr-maker-container">
        <h2 className="panel-title">PR Maker</h2>

        <div className="pr-maker-dates">
          <label className="pr-maker-date-label">
            Start
            <input
              type="date"
              value={startDate}
              onChange={(e) => handleDateChange('start', e.target.value)}
            />
          </label>
          <label className="pr-maker-date-label">
            End
            <input
              type="date"
              value={endDate}
              onChange={(e) => handleDateChange('end', e.target.value)}
            />
          </label>
        </div>

        <div className="quarter-row">
          {QUARTERS.map((q, idx) => (
            <button
              key={idx}
              type="button"
              className={`quarter-btn ${activeQuarter === idx ? 'quarter-btn--active' : ''}`}
              onClick={() => selectQuarter(idx)}
            >
              {q.label}
            </button>
          ))}
        </div>

        <div className="pr-maker-actions">
          <button
            type="button"
            className="pr-maker-action-btn pr-maker-action-btn--primary"
            disabled={isRunning}
            onClick={() => startScrape(false)}
          >
            {isRunning && !jobStatus?.fresh ? 'Scraping...' : 'Scrape'}
          </button>
          <button
            type="button"
            className="pr-maker-action-btn pr-maker-action-btn--secondary"
            disabled={isRunning}
            onClick={() => startScrape(true)}
          >
            {isRunning && jobStatus?.fresh ? 'Fresh Scraping...' : 'Fresh Scrape'}
          </button>
        </div>

        {scrapeError ? <p className="error">{scrapeError}</p> : null}

        {isRunning ? (
          <div className="pr-maker-progress">
            <div className="progress-meta">
              <span>{phaseLabel}</span>
              <span>{totalFound} ELO-eligible event(s) found</span>
            </div>
            <div className="progress-track">
              <div className="progress-fill progress-fill--indeterminate" />
            </div>
          </div>
        ) : null}

        {isDone && !isRunning ? (
          <div className="pr-maker-progress">
            <div className="progress-meta">
              <span>Complete</span>
              <span>
                {jobStatus.tournamentsCached} cached, {jobStatus.tournamentsNew} new —{' '}
                {totalFound} total
              </span>
            </div>
            <div className="progress-track">
              <div className="progress-fill" style={{ width: '100%' }} />
            </div>
          </div>
        ) : null}

        {logLines.length > 0 ? (
          <div className="pr-maker-log-section">
            <button
              type="button"
              className="pr-maker-console-toggle"
              aria-expanded={consoleOpen}
              aria-controls="pr-maker-console-panel"
              id="pr-maker-console-toggle"
              onClick={() => setConsoleOpen((o) => !o)}
            >
              <span className="pr-maker-console-toggle-label">Cache &amp; scrape log</span>
              <span className="pr-maker-console-toggle-meta">
                {logLines.length} line{logLines.length === 1 ? '' : 's'}
              </span>
              <span className="pr-maker-console-toggle-chevron" aria-hidden="true">
                {consoleOpen ? '▾' : '▸'}
              </span>
            </button>
            <div
              id="pr-maker-console-panel"
              role="region"
              aria-labelledby="pr-maker-console-toggle"
              aria-hidden={!consoleOpen}
              className={`pr-maker-console-panel ${consoleOpen ? 'pr-maker-console-panel--open' : ''}`}
            >
              <div className="pr-maker-console" role="log" aria-live="polite">
                {logLines.map((line, idx) => (
                  <div
                    key={idx}
                    className={
                      line.startsWith('[CACHED]')
                        ? 'console-line console-line--cached'
                        : line.startsWith('[NOT CACHED]')
                          ? 'console-line console-line--new'
                          : line.startsWith('[DONE]')
                            ? 'console-line console-line--done'
                            : line.startsWith('[FRESH]')
                              ? 'console-line console-line--fresh'
                              : line.startsWith('[FILTER]')
                                ? 'console-line console-line--filter'
                                : 'console-line'
                    }
                  >
                    {line}
                  </div>
                ))}
                <div ref={consoleEndRef} />
              </div>
            </div>
          </div>
        ) : null}

        {isDone ? (
          <div className="pr-maker-continue-wrap">
            <Link
              to={`/pr-maker/process?start=${encodeURIComponent(startDate)}&end=${encodeURIComponent(endDate)}`}
              className="pr-maker-continue-btn"
            >
              Continue
            </Link>
          </div>
        ) : null}
      </div>
    </main>
  )
}
