import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { useDebugLog } from '../debug/DebugContext.jsx'

const DEFAULT_SORT_ORDER = [
  { key: 'date', label: 'Date' },
  { key: 'name', label: 'Name' },
  { key: 'entrants', label: '# of Entrants' },
]

function fmtDatePacific(iso) {
  if (!iso) return ''
  const [y, m, d] = iso.split('-').map(Number)
  const dt = new Date(y, m - 1, d)
  return new Intl.DateTimeFormat('en-US', {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    timeZone: 'America/Los_Angeles',
  }).format(dt)
}

function sortEvents(events, sortOrder) {
  return [...events].sort((a, b) => {
    for (const criterion of sortOrder) {
      let cmp = 0
      if (criterion.key === 'date') {
        cmp = (a.startAt || 0) - (b.startAt || 0)
      } else if (criterion.key === 'name') {
        cmp = (a.title || '').localeCompare(b.title || '')
      } else if (criterion.key === 'entrants') {
        cmp = (b.entrantCount || 0) - (a.entrantCount || 0)
      }
      if (cmp !== 0) return cmp
    }
    return 0
  })
}

function normalizeTournamentId(ev) {
  return String(ev.tournamentId || '').trim()
}

/** Split events: same start.gg tournament (non-empty id) with 2+ eligible rows → duplicates pool. */
function partitionEventsByTournament(events) {
  const counts = new Map()
  for (const ev of events) {
    const tid = normalizeTournamentId(ev)
    if (!tid) continue
    counts.set(tid, (counts.get(tid) || 0) + 1)
  }
  const singletons = []
  const duplicates = []
  for (const ev of events) {
    const tid = normalizeTournamentId(ev)
    if (!tid || (counts.get(tid) || 0) < 2) {
      singletons.push(ev)
    } else {
      duplicates.push(ev)
    }
  }
  const duplicateTournamentCount = [...counts.values()].filter((n) => n >= 2).length
  return { singletons, duplicates, duplicateTournamentCount }
}

/** Group duplicate events by tournament, sort groups by date then name; sort inside each group by user sort order. */
function buildDuplicateGroupsSorted(duplicates, sortOrder) {
  const byTid = new Map()
  for (const ev of duplicates) {
    const tid = normalizeTournamentId(ev)
    if (!byTid.has(tid)) byTid.set(tid, [])
    byTid.get(tid).push(ev)
  }
  const groups = [...byTid.values()].map((g) => sortEvents(g, sortOrder))
  groups.sort((a, b) => {
    const aMin = Math.min(...a.map((e) => e.startAt || 0))
    const bMin = Math.min(...b.map((e) => e.startAt || 0))
    if (aMin !== bMin) return aMin - bMin
    return (a[0]?.tournamentName || '').localeCompare(b[0]?.tournamentName || '')
  })
  return groups
}

function ProcessEventRow({ ev, checked, onToggle, duplicateSection }) {
  const showBracket =
    duplicateSection &&
    ev.eventName &&
    ev.eventName !== '(unknown event)'
  return (
    <li
      className={`process-event-row${duplicateSection ? ' process-event-row--duplicate' : ''}`}
    >
      <label className="process-event-checkbox-wrap">
        <input
          type="checkbox"
          className="process-event-checkbox"
          checked={checked}
          onChange={() => onToggle(ev.eventSlug)}
        />
        <span className="process-checkbox-visual" aria-hidden="true" />
      </label>
      <a
        className={`process-event-card${checked ? '' : ' process-event-card--unchecked'}`}
        href={ev.eventLink}
        target="_blank"
        rel="noopener noreferrer"
      >
        <span className="process-event-title">{ev.title}</span>
        <span className="process-event-meta">
          {fmtDatePacific(ev.date)} · {ev.entrantCount} entrant{ev.entrantCount === 1 ? '' : 's'}
        </span>
        {showBracket ? (
          <span className="process-event-meta process-event-meta--bracket">{ev.eventName}</span>
        ) : null}
      </a>
    </li>
  )
}

export default function PRMakerProcessPage() {
  const dlog = useDebugLog()
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const startDate = searchParams.get('start') || ''
  const endDate = searchParams.get('end') || ''

  const [events, setEvents] = useState([])
  const [selected, setSelected] = useState(new Set())
  const [sortOrder, setSortOrder] = useState(DEFAULT_SORT_ORDER)
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState('')

  const [processJobId, setProcessJobId] = useState(null)
  const [processStatus, setProcessStatus] = useState(null)
  const [processError, setProcessError] = useState('')
  const [showModal, setShowModal] = useState(false)
  const [lastIngestSlugs, setLastIngestSlugs] = useState([])

  const dragIdx = useRef(null)
  const dragOverIdx = useRef(null)

  useEffect(() => {
    if (!startDate || !endDate) {
      setLoading(false)
      setLoadError('Missing date range — go back to the scrape page.')
      return
    }
    let cancelled = false
    ;(async () => {
      dlog('info', 'PRMaker/Process', `Loading cached events for ${startDate} — ${endDate}`)
      setLoading(true)
      setLoadError('')
      try {
        const res = await fetch(
          `/api/pr-maker/events?start=${encodeURIComponent(startDate)}&end=${encodeURIComponent(endDate)}`
        )
        const data = await res.json()
        if (!res.ok) throw new Error(data.error || 'Failed to load events')
        if (cancelled) return
        const evts = data.events || []
        dlog('info', 'PRMaker/Process', `Loaded ${evts.length} cached events, all selected by default`)
        setEvents(evts)
        setSelected(new Set(evts.map((e) => e.eventSlug)))
      } catch (err) {
        if (!cancelled) setLoadError(err instanceof Error ? err.message : 'Load failed')
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => { cancelled = true }
  }, [startDate, endDate])

  useEffect(() => {
    if (!processJobId) return undefined
    let cancelled = false
    const intervalId = window.setInterval(async () => {
      if (cancelled) return
      try {
        const res = await fetch(
          `/api/pr-maker/process/status?jobId=${encodeURIComponent(processJobId)}`
        )
        const data = await res.json()
        if (!res.ok) throw new Error(data.error || 'Poll failed')
        if (cancelled) return
        dlog('info', 'PRMaker/Process', `Ingest poll — event ${data.currentEvent || '?'}/${data.totalEvents || '?'}, sets ${data.currentEventSetsProcessed || 0}/${data.currentEventSets || '?'}, ${Math.round(data.progressPct || 0)}%${data.currentEventName ? ' · ' + data.currentEventName : ''}`)
        setProcessStatus(data)
        if (data.status === 'done' || data.status === 'error') {
          dlog(data.status === 'done' ? 'info' : 'error', 'PRMaker/Process', `Ingest ${data.status}${data.totalSetsProcessed ? ' — ' + data.totalSetsProcessed + ' total sets' : ''}`)
          window.clearInterval(intervalId)
          if (data.status === 'error') setProcessError(data.error || 'Processing failed')
        }
      } catch (err) {
        if (!cancelled) {
          setProcessError(err instanceof Error ? err.message : 'Polling failed')
          window.clearInterval(intervalId)
        }
      }
    }, 800)
    return () => {
      cancelled = true
      window.clearInterval(intervalId)
    }
  }, [processJobId])

  const toggleEvent = useCallback((slug) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(slug)) next.delete(slug)
      else next.add(slug)
      return next
    })
  }, [])

  const selectAll = useCallback(() => {
    setSelected(new Set(events.map((e) => e.eventSlug)))
  }, [events])

  const deselectAll = useCallback(() => {
    setSelected(new Set())
  }, [])

  const moveSortItem = useCallback((fromIdx, toIdx) => {
    setSortOrder((prev) => {
      if (toIdx < 0 || toIdx >= prev.length) return prev
      const next = [...prev]
      const [item] = next.splice(fromIdx, 1)
      next.splice(toIdx, 0, item)
      return next
    })
  }, [])

  const handleDragStart = (idx) => { dragIdx.current = idx }
  const handleDragOver = (e, idx) => {
    e.preventDefault()
    dragOverIdx.current = idx
  }
  const handleDrop = () => {
    if (dragIdx.current !== null && dragOverIdx.current !== null && dragIdx.current !== dragOverIdx.current) {
      moveSortItem(dragIdx.current, dragOverIdx.current)
    }
    dragIdx.current = null
    dragOverIdx.current = null
  }

  const { singletonSorted, duplicateGroups, duplicateSortedFlat, duplicateTournamentCount, displayOrderForIngest } =
    useMemo(() => {
      const { singletons, duplicates, duplicateTournamentCount: dtc } = partitionEventsByTournament(events)
      const singletonSortedInner = sortEvents(singletons, sortOrder)
      const duplicateGroupsInner = buildDuplicateGroupsSorted(duplicates, sortOrder)
      const duplicateSortedFlatInner = duplicateGroupsInner.flat()
      return {
        singletonSorted: singletonSortedInner,
        duplicateGroups: duplicateGroupsInner,
        duplicateSortedFlat: duplicateSortedFlatInner,
        duplicateTournamentCount: dtc,
        displayOrderForIngest: [...singletonSortedInner, ...duplicateSortedFlatInner],
      }
    }, [events, sortOrder])
  const selectedCount = selected.size

  async function startIngestion() {
    const slugs = displayOrderForIngest
      .filter((e) => selected.has(e.eventSlug))
      .map((e) => e.eventSlug)
    if (!slugs.length) return
    dlog('info', 'PRMaker/Process', `Starting ingestion — ${slugs.length} events selected`)
    setLastIngestSlugs(slugs)
    setProcessError('')
    setProcessStatus(null)
    setShowModal(true)
    try {
      const res = await fetch('/api/pr-maker/process/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ eventSlugs: slugs }),
      })
      const data = await res.json()
      if (!res.ok) throw new Error(data.error || 'Failed to start')
      setProcessJobId(data.jobId)
    } catch (err) {
      setProcessError(err instanceof Error ? err.message : 'Failed to start')
    }
  }

  function handleProceed() {
    const authoritative =
      isDone &&
      processStatus &&
      Array.isArray(processStatus.eventSlugs) &&
      processStatus.eventSlugs.length > 0
        ? processStatus.eventSlugs
        : lastIngestSlugs
    dlog('info', 'PRMaker/Process', `Proceeding to candidates with ${authoritative.length} event slug(s) (from ${processStatus?.eventSlugs?.length ? 'completed job' : 'client ingest list'})`)
    const stored = {
      startDate,
      endDate,
      eventSlugs: authoritative,
      savedAt: Date.now(),
    }
    try {
      sessionStorage.setItem('prMakerCandidatesContext', JSON.stringify(stored))
      sessionStorage.setItem(`prMakerCandidates_${startDate}_${endDate}`, JSON.stringify(stored))
    } catch { /* ignore quota / private mode */ }
    navigate('/pr-maker/candidates', {
      state: { startDate, endDate, eventSlugs: authoritative },
    })
  }

  const isProcessing = processStatus && processStatus.status === 'running'
  const isDone = processStatus && processStatus.status === 'done'

  return (
    <>
      <main className="process-page" aria-label="PR Maker — Select Events">
        <div className="process-page-inner">
          <header className="process-header">
            <h2 className="panel-title">PR Maker</h2>
            <p className="process-subtitle">Select Events</p>
            {startDate && endDate ? (
              <p className="process-range-label">
                {fmtDatePacific(startDate)} — {fmtDatePacific(endDate)}
              </p>
            ) : null}
          </header>

          {loading ? (
            <div className="process-loading">
              <span className="spinner" aria-hidden="true" />
              Loading events...
            </div>
          ) : loadError ? (
            <div className="process-error">
              <p className="error">{loadError}</p>
              <Link to="/pr-maker" className="pr-maker-back-link">← Back to scrape</Link>
            </div>
          ) : events.length === 0 ? (
            <div className="process-empty">
              <p>No cached events found for this date range.</p>
              <Link to="/pr-maker" className="pr-maker-back-link">← Back to scrape</Link>
            </div>
          ) : (
            <div className="process-content-grid">
              <section className="process-events-section">
                <div className="process-events-toolbar">
                  <span className="process-events-count">
                    {events.length} event{events.length === 1 ? '' : 's'} · {selectedCount} selected
                    {duplicateTournamentCount > 0
                      ? ` · ${duplicateTournamentCount} tournament${duplicateTournamentCount === 1 ? '' : 's'} with multiple events`
                      : ''}
                  </span>
                  <div className="process-events-bulk">
                    <button type="button" className="process-bulk-btn" onClick={selectAll}>
                      Select All
                    </button>
                    <button type="button" className="process-bulk-btn" onClick={deselectAll}>
                      Deselect All
                    </button>
                  </div>
                </div>

                <ul className="process-event-list">
                  {singletonSorted.map((ev) => (
                    <ProcessEventRow
                      key={ev.eventSlug}
                      ev={ev}
                      checked={selected.has(ev.eventSlug)}
                      onToggle={toggleEvent}
                      duplicateSection={false}
                    />
                  ))}
                </ul>

                {duplicateSortedFlat.length > 0 ? (
                  <div className="process-duplicate-section">
                    <div className="process-events-divider" role="separator" />
                    <h3 className="process-duplicate-heading">Same tournament, multiple events</h3>
                    <p className="process-duplicate-hint">
                      These brackets share one start.gg tournament. Uncheck any you do not want ingested (e.g. ladder vs singles).
                    </p>
                    <ul className="process-event-list process-event-list--duplicates">
                      {duplicateGroups.flatMap((group) => {
                        const tid = normalizeTournamentId(group[0])
                        const tname = group[0]?.tournamentName || 'Tournament'
                        return [
                          <li key={`dup-head-${tid}`} className="process-duplicate-group-head">
                            <span className="process-duplicate-group-title">{tname}</span>
                            <span className="process-duplicate-group-count">
                              {group.length} event{group.length === 1 ? '' : 's'}
                            </span>
                          </li>,
                          ...group.map((ev) => (
                            <ProcessEventRow
                              key={ev.eventSlug}
                              ev={ev}
                              checked={selected.has(ev.eventSlug)}
                              onToggle={toggleEvent}
                              duplicateSection
                            />
                          )),
                        ]
                      })}
                    </ul>
                  </div>
                ) : null}
              </section>

              <aside className="process-filters-section">
                <h3 className="process-filters-title">Filters</h3>
                <p className="process-filters-hint">Drag or use arrows to reorder — top is highest priority</p>
                <ul className="process-sort-list">
                  {sortOrder.map((item, idx) => (
                    <li
                      key={item.key}
                      className="process-sort-item"
                      draggable
                      onDragStart={() => handleDragStart(idx)}
                      onDragOver={(e) => handleDragOver(e, idx)}
                      onDrop={handleDrop}
                    >
                      <span className="process-sort-handle" aria-hidden="true">☰</span>
                      <span className="process-sort-label">{item.label}</span>
                      <span className="process-sort-arrows">
                        <button
                          type="button"
                          className="process-sort-arrow"
                          aria-label={`Move ${item.label} up`}
                          disabled={idx === 0}
                          onClick={() => moveSortItem(idx, idx - 1)}
                        >
                          ▲
                        </button>
                        <button
                          type="button"
                          className="process-sort-arrow"
                          aria-label={`Move ${item.label} down`}
                          disabled={idx === sortOrder.length - 1}
                          onClick={() => moveSortItem(idx, idx + 1)}
                        >
                          ▼
                        </button>
                      </span>
                    </li>
                  ))}
                </ul>
              </aside>
            </div>
          )}
        </div>
      </main>

      {events.length > 0 && !showModal ? (
        <>
          <div className="process-bottom-fade" aria-hidden="true" />
          <div className="process-bottom-bar">
            <button
              type="button"
              className="process-ingest-btn"
              disabled={selectedCount === 0}
              onClick={startIngestion}
            >
              Ingest {selectedCount} Event{selectedCount === 1 ? '' : 's'}
            </button>
          </div>
        </>
      ) : null}

      {showModal ? (
        <div className="process-modal-overlay" aria-modal="true" role="dialog" aria-label="Ingesting events">
          <div className="process-modal">
            <h3 className="process-modal-title">
              {isDone ? 'Ingestion Complete' : isProcessing ? 'Ingesting Events...' : processError ? 'Error' : 'Starting...'}
            </h3>

            {processError ? (
              <p className="error">{processError}</p>
            ) : null}

            {processStatus && !processError ? (
              <>
                <p className="process-modal-phase">
                  {isDone
                    ? `Processed ${processStatus.totalEvents} event${processStatus.totalEvents === 1 ? '' : 's'} — ${processStatus.totalSetsProcessed ?? 0} total set${(processStatus.totalSetsProcessed ?? 0) === 1 ? '' : 's'}`
                    : `Event ${processStatus.currentEvent} of ${processStatus.totalEvents}`}
                </p>
                {!isDone && processStatus.currentEventName ? (
                  <p className="process-modal-event-name">{processStatus.currentEventName}</p>
                ) : null}
                {!isDone && processStatus.currentEventSets > 0 ? (
                  <p className="process-modal-sets">
                    Sets: {processStatus.currentEventSetsProcessed} / {processStatus.currentEventSets}
                  </p>
                ) : null}

                <div className="progress-track process-modal-progress">
                  <div
                    className={`progress-fill${isDone ? '' : isProcessing ? '' : ' progress-fill--indeterminate'}`}
                    style={{ width: `${processStatus.progressPct ?? 0}%` }}
                  />
                </div>
              </>
            ) : !processError ? (
              <div className="progress-track process-modal-progress">
                <div className="progress-fill progress-fill--indeterminate" />
              </div>
            ) : null}

            {processError ? (
              <button
                type="button"
                className="process-modal-close-btn"
                onClick={() => setShowModal(false)}
              >
                Close
              </button>
            ) : null}

            {isDone && !processError ? (
              <div className="process-modal-actions">
                <button
                  type="button"
                  className="process-modal-review-btn"
                  onClick={() => setShowModal(false)}
                >
                  Review
                </button>
                <button
                  type="button"
                  className="process-modal-proceed-btn"
                  onClick={handleProceed}
                >
                  Proceed
                </button>
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </>
  )
}
