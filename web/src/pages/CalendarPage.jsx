import { useEffect, useState } from 'react'
import { useDebugLog } from '../debug/DebugContext.jsx'

/** Today's calendar date in America/Los_Angeles as YYYY-MM-DD (matches NorCal scheduling). */
function getTodayPacificIso() {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Los_Angeles',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(new Date())
  const y = parts.find((p) => p.type === 'year')?.value
  const m = parts.find((p) => p.type === 'month')?.value
  const d = parts.find((p) => p.type === 'day')?.value
  if (!y || !m || !d) {
    return new Date().toISOString().slice(0, 10)
  }
  return `${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`
}

/** Add calendar days to a YYYY-MM-DD string without local-timezone drift. */
function addCalendarDaysIso(iso, deltaDays) {
  const [y, m, d] = iso.split('-').map(Number)
  const utcMs = Date.UTC(y, m - 1, d + deltaDays)
  const nd = new Date(utcMs)
  const yy = nd.getUTCFullYear()
  const mm = String(nd.getUTCMonth() + 1).padStart(2, '0')
  const dd = String(nd.getUTCDate()).padStart(2, '0')
  return `${yy}-${mm}-${dd}`
}

/** Format API YYYY-MM-DD (Pacific calendar day) for display in Pacific. */
function fmtDatePacific(dateStr) {
  if (!dateStr) {
    return 'Unknown date'
  }
  const [y, m, d] = dateStr.split('-').map(Number)
  if (!y || !m || !d) {
    return 'Unknown date'
  }
  return new Intl.DateTimeFormat(undefined, {
    timeZone: 'America/Los_Angeles',
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  }).format(new Date(Date.UTC(y, m - 1, d, 12, 0, 0)))
}

function weekdayHeaderLabel(day) {
  const pretty = fmtDatePacific(day.date)
  return `${day.weekday} - ${pretty}`
}

export default function CalendarPage() {
  const dlog = useDebugLog()
  const [selectedDate, setSelectedDate] = useState(getTodayPacificIso)
  const [dayEvents, setDayEvents] = useState([])
  const [weekDays, setWeekDays] = useState([])
  const [expanded, setExpanded] = useState({})
  const [dayLoading, setDayLoading] = useState(false)
  const [dayRefreshing, setDayRefreshing] = useState(false)
  const [weekLoading, setWeekLoading] = useState(false)
  const [dayError, setDayError] = useState('')
  const [weekError, setWeekError] = useState('')

  function cardExpansionKey(scope, eventId) {
    return `${scope}:${eventId}`
  }

  function toggleCard(scope, eventId) {
    const key = cardExpansionKey(scope, eventId)
    setExpanded((prev) => ({ ...prev, [key]: !prev[key] }))
  }

  async function loadDay(dateIso, { isRefresh = false } = {}) {
    dlog('info', 'Calendar', `loadDay — date=${dateIso}${isRefresh ? ' (refresh)' : ''}`)
    setDayError('')
    if (isRefresh) {
      setDayRefreshing(true)
    } else {
      setDayLoading(true)
    }
    try {
      const res = await fetch(`/api/calendar/day?date=${encodeURIComponent(dateIso)}&sampleRegistrants=12`)
      const data = await res.json()
      if (!res.ok) {
        throw new Error(data.error || 'Failed to load tournaments for selected day')
      }
      dlog('info', 'Calendar', `loadDay done — ${(data.events || []).length} tournaments on ${dateIso}`)
      setDayEvents(data.events || [])
    } catch (err) {
      dlog('error', 'Calendar', `loadDay failed: ${err.message}`)
      setDayError(err instanceof Error ? err.message : 'Failed to load tournaments for selected day')
      setDayEvents([])
    } finally {
      if (isRefresh) {
        setDayRefreshing(false)
      } else {
        setDayLoading(false)
      }
    }
  }

  async function loadWeek() {
    dlog('info', 'Calendar', 'loadWeek — fetching tournaments this week')
    setWeekLoading(true)
    setWeekError('')
    try {
      const res = await fetch(`/api/calendar/week?date=${encodeURIComponent(getTodayPacificIso())}&sampleRegistrants=8`)
      const data = await res.json()
      if (!res.ok) {
        throw new Error(data.error || 'Failed to load tournaments this week')
      }
      const totalEvents = (data.days || []).reduce((s, d) => s + (d.events || []).length, 0)
      dlog('info', 'Calendar', `loadWeek done — ${(data.days || []).length} day groups, ${totalEvents} total events`)
      setWeekDays(data.days || [])
    } catch (err) {
      dlog('error', 'Calendar', `loadWeek failed: ${err.message}`)
      setWeekError(err instanceof Error ? err.message : 'Failed to load tournaments this week')
      setWeekDays([])
    } finally {
      setWeekLoading(false)
    }
  }

  useEffect(() => {
    loadDay(selectedDate)
  }, [selectedDate])

  useEffect(() => {
    loadWeek()
  }, [])

  function renderEventCard(eventCard, scope) {
    const expansionKey = cardExpansionKey(scope, eventCard.id)
    const isOpen = !!expanded[expansionKey]
    const isConcluded = !!eventCard.hasConcluded
    return (
      <a key={expansionKey} className={`event-card ${isOpen ? 'open' : ''}`} href={eventCard.eventLink} target="_blank" rel="noreferrer">
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
              toggleCard(scope, eventCard.id)
            }}
          >
            {isOpen ? '-' : '+'}
          </button>
        </div>
        <p className="meta-line">
          <span>Date:</span> {fmtDatePacific(eventCard.date)}
        </p>
        {isConcluded ? (
          <p className="meta-line">
            <span>Winner:</span> {eventCard.winner}
          </p>
        ) : null}

        {isOpen ? (
          <div className="expanded">
            {isConcluded ? (
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
            ) : null}

            <div className="registrants">
              <h4>{isConcluded ? 'Other Entrants' : 'Registered Entrants'}</h4>
              <div className="registrant-grid">
                {eventCard.randomRegistrants.map((name, idx) => (
                  <span key={`${eventCard.id}-${name}-${idx}`}>{name}</span>
                ))}
              </div>
              {eventCard.randomRegistrants.length === 0 ? <p className="empty">No entrants available.</p> : null}
            </div>
            <p className="entrant-count">Total entrants: {eventCard.entrantCount}</p>
          </div>
        ) : null}
      </a>
    )
  }

  return (
    <main className="calendar-page" aria-label="Calendar">
      <div className="content-grid calendar-content-grid">
        <section className="panel">
          <h2 className="panel-title">Calendar</h2>
          <div className="calendar-date-nav" role="group" aria-label="Select date">
            <button
              type="button"
              className="calendar-date-arrow"
              aria-label="Previous day"
              onClick={() => setSelectedDate((prev) => addCalendarDaysIso(prev, -1))}
            >
              ‹
            </button>
            <label className="calendar-date-label calendar-date-label--flex">
              Date
              <input
                type="date"
                value={selectedDate}
                onChange={(e) => {
                  setSelectedDate(e.target.value)
                }}
              />
            </label>
            <button
              type="button"
              className="calendar-date-arrow"
              aria-label="Next day"
              onClick={() => setSelectedDate((prev) => addCalendarDaysIso(prev, 1))}
            >
              ›
            </button>
          </div>
          {dayError ? <p className="error">{dayError}</p> : null}
          {dayLoading ? <p className="loading">Loading tournaments...</p> : null}
          {!dayLoading && dayEvents.length === 0 ? (
            <div className="calendar-empty-wrap">
              <p className="empty">No tournaments found.</p>
              <button
                type="button"
                className="calendar-refresh-btn"
                onClick={() => loadDay(selectedDate, { isRefresh: true })}
                disabled={dayRefreshing}
              >
                {dayRefreshing ? <span className="spinner" aria-hidden="true" /> : null}
                {dayRefreshing ? 'Refreshing...' : 'Refresh'}
              </button>
            </div>
          ) : null}
          <div className="event-list">
            {dayEvents.map((eventCard) => renderEventCard(eventCard, 'day'))}
          </div>
        </section>

        <section className="panel">
          <h2 className="panel-title">Tournaments This Week</h2>
          <p className="panel-subtitle">Sunday through Saturday, including completed and upcoming events.</p>
          {weekError ? <p className="error">{weekError}</p> : null}
          {weekLoading ? <p className="loading">Loading week tournaments...</p> : null}

          {!weekLoading && weekDays.length === 0 ? <p className="empty">No tournaments found this week.</p> : null}

          <div className="calendar-week-list">
            {weekDays.map((day) => (
              <div key={day.date} className="calendar-week-day-group">
                <h3 className="calendar-weekday-heading">{weekdayHeaderLabel(day)}</h3>
                <div className="event-list">
                  {day.events.map((eventCard) => renderEventCard(eventCard, `week-${day.date}`))}
                </div>
              </div>
            ))}
          </div>
        </section>
      </div>
    </main>
  )
}
