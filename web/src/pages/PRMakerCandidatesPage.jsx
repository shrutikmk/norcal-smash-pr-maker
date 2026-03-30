import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { useDebugLog } from '../debug/DebugContext.jsx'

const COLUMN_GROUPS = [
  { id: 'elo', label: 'ELO', columns: ['elo'] },
  { id: 'in_region', label: 'In-region record', columns: ['in_region_wins', 'in_region_losses', 'in_region_tournaments'] },
  { id: 'combined', label: 'Combined record', columns: ['wins', 'losses', 'total_sets'] },
  { id: 'h2h', label: 'H2H breakdown', columns: ['positive_h2h', 'even_h2h', 'negative_h2h'] },
  { id: 'attendance', label: 'Attendance / consistency', columns: ['tournaments_attended', 'loss_to_tournament_ratio'] },
  { id: 'oor_counts', label: 'Out-region counts', columns: ['out_region_wins', 'out_region_losses', 'out_region_tournaments'] },
  { id: 'oor_notable', label: 'Notable OOR', columns: ['notable_oor_wins', 'notable_oor_losses'] },
  { id: 'placement', label: 'Rank / Copeland', columns: ['rank', 'copeland_score'] },
]

function readPrMakerContextFromSessionStorage() {
  try {
    let best = null
    let bestTs = -1
    for (let i = 0; i < sessionStorage.length; i += 1) {
      const k = sessionStorage.key(i)
      if (!k || !k.startsWith('prMakerCandidates')) continue
      const raw = sessionStorage.getItem(k)
      if (!raw) continue
      const p = JSON.parse(raw)
      if (!p?.startDate || !p?.endDate || !Array.isArray(p.eventSlugs) || p.eventSlugs.length === 0) {
        continue
      }
      const ts = typeof p.savedAt === 'number' ? p.savedAt : 0
      if (ts >= bestTs) {
        bestTs = ts
        best = p
      }
    }
    if (best) {
      return {
        startDate: best.startDate,
        endDate: best.endDate,
        eventSlugs: best.eventSlugs,
      }
    }
  } catch { /* ignore */ }
  return null
}

function getContext(locationState) {
  if (locationState?.startDate && locationState?.endDate && locationState?.eventSlugs?.length) {
    return {
      startDate: locationState.startDate,
      endDate: locationState.endDate,
      eventSlugs: locationState.eventSlugs,
    }
  }
  return readPrMakerContextFromSessionStorage()
}

export default function PRMakerCandidatesPage() {
  const dlog = useDebugLog()
  const location = useLocation()
  const navigate = useNavigate()
  const ctx = useMemo(() => getContext(location.state), [location.state])

  const [players, setPlayers] = useState([])
  const [selected, setSelected] = useState(new Set())
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState('')

  const [mergeRules, setMergeRules] = useState([])
  const [mergeKeep, setMergeKeep] = useState('')
  const [mergeDrop, setMergeDrop] = useState('')
  const [mergeError, setMergeError] = useState('')

  const [minAttendance, setMinAttendance] = useState(1)
  const [showAttendancePopover, setShowAttendancePopover] = useState(false)
  const attendanceRef = useRef(null)

  const [warmStatus, setWarmStatus] = useState(null)
  const warmDebounceRef = useRef(null)
  const warmJobRef = useRef(null)
  const warmPollRef = useRef(null)

  const [csvLoading, setCsvLoading] = useState(false)
  const [csvError, setCsvError] = useState('')
  const [csvPhase, setCsvPhase] = useState('')
  const [exportTopX, setExportTopX] = useState('')
  const [exportMinAttendance, setExportMinAttendance] = useState(1)
  const [exportCols, setExportCols] = useState(() => COLUMN_GROUPS.map((g) => g.id))
  const csvWarmPollRef = useRef(null)

  const fetchCandidates = useCallback(
    async (rules) => {
      if (!ctx) return
      dlog('info', 'PRMaker/Candidates', `fetchCandidates — ${(rules || []).length} merge rules`)
      setLoading(true)
      setLoadError('')
      try {
        const res = await fetch('/api/pr-maker/candidates', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            start: ctx.startDate,
            end: ctx.endDate,
            eventSlugs: ctx.eventSlugs,
            mergeRules: rules,
          }),
        })
        const data = await res.json()
        if (!res.ok) throw new Error(data.error || 'Failed to load candidates')
        dlog('info', 'PRMaker/Candidates', `Loaded ${(data.players || []).length} candidates`)
        setPlayers(data.players || [])
      } catch (err) {
        dlog('error', 'PRMaker/Candidates', `fetchCandidates failed: ${err.message}`)
        setLoadError(err instanceof Error ? err.message : 'Load failed')
      } finally {
        setLoading(false)
      }
    },
    [ctx, dlog],
  )

  useEffect(() => {
    fetchCandidates(mergeRules)
  }, [fetchCandidates])

  useEffect(() => {
    function handleClickOutside(e) {
      if (attendanceRef.current && !attendanceRef.current.contains(e.target)) {
        setShowAttendancePopover(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const togglePlayer = useCallback((name) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }, [])

  const selectAllVisible = useCallback(() => {
    const vis = players.filter((p) => p.attendance >= minAttendance)
    setSelected(new Set(vis.map((p) => p.name)))
  }, [players, minAttendance])

  const deselectAll = useCallback(() => {
    setSelected(new Set())
  }, [])

  function handleMerge() {
    const keep = mergeKeep.trim()
    const drop = mergeDrop.trim()
    if (!keep || !drop) { setMergeError('Both fields are required.'); return }
    if (keep === drop) { setMergeError('Cannot merge a player into themselves.'); return }
    const allNames = new Set(players.map((p) => p.name))
    if (!allNames.has(drop)) { setMergeError(`"${drop}" not found in current players.`); return }
    dlog('info', 'PRMaker/Candidates', `Merge: "${drop}" → "${keep}"`)
    setMergeError('')
    const next = [...mergeRules, { keep, drop }]
    setMergeRules(next)
    setMergeKeep('')
    setMergeDrop('')
    setSelected(new Set())
    fetchCandidates(next)
  }

  function undoLastMerge() {
    if (mergeRules.length === 0) return
    dlog('info', 'PRMaker/Candidates', 'Undo last merge')
    const next = mergeRules.slice(0, -1)
    setMergeRules(next)
    setSelected(new Set())
    fetchCandidates(next)
  }

  const visible = useMemo(
    () => players.filter((p) => p.attendance >= minAttendance),
    [players, minAttendance],
  )

  const exportEligible = useMemo(
    () => players.filter((p) => p.attendance >= exportMinAttendance),
    [players, exportMinAttendance],
  )

  const selectedCount = useMemo(
    () => visible.filter((p) => selected.has(p.name)).length,
    [visible, selected],
  )

  const playerNames = useMemo(() => players.map((p) => p.name), [players])

  const selectedNames = useMemo(
    () => visible.filter((p) => selected.has(p.name)).map((p) => p.name),
    [visible, selected],
  )

  const fireWarm = useCallback((names) => {
    if (!ctx || names.length === 0) return
    dlog('info', 'PRMaker/Candidates', `fireWarm — warming OOR data for ${names.length} candidates`)
    const startWarm = async () => {
      try {
        const res = await fetch('/api/pr-maker/oor-warm/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            start: ctx.startDate, end: ctx.endDate,
            eventSlugs: ctx.eventSlugs, mergeRules,
            names,
          }),
        })
        const data = await res.json()
        if (!res.ok || !data.jobId) return
        warmJobRef.current = data.jobId
        setWarmStatus({ completed: 0, total: names.length, status: 'running' })
        if (warmPollRef.current) clearInterval(warmPollRef.current)
        warmPollRef.current = setInterval(async () => {
          try {
            const sr = await fetch(`/api/pr-maker/oor-warm/status?jobId=${data.jobId}`)
            const sd = await sr.json()
            dlog('info', 'PRMaker/Candidates', `Warm poll — ${sd.status}, ${sd.completed || 0}/${sd.total || '?'}${sd.currentPlayer ? ' · ' + sd.currentPlayer : ''}`)
            setWarmStatus(sd)
            if (sd.status === 'done' || sd.status === 'error') {
              clearInterval(warmPollRef.current)
              warmPollRef.current = null
            }
          } catch { /* ignore poll errors */ }
        }, 1500)
      } catch { /* ignore warm start errors */ }
    }
    startWarm()
  }, [ctx, mergeRules, dlog])

  useEffect(() => {
    if (warmDebounceRef.current) clearTimeout(warmDebounceRef.current)
    if (selectedNames.length === 0) {
      setWarmStatus(null)
      return
    }
    warmDebounceRef.current = setTimeout(() => {
      fireWarm(selectedNames)
    }, 600)
    return () => {
      if (warmDebounceRef.current) clearTimeout(warmDebounceRef.current)
    }
  }, [selectedNames.join(','), fireWarm])

  useEffect(() => {
    return () => {
      if (warmPollRef.current) clearInterval(warmPollRef.current)
      if (csvWarmPollRef.current) clearInterval(csvWarmPollRef.current)
    }
  }, [])

  function toggleExportCol(groupId) {
    setExportCols((prev) =>
      prev.includes(groupId) ? prev.filter((id) => id !== groupId) : [...prev, groupId],
    )
  }

  function getExportNames() {
    const sorted = [...exportEligible].sort(
      (a, b) => b.elo - a.elo || a.name.localeCompare(b.name),
    )
    const n = parseInt(exportTopX, 10)
    if (n > 0 && n < sorted.length) return sorted.slice(0, n)
    return sorted
  }

  async function handleExportCsv() {
    if (!ctx || exportEligible.length === 0) return
    const exportPlayers = getExportNames()
    const exportNames = exportPlayers.map((p) => p.name)
    const selectedColumns = ['player', ...COLUMN_GROUPS.filter((g) => exportCols.includes(g.id)).flatMap((g) => g.columns)]
    const t0 = performance.now()
    dlog('info', 'PRMaker/Candidates', `CSV export: ${exportNames.length} players, warming OOR first`)
    setCsvLoading(true)
    setCsvError('')
    setCsvPhase(`Warming OOR data for ${exportNames.length} players…`)
    try {
      const warmRes = await fetch('/api/pr-maker/oor-warm/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          start: ctx.startDate, end: ctx.endDate,
          eventSlugs: ctx.eventSlugs, mergeRules,
          names: exportNames,
        }),
      })
      const warmData = await warmRes.json()
      if (warmRes.ok && warmData.jobId) {
        await new Promise((resolve) => {
          if (csvWarmPollRef.current) clearInterval(csvWarmPollRef.current)
          csvWarmPollRef.current = setInterval(async () => {
            try {
              const sr = await fetch(`/api/pr-maker/oor-warm/status?jobId=${warmData.jobId}`)
              const sd = await sr.json()
              dlog('info', 'PRMaker/Candidates', `CSV warm poll — ${sd.status}, ${sd.completed || 0}/${sd.total || '?'}${sd.currentPlayer ? ' · ' + sd.currentPlayer : ''}`)
              setCsvPhase(`Warming OOR data (${sd.completed || 0}/${sd.total || '?'})${sd.currentPlayer ? ' · ' + sd.currentPlayer : ''}`)
              if (sd.status === 'done' || sd.status === 'error') {
                clearInterval(csvWarmPollRef.current)
                csvWarmPollRef.current = null
                if (sd.status === 'error') dlog('warn', 'PRMaker/Candidates', 'CSV warm job ended with error — proceeding with best-effort cache')
                resolve()
              }
            } catch {
              clearInterval(csvWarmPollRef.current)
              csvWarmPollRef.current = null
              resolve()
            }
          }, 1500)
        })
      } else {
        dlog('warn', 'PRMaker/Candidates', 'CSV warm start failed — proceeding with best-effort cache')
      }

      setCsvPhase('Generating CSV…')
      dlog('info', 'PRMaker/Candidates', 'CSV export: calling candidates-export')
      const res = await fetch('/api/pr-maker/candidates-export', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          start: ctx.startDate,
          end: ctx.endDate,
          eventSlugs: ctx.eventSlugs,
          mergeRules,
          names: exportNames,
          columns: selectedColumns,
        }),
      })
      const data = await res.json()
      if (!res.ok) {
        setCsvError(data.error || 'Export failed')
        return
      }
      const blob = new Blob([data.csv], { type: 'text/csv;charset=utf-8;' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = 'norcal-pr-candidates-export.csv'
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
      const elapsed = ((performance.now() - t0) / 1000).toFixed(1)
      dlog('info', 'PRMaker/Candidates', `CSV export: done in ${elapsed}s`)
    } catch (err) {
      setCsvError(`Error: ${err.message}`)
    } finally {
      setCsvLoading(false)
      setCsvPhase('')
      if (csvWarmPollRef.current) { clearInterval(csvWarmPollRef.current); csvWarmPollRef.current = null }
    }
  }

  function handleProceedToRanking() {
    const names = visible.filter((p) => selected.has(p.name)).map((p) => p.name)
    dlog('info', 'PRMaker/Candidates', `Proceeding to ranking with ${names.length} candidates: ${names.join(', ')}`)
    fireWarm(names)
    const payload = { startDate: ctx?.startDate, endDate: ctx?.endDate, eventSlugs: ctx?.eventSlugs, selectedNames: names, mergeRules }
    try { sessionStorage.setItem('prMakerRankingContext', JSON.stringify(payload)) } catch {}
    navigate('/pr-maker/ranking', { state: payload })
  }

  if (!ctx) {
    return (
      <main className="process-page" aria-label="PR Maker — Candidates">
        <div className="process-page-inner">
          <h2 className="panel-title">PR Maker</h2>
          <p className="process-subtitle" style={{ marginTop: 12 }}>
            No context found. Please go through the scrape and ingest flow first.
          </p>
          <Link to="/pr-maker" className="pr-maker-back-link">← Back to PR Maker</Link>
        </div>
      </main>
    )
  }

  return (
    <>
      <main className="process-page" aria-label="PR Maker — Candidate Selection">
        <div className="process-page-inner">
          <header className="process-header">
            <h2 className="panel-title">PR Maker</h2>
            <p className="process-subtitle">Candidate Selection</p>
            <p className="candidates-scope-hint" title="Matches POST /api/pr-maker/candidates eventSlugs — attendance and ELO use only these events.">
              ELO / attendance scope:{' '}
              <strong>{ctx.eventSlugs.length}</strong> event{ctx.eventSlugs.length === 1 ? '' : 's'} selected for ingest
              {' · '}
              {ctx.startDate} — {ctx.endDate}
            </p>
          </header>

          {loading && players.length === 0 ? (
            <div className="process-loading">
              <span className="spinner" aria-hidden="true" />
              Loading candidates...
            </div>
          ) : loadError ? (
            <div className="process-error">
              <p className="error">{loadError}</p>
              <Link to="/pr-maker" className="pr-maker-back-link">← Back to PR Maker</Link>
            </div>
          ) : (
            <div className="candidates-grid">
              <section className="candidates-table-section">
                <div className="process-events-toolbar">
                  <span className="process-events-count">
                    {visible.length} player{visible.length === 1 ? '' : 's'} · {selectedCount} selected
                  </span>
                  <div className="process-events-bulk">
                    <button type="button" className="process-bulk-btn" onClick={selectAllVisible}>Select All</button>
                    <button type="button" className="process-bulk-btn" onClick={deselectAll}>Deselect All</button>
                  </div>
                </div>

                <div className="candidates-table-wrap">
                  <table className="candidates-table">
                    <thead>
                      <tr>
                        <th className="candidates-th candidates-th--cb" />
                        <th className="candidates-th">Name</th>
                        <th className="candidates-th candidates-th--att" ref={attendanceRef}>
                          <button
                            type="button"
                            className="candidates-att-btn"
                            title="Distinct NorCal tournaments in the selected season (start.gg tournament ID). Multiple brackets at the same tournament count once."
                            onClick={() => setShowAttendancePopover((v) => !v)}
                          >
                            Attendance
                            <span className="candidates-att-icon" aria-hidden="true">▾</span>
                          </button>
                          {showAttendancePopover ? (
                            <div className="candidates-att-popover">
                              <p className="candidates-att-popover-hint">
                                Counts distinct in-region tournaments only (not each bracket). Singles and doubles at the same local count as one.
                              </p>
                              <label className="candidates-att-popover-label">
                                Minimum tournaments
                                <input
                                  type="number"
                                  className="candidates-att-popover-input"
                                  min={1}
                                  value={minAttendance}
                                  onChange={(e) => {
                                    const v = Math.max(1, parseInt(e.target.value, 10) || 1)
                                    setMinAttendance(v)
                                  }}
                                />
                              </label>
                            </div>
                          ) : null}
                        </th>
                        <th className="candidates-th candidates-th--elo">ELO</th>
                      </tr>
                    </thead>
                    <tbody>
                      {visible.map((p) => {
                        const checked = selected.has(p.name)
                        return (
                          <tr key={p.name} className={`candidates-row${checked ? ' candidates-row--selected' : ''}`}>
                            <td className="candidates-td candidates-td--cb">
                              <label className="process-event-checkbox-wrap">
                                <input
                                  type="checkbox"
                                  className="process-event-checkbox"
                                  checked={checked}
                                  onChange={() => togglePlayer(p.name)}
                                />
                                <span className="process-checkbox-visual" aria-hidden="true" />
                              </label>
                            </td>
                            <td className="candidates-td candidates-td--name">{p.name}</td>
                            <td className="candidates-td candidates-td--num">{p.attendance}</td>
                            <td className="candidates-td candidates-td--num">{p.elo.toFixed(0)}</td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </section>

              <div className="candidates-sidebar-stack">
                <aside className="candidates-merge-section" aria-label="Merge player profiles">
                  <h3 className="process-filters-title">Merge</h3>
                  <p className="process-filters-hint">
                    Combine two player profiles. The merged player keeps the first name.
                  </p>

                  <div className="candidates-merge-form">
                    <label className="candidates-merge-label">
                      Keep name
                      <input
                        type="text"
                        className="candidates-merge-input"
                        list="candidates-keep-list"
                        value={mergeKeep}
                        onChange={(e) => setMergeKeep(e.target.value)}
                        placeholder="Player A"
                      />
                      <datalist id="candidates-keep-list">
                        {playerNames.map((n) => <option key={n} value={n} />)}
                      </datalist>
                    </label>
                    <label className="candidates-merge-label">
                      Merge into (drop)
                      <input
                        type="text"
                        className="candidates-merge-input"
                        list="candidates-drop-list"
                        value={mergeDrop}
                        onChange={(e) => setMergeDrop(e.target.value)}
                        placeholder="Player B"
                      />
                      <datalist id="candidates-drop-list">
                        {playerNames.map((n) => <option key={n} value={n} />)}
                      </datalist>
                    </label>
                    {mergeError ? <p className="error" style={{ fontSize: '0.82rem', margin: '4px 0 0' }}>{mergeError}</p> : null}
                    <button
                      type="button"
                      className="candidates-merge-btn"
                      onClick={handleMerge}
                      disabled={loading}
                    >
                      Merge
                    </button>
                  </div>

                  {mergeRules.length > 0 ? (
                    <div className="candidates-merge-history">
                      <h4 className="candidates-merge-history-title">Applied merges</h4>
                      <ul className="candidates-merge-history-list">
                        {mergeRules.map((r, i) => (
                          <li key={i} className="candidates-merge-history-item">
                            <span className="candidates-merge-history-drop">{r.drop}</span>
                            <span className="candidates-merge-history-arrow">→</span>
                            <span className="candidates-merge-history-keep">{r.keep}</span>
                          </li>
                        ))}
                      </ul>
                      <button
                        type="button"
                        className="process-bulk-btn"
                        onClick={undoLastMerge}
                        disabled={loading}
                      >
                        Undo last merge
                      </button>
                    </div>
                  ) : null}
                </aside>

                <aside className="candidates-export-panel" aria-label="Export contender datasheet">
                  <h3 className="process-filters-title">Export datasheet</h3>
                  <p className="process-filters-hint">
                    Generate a CSV with derived statistics for contender evaluation.
                  </p>

                  <div className="candidates-export-fields">
                    <label className="candidates-merge-label">
                      Minimum tournaments
                      <input
                        type="number"
                        className="candidates-merge-input"
                        min={1}
                        value={exportMinAttendance}
                        onChange={(e) => {
                          const v = Math.max(1, parseInt(e.target.value, 10) || 1)
                          setExportMinAttendance(v)
                        }}
                      />
                    </label>
                    <label className="candidates-merge-label">
                      Top X contenders
                      <input
                        type="number"
                        className="candidates-merge-input"
                        min={0}
                        placeholder={`All (${exportEligible.length})`}
                        value={exportTopX}
                        onChange={(e) => setExportTopX(e.target.value)}
                      />
                    </label>
                  </div>

                  <fieldset className="candidates-export-cols">
                    <legend className="candidates-export-cols-legend">Columns</legend>
                    {COLUMN_GROUPS.map((g) => (
                      <label key={g.id} className="candidates-export-col-label">
                        <input
                          type="checkbox"
                          className="candidates-export-col-cb"
                          checked={exportCols.includes(g.id)}
                          onChange={() => toggleExportCol(g.id)}
                        />
                        {g.label}
                      </label>
                    ))}
                  </fieldset>

                  {csvPhase ? <p className="candidates-export-phase">{csvPhase}</p> : null}
                  {csvError ? <p className="candidates-csv-error">{csvError}</p> : null}

                  <button
                    type="button"
                    className="candidates-merge-btn"
                    disabled={exportEligible.length === 0 || csvLoading}
                    onClick={handleExportCsv}
                  >
                    {csvLoading ? 'Generating…' : 'Generate CSV'}
                  </button>
                </aside>
              </div>
            </div>
          )}
        </div>
      </main>

      {players.length > 0 ? (
        <>
          <div className="process-bottom-fade" aria-hidden="true" />
          <div className="process-bottom-bar">
            {warmStatus && warmStatus.status === 'running' && warmStatus.total > 0 ? (
              <span className="candidates-warm-status">
                Preparing comparison data ({warmStatus.completed}/{warmStatus.total})
              </span>
            ) : warmStatus && warmStatus.status === 'done' ? (
              <span className="candidates-warm-status candidates-warm-status--done">
                Comparison data ready
              </span>
            ) : null}
            <button
              type="button"
              className="process-ingest-btn"
              disabled={selectedCount === 0}
              onClick={handleProceedToRanking}
            >
              Continue to comparisons — {selectedCount} Candidate{selectedCount === 1 ? '' : 's'}
            </button>
          </div>
        </>
      ) : null}
    </>
  )
}
