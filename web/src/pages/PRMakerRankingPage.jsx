import { useState, useEffect, useMemo, useCallback, useRef } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { useDebugLog } from '../debug/DebugContext.jsx'

function shuffleArray(arr) {
  const a = [...arr]
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[a[i], a[j]] = [a[j], a[i]]
  }
  return a
}

/** Worst-case comparisons to build a total order via binary insertion (O(n log n)). */
function maxComparisonsUpperBound(n) {
  if (n <= 1) return 0
  let s = 0
  for (let k = 1; k < n; k++) {
    s += Math.ceil(Math.log2(k + 1))
  }
  return s
}

function buildInitialInsert(names) {
  const shuffled = shuffleArray([...names])
  if (shuffled.length === 0) return { ranking: [], pool: [], active: null }
  if (shuffled.length === 1) return { ranking: [...shuffled], pool: [], active: null }
  const ranking = [shuffled[0]]
  const pool = shuffled.slice(1)
  return {
    ranking,
    pool,
    active: { player: pool[0], lo: 0, hi: ranking.length },
  }
}

function namesFingerprint(names) {
  return JSON.stringify([...names].sort())
}

function orderedFromRankingAndEdges(ranking, edges, allNames) {
  const wins = {}
  for (const n of allNames) wins[n] = 0
  for (const e of edges) {
    if (wins[e.winner] !== undefined) wins[e.winner]++
  }
  return ranking.map((name) => ({ name, score: wins[name] ?? 0 }))
}

const STORAGE_INSERT = 'prMakerCompareInsert'
const STORAGE_EDGES = 'prMakerCompareEdges'
const STORAGE_ALGO = 'prMakerCompareAlgo'
const STORAGE_NAMES_FP = 'prMakerCompareNamesFp'

function loadJson(key) {
  try { return JSON.parse(sessionStorage.getItem(key)) } catch { return null }
}
function saveJson(key, val) {
  try { sessionStorage.setItem(key, JSON.stringify(val)) } catch {}
}

function humanOorStreamLine(evt) {
  if (!evt || typeof evt !== 'object') return String(evt)
  if (evt.type === 'done') return evt.message || 'OOR load complete.'
  if (evt.type === 'error') return `Error: ${evt.message || 'unknown'}`

  const d = evt.detail && typeof evt.detail === 'object' ? evt.detail : null
  const phase = evt.phase || d?.phase || ''
  const p = evt.player ? ` — ${evt.player}` : ''

  if (phase === 'player_cache_check') return evt.message || `Checking cache${p}…`
  if (phase === 'set_history_cache_hit') {
    const msg = evt.message || d?.message
    if (msg) return msg
    const n = evt.nodes ?? d?.set_nodes ?? '?'
    return `Set history CACHE HIT${p}: ${n} set node(s) cached`
  }
  if (phase === 'set_history_cache_miss') return evt.message || d?.message || `Set history CACHE MISS${p}: fetching from Start.gg…`
  if (phase === 'set_history_stored') return evt.message || d?.message || `Set history stored${p}`
  if (phase === 'set_history_page' && (evt.page != null || d?.page != null)) {
    const pg = evt.page ?? d?.page
    const tot = evt.totalPages ?? d?.totalPages ?? '?'
    const n = evt.nodesThisPage ?? d?.nodesThisPage ?? 0
    return `Set history: page ${pg} of ${tot} (${n} sets this page)`
  }
  if (phase === 'oor_tournaments_discovered') {
    const msg = evt.message || d?.message
    if (msg) return msg
    const ct = d?.count ?? '?'
    return `Discovered ${ct} OOR tournament(s) in date range${p}`
  }
  if (phase === 'oor_tournament_cache_hit') return evt.message || d?.message || `Tournament CACHE HIT${p}`
  if (phase === 'oor_tournament_cache_miss') return evt.message || d?.message || `Tournament CACHE MISS${p}`
  if (phase === 'oor_tournament_processed') return evt.message || d?.message || `Tournament processed${p}`
  if (phase === 'oor_tournament_summary') return evt.message || d?.message || `OOR tournament summary${p}`

  if (evt.message) return evt.message
  if (d?.message) return d.message
  return phase ? `${phase}${p}` : JSON.stringify(evt)
}

/** Streaming POST /api/pr-maker/player-oor with ``stream: true``; updates UI line + debug heartbeat between server lines. */
async function streamPlayerOor(shared, player, cancelId, signal, { dlog, setPhaseLine }) {
  const payload = { ...shared, player, cancelId, stream: true }
  let lastServerAt = Date.now()
  let lastLine = `Connecting OOR stream for ${player}…`
  setPhaseLine(lastLine)
  const heartbeatMs = 2800
  const hb = window.setInterval(() => {
    const gapSec = ((Date.now() - lastServerAt) / 1000).toFixed(0)
    dlog(
      'info',
      'PRMaker/Ranking',
      `Heartbeat: still working on ${player}`,
      `${lastLine} · ${gapSec}s since last server progress line (request still open)`,
    )
  }, heartbeatMs)
  try {
    const res = await fetch('/api/pr-maker/player-oor', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal,
    })
    if (!res.ok) {
      const t = await res.text()
      throw new Error(t || res.statusText)
    }
    const reader = res.body.getReader()
    const dec = new TextDecoder()
    let buf = ''
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      buf += dec.decode(value, { stream: true })
      let nl
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl).trim()
        buf = buf.slice(nl + 1)
        if (!line) continue
        let evt
        try {
          evt = JSON.parse(line)
        } catch {
          continue
        }
        lastServerAt = Date.now()
        lastLine = humanOorStreamLine(evt)
        setPhaseLine(lastLine)
        const extra =
          evt.phase === 'set_history_page' && evt.page != null
            ? `page ${evt.page}/${evt.totalPages ?? '?'}`
            : evt.detail && typeof evt.detail === 'object' && evt.detail.event_slug
              ? String(evt.detail.event_slug)
              : ''
        dlog('info', 'PRMaker/Ranking', `OOR stream ← ${lastLine}`, extra)
        if (evt.type === 'error') throw new Error(evt.message || 'OOR stream error')
        if (evt.type === 'done') return
      }
    }
    dlog('warn', 'PRMaker/Ranking', `OOR stream for ${player} closed without a done line`)
  } finally {
    window.clearInterval(hb)
  }
}

export default function PRMakerRankingPage() {
  const dlog = useDebugLog()
  const location = useLocation()
  const navigate = useNavigate()
  const ctx = useMemo(() => {
    const s = location.state
    if (s && Array.isArray(s.selectedNames) && s.selectedNames.length >= 2) return s
    try {
      const raw = JSON.parse(sessionStorage.getItem('prMakerRankingContext'))
      if (raw && Array.isArray(raw.selectedNames) && raw.selectedNames.length >= 2) return raw
    } catch {}
    return null
  }, [location.state])

  const [insertState, setInsertState] = useState(null)
  const [edges, setEdges] = useState(() => loadJson(STORAGE_EDGES) || [])

  useEffect(() => {
    if (!ctx) return
    const fp = namesFingerprint(ctx.selectedNames)
    const savedFp = loadJson(STORAGE_NAMES_FP)
    const saved = loadJson(STORAGE_INSERT)
    const algo = loadJson(STORAGE_ALGO)
    if (saved && algo === 'v2' && savedFp === fp && saved.ranking && Array.isArray(saved.pool)) {
      setInsertState(saved)
      const e = loadJson(STORAGE_EDGES)
      if (Array.isArray(e)) setEdges(e)
      return
    }
    const st = buildInitialInsert(ctx.selectedNames)
    setInsertState(st)
    setEdges([])
    saveJson(STORAGE_INSERT, st)
    saveJson(STORAGE_EDGES, [])
    saveJson(STORAGE_ALGO, 'v2')
    saveJson(STORAGE_NAMES_FP, fp)
  }, [ctx])

  const n = ctx?.selectedNames?.length ?? 0
  const maxTotal = useMemo(() => maxComparisonsUpperBound(n), [n])
  const answered = edges.length

  const isDone = insertState && insertState.pool.length === 0 && insertState.active == null && n >= 1

  const [card, setCard] = useState(null)
  const [expanded, setExpanded] = useState(null)
  const [loading, setLoading] = useState(false)
  const [loadPhase, setLoadPhase] = useState('')
  const [oorProgressPct, setOorProgressPct] = useState(0)
  const [argText, setArgText] = useState('')
  const [argLoading, setArgLoading] = useState(false)
  const [argPanelOpen, setArgPanelOpen] = useState(false)
  const fetchCtrl = useRef(null)
  const cancelIdRef = useRef(null)

  const active = insertState?.active
  const ranking = insertState?.ranking ?? []
  const mid = active && ranking.length > 0
    ? Math.floor((active.lo + active.hi) / 2)
    : 0
  const pivot = active && ranking.length > 0 ? ranking[mid] : null
  const pairPlayerA = active?.player ?? null
  const pairPlayerB = pivot ?? null
  const currentPair = pairPlayerA && pairPlayerB ? [pairPlayerA, pairPlayerB] : null

  const fetchComparison = useCallback(async (pA, pB) => {
    if (fetchCtrl.current) fetchCtrl.current.abort()
    if (cancelIdRef.current) {
      fetch('/api/pr-maker/oor-cancel', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ cancelId: cancelIdRef.current }),
      }).catch(() => {})
    }
    const ctrl = new AbortController()
    fetchCtrl.current = ctrl
    const cid = `${pA}-${pB}-${Date.now()}`
    cancelIdRef.current = cid
    setLoading(true)
    setCard(null)
    setExpanded(null)
    setArgText('')
    setArgPanelOpen(false)
    setOorProgressPct(0)
    setLoadPhase('Loading in-region data (cached)…')
    dlog('info', 'PRMaker/Ranking', `New comparison pair: ${pA} vs ${pB}`)
    const shared = {
      start: ctx.startDate, end: ctx.endDate,
      eventSlugs: ctx.eventSlugs, mergeRules: ctx.mergeRules || [],
    }
    try {
      const res = await fetch('/api/pr-maker/comparison', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...shared, playerA: pA, playerB: pB, includeOOR: false }),
        signal: ctrl.signal,
      })
      const data = await res.json()
      if (ctrl.signal.aborted) return
      dlog('info', 'PRMaker/Ranking', `In-region comparison loaded for ${pA} vs ${pB}`)
      setCard(data.card || null)
      setExpanded(data.expanded || null)
      setLoading(false)

      setLoadPhase(`Fetching out-of-region data for ${pA}…`)
      setOorProgressPct(0)
      try {
        await streamPlayerOor(shared, pA, cid, ctrl.signal, {
          dlog,
          setPhaseLine: (msg) => setLoadPhase(`OOR (${pA}): ${msg}`),
        })
        if (ctrl.signal.aborted) return
        dlog('info', 'PRMaker/Ranking', `OOR for ${pA} done (50%) — stream finished`)
        setOorProgressPct(50)
        setLoadPhase(`Fetching out-of-region data for ${pB}…`)

        await streamPlayerOor(shared, pB, cid, ctrl.signal, {
          dlog,
          setPhaseLine: (msg) => setLoadPhase(`OOR (${pB}): ${msg}`),
        })
        if (ctrl.signal.aborted) return
        dlog('info', 'PRMaker/Ranking', `OOR for ${pB} done (100%) — loading OOR comparison`)
        setOorProgressPct(100)
        setLoadPhase('Loading comparison with out-of-region data…')

        const oorRes = await fetch('/api/pr-maker/comparison', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...shared, playerA: pA, playerB: pB, includeOOR: true }),
          signal: ctrl.signal,
        })
        const oorData = await oorRes.json()
        if (!ctrl.signal.aborted) {
          dlog('info', 'PRMaker/Ranking', `OOR comparison loaded for ${pA} vs ${pB}`)
          setCard(oorData.card || null)
          setExpanded(oorData.expanded || null)
        }
      } catch (oorErr) {
        if (oorErr.name !== 'AbortError') console.warn('OOR fetch failed (non-critical)', oorErr)
      }
    } catch (err) {
      if (err.name !== 'AbortError') console.error('comparison fetch failed', err)
    } finally {
      if (!ctrl.signal.aborted) {
        setLoading(false)
        setLoadPhase('')
        setOorProgressPct(0)
      }
    }
  }, [ctx, dlog])

  useEffect(() => {
    if (pairPlayerA && pairPlayerB && ctx) {
      fetchComparison(pairPlayerA, pairPlayerB)
    }
  }, [pairPlayerA, pairPlayerB, ctx, fetchComparison])

  function persistInsert(next) {
    setInsertState(next)
    saveJson(STORAGE_INSERT, next)
  }

  function recordComparison(insertingPlayerWins) {
    if (!insertState?.active || pivot == null) return
    const { player, lo, hi } = insertState.active
    dlog('info', 'PRMaker/Ranking', `Decision: ${insertingPlayerWins ? player + ' > ' + pivot : pivot + ' > ' + player} (lo=${lo}, hi=${hi}, mid=${mid})`)
    const newEdges = [...edges, {
      winner: insertingPlayerWins ? player : pivot,
      loser: insertingPlayerWins ? pivot : player,
    }]
    setEdges(newEdges)
    saveJson(STORAGE_EDGES, newEdges)

    let newLo = lo
    let newHi = hi
    if (insertingPlayerWins) {
      newHi = mid
    } else {
      newLo = mid + 1
    }

    if (newLo < newHi) {
      persistInsert({
        ...insertState,
        active: { player, lo: newLo, hi: newHi },
      })
      return
    }

    const newRanking = [...insertState.ranking]
    newRanking.splice(newLo, 0, player)
    const newPool = insertState.pool.slice(1)
    let newActive = null
    if (newPool.length > 0) {
      newActive = { player: newPool[0], lo: 0, hi: newRanking.length }
    }
    persistInsert({
      ranking: newRanking,
      pool: newPool,
      active: newActive,
    })
  }

  async function generateArgument() {
    if (!currentPair || !ctx) return
    dlog('info', 'PRMaker/Ranking', `Generating AI argument for ${currentPair[0]} vs ${currentPair[1]}`)
    setArgLoading(true)
    setArgText('')
    try {
      const res = await fetch('/api/pr-maker/comparison/argument', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          start: ctx.startDate, end: ctx.endDate,
          eventSlugs: ctx.eventSlugs, mergeRules: ctx.mergeRules || [],
          playerA: currentPair[0], playerB: currentPair[1],
          includeOOR: false,
        }),
      })
      const data = await res.json()
      dlog('info', 'PRMaker/Ranking', `AI argument received (${(data.text || '').length} chars)`)
      setArgText(data.text || data.error || 'No response.')
      setArgPanelOpen(true)
    } catch (err) {
      dlog('error', 'PRMaker/Ranking', `AI argument failed: ${err.message}`)
      setArgText(`Error: ${err.message}`)
      setArgPanelOpen(true)
    } finally {
      setArgLoading(false)
    }
  }

  function handleRestart() {
    if (!ctx) return
    const st = buildInitialInsert(ctx.selectedNames)
    setInsertState(st)
    setEdges([])
    saveJson(STORAGE_INSERT, st)
    saveJson(STORAGE_EDGES, [])
    saveJson(STORAGE_NAMES_FP, namesFingerprint(ctx.selectedNames))
    saveJson(STORAGE_ALGO, 'v2')
    setCard(null)
    setExpanded(null)
    setArgText('')
    setArgPanelOpen(false)
  }

  if (!ctx) {
    return (
      <main className="process-page" aria-label="PR Maker — Data Explorer">
        <div className="process-page-inner">
          <h2 className="panel-title">PR Maker</h2>
          <p className="process-subtitle" style={{ marginTop: 12 }}>
            No context found. Please go through the candidate selection first.
          </p>
          <Link to="/pr-maker/candidates" className="pr-maker-back-link">← Back to candidates</Link>
        </div>
      </main>
    )
  }

  function handleContinueToFinal() {
    const ordered = orderedFromRankingAndEdges(insertState.ranking, edges, ctx.selectedNames)
    dlog('info', 'PRMaker/Ranking', `Continue to final — ${ordered.length} players ranked`)
    const payload = { ordered, edges }
    try { sessionStorage.setItem('prMakerFinalSnapshot', JSON.stringify(payload)) } catch {}
    navigate('/pr-maker/final', { state: payload })
  }

  if (isDone) {
    const ordered = orderedFromRankingAndEdges(insertState.ranking, edges, ctx.selectedNames)
    return (
      <main className="process-page" aria-label="PR Maker — Results">
        <div className="process-page-inner">
          <h2 className="panel-title">PR Maker</h2>
          <p className="process-subtitle">Data Explorer: Complete</p>
          <p className="compare-done-note">
            Full ranking built in {answered} comparison{answered === 1 ? '' : 's'}
            {maxTotal > 0 ? ` (at most ${maxTotal} needed in worst case for ${n} players).` : '.'}
          </p>
          <div className="compare-results-table-wrap">
            <table className="compare-results-table">
              <thead>
                <tr><th>#</th><th>Player</th><th>Head-to-head wins</th></tr>
              </thead>
              <tbody>
                {ordered.map((s, i) => (
                  <tr key={s.name}>
                    <td>{i + 1}</td>
                    <td>{s.name}</td>
                    <td>{s.score}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="compare-done-actions">
            <button type="button" className="compare-btn compare-btn-a" onClick={handleContinueToFinal}>
              Continue to final list
            </button>
            <button type="button" className="compare-restart-btn" onClick={handleRestart}>Restart comparisons</button>
            <Link to="/pr-maker/candidates" className="pr-maker-back-link">← Back to candidates</Link>
          </div>
        </div>
      </main>
    )
  }

  if (!insertState || !currentPair) {
    return (
      <main className="process-page" aria-label="PR Maker — Loading">
        <div className="process-page-inner">
          <h2 className="panel-title">PR Maker</h2>
          <div className="compare-loading">
            <div className="compare-spinner" />
            <span>Preparing comparisons…</span>
          </div>
        </div>
      </main>
    )
  }

  const pA = currentPair[0]
  const pB = currentPair[1]
  const progressPct = maxTotal > 0 ? Math.min(100, (answered / maxTotal) * 100) : 0
  const allPairs = n >= 2 ? (n * (n - 1)) / 2 : 0

  return (
    <>
      <main className="process-page compare-page" aria-label="PR Maker — Data Explorer">
        <div className="process-page-inner compare-inner">
          <h2 className="panel-title">PR Maker</h2>
          <p className="process-subtitle">Comparison</p>

          <div className="compare-header-row">
            <span className="compare-player-name compare-player-a">{pA}</span>
            <span className="compare-vs">vs</span>
            <span className="compare-player-name compare-player-b">{pB}</span>
          </div>

          {loading && !card ? (
            <div className="compare-loading">
              <div className="compare-spinner" />
              <span>{loadPhase || 'Loading comparison data…'}</span>
            </div>
          ) : card ? (
            <>
              {loadPhase ? (
                <div className="compare-oor-loading-dock">
                  <p className="compare-oor-loading-text">{loadPhase}</p>
                  <div className="compare-oor-loading-track">
                    {oorProgressPct > 0 ? (
                      <div
                        className="compare-oor-loading-fill compare-oor-loading-fill--determinate"
                        style={{ width: `${oorProgressPct}%` }}
                      />
                    ) : (
                      <div className="compare-oor-loading-fill compare-oor-loading-fill--indeterminate" />
                    )}
                  </div>
                </div>
              ) : null}
              <ComparisonBody card={card} expanded={expanded} pA={pA} pB={pB} />
            </>
          ) : null}

          {argLoading ? (
            <div className="compare-arg-loading-dock" role="status" aria-live="polite">
              <p className="compare-arg-loading-title">Calling OpenAI (one request, in-region stats only)…</p>
              <div className="compare-arg-loading-track">
                <div className="compare-arg-loading-fill compare-arg-loading-fill--indeterminate" />
              </div>
            </div>
          ) : null}

          {argText ? (
            <div className={`compare-arg-collapse ${argPanelOpen ? 'compare-arg-collapse--open' : ''}`}>
              <button
                type="button"
                className="compare-arg-collapse-toggle"
                aria-expanded={argPanelOpen}
                onClick={() => setArgPanelOpen((o) => !o)}
              >
                <span className="compare-collapse-arrow">{argPanelOpen ? '▾' : '▸'}</span>
                AI argument
              </button>
              {argPanelOpen ? (
                <div className="compare-arg-panel compare-arg-panel--embedded">
                  <div className="compare-arg-text">{argText}</div>
                </div>
              ) : null}
            </div>
          ) : null}

          <div className="compare-action-row">
            <button
              type="button"
              className="compare-btn compare-btn-a"
              disabled={!card || argLoading}
              onClick={() => recordComparison(true)}
            >
              {pA} is better
            </button>
            <button
              type="button"
              className="compare-btn compare-btn-gen"
              disabled={!card || argLoading}
              onClick={generateArgument}
            >
              Generate argument
            </button>
            <button
              type="button"
              className="compare-btn compare-btn-b"
              disabled={!card || argLoading}
              onClick={() => recordComparison(false)}
            >
              {pB} is better
            </button>
          </div>
        </div>
      </main>

      <div className="process-bottom-fade" aria-hidden="true" />
      <div className="process-bottom-bar compare-bottom-bar">
        <div className="compare-progress-wrap compare-progress-wrap--stacked">
          <span className="compare-progress-label">
            Comparison {answered + 1} of at most {maxTotal || 1}
          </span>
          <span className="compare-progress-sublabel">
            {answered} answered · comparing all pairs would need {allPairs} questions; this flow needs at most {maxTotal}
          </span>
          <div className="compare-progress-track">
            <div
              className="compare-progress-fill"
              style={{ width: `${progressPct}%` }}
            />
          </div>
        </div>
      </div>
    </>
  )
}

function StatRow({ label, valA, valB }) {
  return (
    <tr className="compare-stat-row">
      <td className="compare-stat-val compare-stat-val-a">{valA}</td>
      <td className="compare-stat-label">{label}</td>
      <td className="compare-stat-val compare-stat-val-b">{valB}</td>
    </tr>
  )
}

function ComparisonBody({ card, expanded, pA, pB }) {
  const inA = card.in_region_summary?.[pA] || {}
  const inB = card.in_region_summary?.[pB] || {}
  const outA = card.out_region_summary?.[pA] || {}
  const outB = card.out_region_summary?.[pB] || {}
  const h2h = card.head_to_head_in_region || {}
  const ltr = card.loss_to_tournament_ratio || {}
  const elo = card.elo || {}

  const h2hA = h2h[pA] ?? 0
  const h2hB = h2h[pB] ?? 0

  return (
    <div className="compare-body">
      <table className="compare-stat-table">
        <thead>
          <tr>
            <th className="compare-stat-header-a">{pA}</th>
            <th className="compare-stat-header-label">Stat</th>
            <th className="compare-stat-header-b">{pB}</th>
          </tr>
        </thead>
        <tbody>
          <StatRow label="ELO" valA={elo[pA]} valB={elo[pB]} />
          <StatRow label="In-Region H2H" valA={`${h2hA}–${h2hB}`} valB={`${h2hB}–${h2hA}`} />
          <StatRow
            label="In-Region W–L / T"
            valA={`${inA.wins ?? 0}–${inA.losses ?? 0} / ${inA.tournaments ?? 0}`}
            valB={`${inB.wins ?? 0}–${inB.losses ?? 0} / ${inB.tournaments ?? 0}`}
          />
          <StatRow
            label="Loss/Tournament Ratio"
            valA={ltr[pA] != null ? ltr[pA].toFixed(4) : '—'}
            valB={ltr[pB] != null ? ltr[pB].toFixed(4) : '—'}
          />
          <StatRow
            label="Out-Region W–L / T"
            valA={`${outA.wins ?? 0}–${outA.losses ?? 0} / ${outA.tournaments ?? 0}`}
            valB={`${outB.wins ?? 0}–${outB.losses ?? 0} / ${outB.tournaments ?? 0}`}
          />
          {expanded?.tournamentsBothAttended != null ? (
            <StatRow
              label="Tournaments Both Attended"
              valA={expanded.tournamentsBothAttended.length}
              valB={expanded.tournamentsBothAttended.length}
            />
          ) : null}
        </tbody>
      </table>

      {expanded?.sharedWins?.length > 0 ? (
        <CollapsibleSection title={`Common opponents both beat (${expanded.sharedWins.length})`} defaultOpen>
          <table className="compare-opp-table">
            <thead>
              <tr><th>Opponent</th><th>{pA} W–L</th><th>{pB} W–L</th><th>Opp ELO</th></tr>
            </thead>
            <tbody>
              {expanded.sharedWins.map(r => (
                <tr key={r.opponent}>
                  <td>{r.opponent}</td>
                  <td>{r.p1Wins}–{r.p1Losses}</td>
                  <td>{r.p2Wins}–{r.p2Losses}</td>
                  <td>{r.oppElo}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </CollapsibleSection>
      ) : null}

      {expanded?.sharedLosses?.length > 0 ? (
        <CollapsibleSection title={`Common opponents both lost to (${expanded.sharedLosses.length})`} defaultOpen>
          <table className="compare-opp-table">
            <thead>
              <tr><th>Opponent</th><th>{pA} W–L</th><th>{pB} W–L</th><th>Opp ELO</th></tr>
            </thead>
            <tbody>
              {expanded.sharedLosses.map(r => (
                <tr key={r.opponent}>
                  <td>{r.opponent}</td>
                  <td>{r.p1Wins}–{r.p1Losses}</td>
                  <td>{r.p2Wins}–{r.p2Losses}</td>
                  <td>{r.oppElo}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </CollapsibleSection>
      ) : null}

      {expanded?.tournamentsBothAttended?.length > 0 ? (
        <CollapsibleSection title={`Tournaments both attended (${expanded.tournamentsBothAttended.length})`} defaultOpen>
          <div className="compare-tourney-list">
            {expanded.tournamentsBothAttended.map((t, i) => (
              <div key={i} className="compare-tourney-row">
                <div className="compare-tourney-name">{t.name}</div>
                <div className="compare-tourney-detail">
                  <span className="compare-player-a">{pA}</span>: {t.p1Place != null ? `#${t.p1Place}` : '—'} · {t.p1WL} · {t.p1Events} bracket{t.p1Events !== 1 ? 's' : ''}
                </div>
                <div className="compare-tourney-detail">
                  <span className="compare-player-b">{pB}</span>: {t.p2Place != null ? `#${t.p2Place}` : '—'} · {t.p2WL} · {t.p2Events} bracket{t.p2Events !== 1 ? 's' : ''}
                </div>
              </div>
            ))}
          </div>
        </CollapsibleSection>
      ) : null}

      <div className="compare-unique-wins-grid">
        <UniqueWinsCol label={`${pA} unique wins`} items={expanded?.p1UniqueWins} playerClass="compare-player-a" />
        <UniqueWinsCol label={`${pB} unique wins`} items={expanded?.p2UniqueWins} playerClass="compare-player-b" />
      </div>

      <div className="compare-shared-lists-grid">
        <TagListCol title="Shared wins" items={card.shared_wins} />
        <TagListCol title="Shared losses" items={card.shared_losses} />
        <TagListCol title={`${pA} unique losses`} items={card.unique_losses?.[pA]} />
        <TagListCol title={`${pB} unique losses`} items={card.unique_losses?.[pB]} />
      </div>

      {(outA.notable_wins?.length > 0 || outB.notable_wins?.length > 0 || outA.notable_losses?.length > 0 || outB.notable_losses?.length > 0) ? (
        <CollapsibleSection title="Out-of-Region Notable Results">
          <div className="compare-oor-grid">
            <OorCol label={`${pA} notable OOR wins`} items={outA.notable_wins} />
            <OorCol label={`${pB} notable OOR wins`} items={outB.notable_wins} />
            <OorCol label={`${pA} notable OOR losses`} items={outA.notable_losses} />
            <OorCol label={`${pB} notable OOR losses`} items={outB.notable_losses} />
          </div>
        </CollapsibleSection>
      ) : null}
    </div>
  )
}

function CollapsibleSection({ title, defaultOpen = false, children }) {
  const [open, setOpen] = useState(defaultOpen)
  return (
    <div className={`compare-collapse ${open ? 'compare-collapse--open' : ''}`}>
      <button
        type="button"
        className="compare-collapse-toggle"
        aria-expanded={open}
        onClick={() => setOpen(o => !o)}
      >
        <span className="compare-collapse-arrow">{open ? '▾' : '▸'}</span>
        {title}
      </button>
      {open ? <div className="compare-collapse-body">{children}</div> : null}
    </div>
  )
}

function UniqueWinsCol({ label, items, playerClass }) {
  if (!items || items.length === 0) return null
  return (
    <div className="compare-unique-col">
      <h4 className={`compare-unique-title ${playerClass}`}>{label}</h4>
      <table className="compare-opp-table compare-opp-table--sm">
        <thead><tr><th>Opponent</th><th>W–L</th><th>Opp ELO</th></tr></thead>
        <tbody>
          {items.map(r => (
            <tr key={r.opponent}>
              <td>{r.opponent}</td>
              <td>{r.wins}–{r.losses}</td>
              <td>{r.oppElo}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function TagListCol({ title, items }) {
  if (!items || items.length === 0) return null
  return (
    <div className="compare-list-col">
      <h4 className="compare-list-title">{title}</h4>
      <ul className="compare-list-items">
        {items.map((item, i) => {
          const name = Array.isArray(item) ? item[0] : String(item)
          const count = Array.isArray(item) ? item[1] : null
          return <li key={i}>{name}{count != null ? ` (${count})` : ''}</li>
        })}
      </ul>
    </div>
  )
}

function OorCol({ label, items }) {
  if (!items || items.length === 0) return null
  return (
    <div className="compare-oor-col">
      <h5 className="compare-oor-title">{label}</h5>
      <ul className="compare-oor-items">
        {items.map((name, i) => <li key={i}>{String(name)}</li>)}
      </ul>
    </div>
  )
}
