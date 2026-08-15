import { useState, useEffect, useMemo, useCallback, useRef, memo } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { useDebugLog } from '../debug/DebugContext.jsx'
import RankingListPanel from '../components/RankingListPanel.jsx'

// Module-level comparison cache: revisiting a pair (back nav, restart, refresh
// within the SPA session) renders instantly instead of refetching + restreaming.
// Keyed by context fingerprint + sorted pair + whether OOR was included.
// Bump COMPARISON_CACHE_VERSION when expanded payload shape changes so stale
// entries (e.g. missing tournamentsAttended) are not reused.
const COMPARISON_CACHE_VERSION = 3
const comparisonCache = new Map()
const COMPARISON_CACHE_MAX = 300

function comparisonCacheKey(ctx, pA, pB, withOor) {
  const pair = [pA, pB].sort().join('|')
  const ctxFp = JSON.stringify([
    ctx.startDate, ctx.endDate,
    [...(ctx.eventSlugs || [])].sort(),
    ctx.mergeRules || [],
  ])
  return `v${COMPARISON_CACHE_VERSION}|${ctxFp}|${pair}|oor:${withOor ? 1 : 0}`
}

/** True when cached/API expanded has the union tournament list (not legacy shared-only). */
function hasTournamentsAttendedPayload(expanded) {
  return Array.isArray(expanded?.tournamentsAttended)
}

function comparisonCachePut(key, value) {
  if (comparisonCache.size >= COMPARISON_CACHE_MAX) {
    comparisonCache.delete(comparisonCache.keys().next().value)
  }
  comparisonCache.set(key, value)
}

function shuffleArray(arr) {
  const a = [...arr]
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[a[i], a[j]] = [a[j], a[i]]
  }
  return a
}

/**
 * Worst-case comparisons for merge-insertion (Ford–Johnson) sort:
 * F(n) = Σ_{k=1..n} ceil(log2(3k/4)). This matches the information-theoretic
 * lower bound ceil(log2(n!)) for n ≤ 11 and n = 20..22 — i.e. it is the
 * provably minimal worst-case question count for typical tier sizes.
 */
function maxComparisonsUpperBound(n) {
  let s = 0
  for (let k = 1; k <= n; k++) {
    // ceil(log2(3k/4)) computed exactly: smallest b with 2^(b+2) >= 3k
    let bits = 0
    while (1 << (bits + 2) < 3 * k) bits++
    s += bits
  }
  return s
}

const NEEDS_ANSWER = Symbol('needsAnswer')
const INCONSISTENT_EDGES = Symbol('inconsistentEdges')

/**
 * Insertion order (indices into the pend array) for Ford–Johnson: elements are
 * inserted in groups bounded by Jacobsthal numbers (3, 5, 11, 21, …),
 * descending within each group, so every binary search spans 2^k − 1 elements
 * in the worst case.
 */
function pendInsertionOrder(m) {
  const order = []
  let prevLabel = 1
  let jPrev = 1
  let jCur = 3
  while (prevLabel < m + 1) {
    const boundary = Math.min(jCur, m + 1)
    for (let label = boundary; label > prevLabel; label--) order.push(label - 2)
    prevLabel = boundary
    const next = jCur + 2 * jPrev
    jPrev = jCur
    jCur = next
  }
  return order
}

/** Ford–Johnson merge-insertion sort, ascending by `less`. */
function fjSortAsc(items, less) {
  const n = items.length
  if (n <= 1) return [...items]
  const pairs = [] // [high, low] with high > low
  let straggler = null
  for (let i = 0; i + 1 < n; i += 2) {
    const a = items[i]
    const b = items[i + 1]
    if (less(a, b)) pairs.push([b, a])
    else pairs.push([a, b])
  }
  if (n % 2 === 1) straggler = items[n - 1]
  const sortedHighs = fjSortAsc(pairs.map((p) => p[0]), less)
  const lowOf = new Map(pairs)
  // The partner of the smallest sorted high is smaller than everything sorted
  // so far — it leads the chain for free, without a comparison.
  const chain = [lowOf.get(sortedHighs[0]), ...sortedHighs]
  const pend = sortedHighs.slice(1).map((h) => ({ item: lowOf.get(h), partner: h }))
  if (straggler != null) pend.push({ item: straggler, partner: null })
  for (const pi of pendInsertionOrder(pend.length)) {
    const { item, partner } = pend[pi]
    let lo = 0
    // item < partner, so the search never needs to look past the partner.
    let hi = partner == null ? chain.length : chain.indexOf(partner)
    while (lo < hi) {
      const mid = (lo + hi) >> 1
      if (less(item, chain[mid])) hi = mid
      else lo = mid + 1
    }
    chain.splice(lo, 0, item)
  }
  return chain
}

/**
 * Deterministically replay a tier's merge-insertion sort against the recorded
 * answers. Returns { done: true, ranking } (best first) once every needed
 * answer is recorded, or { done: false, nextPair: [a, b] } identifying the
 * next question to ask.
 */
function computeTierSort(order, tierEdges) {
  let i = 0
  let pendingPair = null
  const better = (x, y) => {
    if (i < tierEdges.length) {
      const e = tierEdges[i++]
      const matches = (e.winner === x && e.loser === y) || (e.winner === y && e.loser === x)
      if (!matches) throw INCONSISTENT_EDGES
      return e.winner === x
    }
    pendingPair = [x, y]
    throw NEEDS_ANSWER
  }
  try {
    const ascending = fjSortAsc(order, (a, b) => better(b, a))
    return { done: true, ranking: ascending.reverse() }
  } catch (err) {
    if (err === NEEDS_ANSWER) return { done: false, nextPair: pendingPair }
    throw err
  }
}

/**
 * Derive the entire comparison flow state (current tier, finished rankings,
 * next question) from the persisted shuffled orders + answer log.
 */
function computeCompareState(orders, edges) {
  const completedRankings = []
  for (let t = 0; t < orders.length; t++) {
    const tierSet = new Set(orders[t])
    const tierEdges = edges.filter((e) => tierSet.has(e.winner) || tierSet.has(e.loser))
    const res = computeTierSort(orders[t], tierEdges)
    if (!res.done) {
      return { done: false, tierIndex: t, completedRankings, nextPair: res.nextPair }
    }
    completedRankings.push(...res.ranking)
  }
  return { done: true, tierIndex: orders.length, completedRankings, nextPair: null }
}

function namesFingerprint(names) {
  return JSON.stringify([...names].sort())
}

function tiersFingerprint(ctx) {
  return JSON.stringify([namesFingerprint(ctx.selectedNames), ctx.tiers])
}

function validateTiers(ctx) {
  if (!ctx?.tiers || !Array.isArray(ctx.tiers) || ctx.tiers.length === 0) return false
  const selected = ctx.selectedNames || []
  if (selected.length === 0) return false
  const flat = ctx.tiers.flat()
  if (flat.length !== selected.length) return false
  const sortedFlat = [...flat].sort()
  const sortedSel = [...selected].sort()
  return sortedFlat.every((n, i) => n === sortedSel[i])
}

function orderedFromRankingAndEdges(ranking, edges, allNames) {
  const wins = {}
  for (const n of allNames) wins[n] = 0
  for (const e of edges) {
    if (wins[e.winner] !== undefined) wins[e.winner]++
  }
  return ranking.map((name) => ({ name, score: wins[name] ?? 0 }))
}

const STORAGE_TIER_COMPARE = 'prMakerTierCompare'
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
    const raw = s || loadJson('prMakerRankingContext')
    if (!raw || !Array.isArray(raw.selectedNames) || raw.selectedNames.length < 1) return null
    if (!validateTiers(raw)) return { ...raw, _needsTiering: true }
    return raw
  }, [location.state])

  useEffect(() => {
    if (ctx?._needsTiering) {
      navigate('/pr-maker/tiering', { state: ctx, replace: true })
    }
  }, [ctx, navigate])

  // Persisted: the shuffled per-tier orders + the ordered answer log. Everything
  // else (current tier, next question, final ranking) is replayed from these,
  // so the merge-insertion engine stays a pure function.
  const [orders, setOrders] = useState(null)
  const [edges, setEdges] = useState(() => loadJson(STORAGE_EDGES) || [])

  useEffect(() => {
    if (!ctx || ctx._needsTiering) return
    const fp = tiersFingerprint(ctx)
    const savedFp = loadJson(STORAGE_NAMES_FP)
    const savedOrders = loadJson(STORAGE_TIER_COMPARE)
    const algo = loadJson(STORAGE_ALGO)
    if (
      Array.isArray(savedOrders) &&
      algo === 'v4' &&
      savedFp === fp &&
      savedOrders.length === ctx.tiers.length
    ) {
      setOrders(savedOrders)
      const e = loadJson(STORAGE_EDGES)
      setEdges(Array.isArray(e) ? e : [])
      return
    }
    const fresh = ctx.tiers.map((tier) => shuffleArray(tier))
    setOrders(fresh)
    setEdges([])
    saveJson(STORAGE_TIER_COMPARE, fresh)
    saveJson(STORAGE_EDGES, [])
    saveJson(STORAGE_ALGO, 'v4')
    saveJson(STORAGE_NAMES_FP, fp)
  }, [ctx])

  const compareState = useMemo(() => {
    if (!orders) return null
    try {
      return computeCompareState(orders, edges)
    } catch (err) {
      if (err === INCONSISTENT_EDGES) return { corrupt: true }
      throw err
    }
  }, [orders, edges])

  // Saved answers that no longer replay cleanly (e.g. edited storage) can't be
  // trusted — restart with a fresh shuffle rather than asking garbage questions.
  useEffect(() => {
    if (!compareState?.corrupt) return
    dlog('warn', 'PRMaker/Ranking', 'Saved comparison answers do not match the replay — restarting comparisons')
    handleRestart()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [compareState])

  const tierIndex = compareState?.tierIndex ?? 0
  const tierCount = ctx?.tiers?.length ?? 0
  const n = ctx?.selectedNames?.length ?? 0
  const maxTotal = useMemo(
    () => (ctx?.tiers || []).reduce((s, t) => s + maxComparisonsUpperBound(t.length), 0),
    [ctx?.tiers],
  )
  const maxWithoutTiering = useMemo(() => maxComparisonsUpperBound(n), [n])
  const answered = edges.length

  const isDone = compareState?.done === true
  const fullRanking = compareState?.completedRankings ?? []

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

  const currentPair = compareState && !compareState.done && !compareState.corrupt
    ? compareState.nextPair
    : null
  const pairPlayerA = currentPair?.[0] ?? null
  const pairPlayerB = currentPair?.[1] ?? null

  const fetchComparison = useCallback(async (pA, pB) => {
    if (fetchCtrl.current) fetchCtrl.current.abort()
    if (cancelIdRef.current) {
      // Both parallel OOR streams: base id (player A) and "-b" suffix (player B).
      for (const cidToCancel of [cancelIdRef.current, `${cancelIdRef.current}-b`]) {
        fetch('/api/pr-maker/oor-cancel', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ cancelId: cidToCancel }),
        }).catch(() => {})
      }
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

    // Full cache hit (incl. OOR): render instantly, no network at all.
    const oorKey = comparisonCacheKey(ctx, pA, pB, true)
    const cachedFull = comparisonCache.get(oorKey)
    if (cachedFull && hasTournamentsAttendedPayload(cachedFull.expanded)) {
      dlog('info', 'PRMaker/Ranking', `Comparison cache hit (with OOR) for ${pA} vs ${pB} — no fetch`)
      setCard(cachedFull.card || null)
      setExpanded(cachedFull.expanded || null)
      setLoading(false)
      setLoadPhase('')
      return
    }
    if (cachedFull) {
      // Stale shape from before tournamentsAttended — drop and refetch.
      comparisonCache.delete(oorKey)
      dlog('info', 'PRMaker/Ranking', `Ignoring stale comparison cache for ${pA} vs ${pB} (missing tournamentsAttended)`)
    }

    try {
      // Phase 1: one round trip — in-region card + whatever OOR the server
      // already has cached (memory → SQLite → granular rebuild, no start.gg).
      // On a warm cache this is the *only* request for the whole comparison.
      const res = await fetch('/api/pr-maker/comparison', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...shared, playerA: pA, playerB: pB, includeOOR: true, oorCacheOnly: true }),
        signal: ctrl.signal,
      })
      const data = await res.json()
      if (ctrl.signal.aborted) return
      const missing = res.ok && Array.isArray(data.missingOOR) ? data.missingOOR : [pA, pB]
      setCard(data.card || null)
      setExpanded(data.expanded || null)
      setLoading(false)

      if (res.ok && missing.length === 0) {
        dlog('info', 'PRMaker/Ranking', `Comparison loaded with full OOR from server cache (1 round trip) for ${pA} vs ${pB}`)
        if (hasTournamentsAttendedPayload(data.expanded)) {
          comparisonCachePut(oorKey, { card: data.card, expanded: data.expanded })
        }
        setLoadPhase('')
        return
      }
      dlog('info', 'PRMaker/Ranking', `Comparison loaded; OOR still needed for: ${missing.join(', ')}`)

      // Phase 2: stream OOR only for the players the cache is missing (parallel).
      // The card above stays interactive — OOR fills in when ready.
      setLoadPhase(`Fetching out-of-region data for ${missing.join(' and ')}…`)
      setOorProgressPct(0)
      try {
        let oorDone = 0
        const trackDone = (player) => {
          oorDone += 1
          if (ctrl.signal.aborted) return
          setOorProgressPct(Math.round((oorDone / missing.length) * 100))
          dlog('info', 'PRMaker/Ranking', `OOR for ${player} done (${oorDone}/${missing.length})`)
        }
        await Promise.all(missing.map((player, idx) =>
          streamPlayerOor(shared, player, idx === 0 ? cid : `${cid}-b`, ctrl.signal, {
            dlog,
            setPhaseLine: (msg) => setLoadPhase(`OOR (${player}): ${msg}`),
          }).then(() => trackDone(player)),
        ))
        if (ctrl.signal.aborted) return
        setLoadPhase('Loading comparison with out-of-region data…')

        // Phase 3: cache-only again — everything is now cached server-side.
        const oorRes = await fetch('/api/pr-maker/comparison', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...shared, playerA: pA, playerB: pB, includeOOR: true, oorCacheOnly: true }),
          signal: ctrl.signal,
        })
        const oorData = await oorRes.json()
        if (!ctrl.signal.aborted) {
          dlog('info', 'PRMaker/Ranking', `OOR comparison loaded for ${pA} vs ${pB}`)
          if (
            oorRes.ok
            && (!oorData.missingOOR || oorData.missingOOR.length === 0)
            && hasTournamentsAttendedPayload(oorData.expanded)
          ) {
            comparisonCachePut(oorKey, { card: oorData.card, expanded: oorData.expanded })
          }
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

  const prevPairRef = useRef(null)
  useEffect(() => {
    if (!pairPlayerA || !pairPlayerB) return
    const pairKey = `${pairPlayerA}|${pairPlayerB}`
    if (prevPairRef.current && prevPairRef.current !== pairKey) {
      window.scrollTo({ top: 0, behavior: 'smooth' })
    }
    prevPairRef.current = pairKey
  }, [pairPlayerA, pairPlayerB])

  // Speculative prefetch: once the current card is settled, warm the two pairs
  // the user can land on next. The pairs come from replaying the engine with
  // each hypothetical answer appended, so this stays correct for any algorithm.
  // Cache-only requests never hit start.gg; any players still missing OOR get
  // a background warm job (resumable, prioritized).
  const prefetchedRef = useRef(new Set())
  useEffect(() => {
    if (!ctx || !card || loadPhase || !currentPair || !orders) return
    const [a, b] = currentPair
    const nextPairs = []
    for (const firstWins of [true, false]) {
      const hypothetical = [...edges, {
        winner: firstWins ? a : b,
        loser: firstWins ? b : a,
      }]
      try {
        const st = computeCompareState(orders, hypothetical)
        if (!st.done && st.nextPair) nextPairs.push(st.nextPair)
      } catch { /* inconsistent hypothetical state — skip prefetch */ }
    }
    if (nextPairs.length === 0) return

    const shared = {
      start: ctx.startDate, end: ctx.endDate,
      eventSlugs: ctx.eventSlugs, mergeRules: ctx.mergeRules || [],
    }
    const missingForWarm = new Set()
    const tasks = []
    for (const [npA, npB] of nextPairs) {
      const key = comparisonCacheKey(ctx, npA, npB, true)
      if (comparisonCache.has(key) || prefetchedRef.current.has(key)) continue
      prefetchedRef.current.add(key)
      tasks.push(
        fetch('/api/pr-maker/comparison', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...shared, playerA: npA, playerB: npB, includeOOR: true, oorCacheOnly: true }),
        })
          .then((r) => r.json().then((d) => ({ ok: r.ok, d })))
          .then(({ ok, d }) => {
            const miss = ok && Array.isArray(d.missingOOR) ? d.missingOOR : []
            if (ok && miss.length === 0 && hasTournamentsAttendedPayload(d.expanded)) {
              comparisonCachePut(key, { card: d.card, expanded: d.expanded })
              dlog('info', 'PRMaker/Ranking', `Prefetched next pair ${npA} vs ${npB} (full cache)`)
            } else {
              for (const m of miss) missingForWarm.add(m)
            }
          })
          .catch(() => {}),
      )
    }
    if (tasks.length === 0) return
    Promise.all(tasks).then(() => {
      if (missingForWarm.size === 0) return
      const names = [...missingForWarm]
      dlog('info', 'PRMaker/Ranking', `Background OOR warm for likely-next players: ${names.join(', ')}`)
      fetch('/api/pr-maker/oor-warm/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...shared, names }),
      }).catch(() => {})
    })
  }, [ctx, card, loadPhase, currentPair, orders, edges, dlog])

  function recordComparison(firstWins) {
    if (!currentPair) return
    const [a, b] = currentPair
    const winner = firstWins ? a : b
    const loser = firstWins ? b : a
    dlog('info', 'PRMaker/Ranking', `Decision: ${winner} > ${loser} (answer ${edges.length + 1})`)
    const newEdges = [...edges, { winner, loser }]
    setEdges(newEdges)
    saveJson(STORAGE_EDGES, newEdges)
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
    if (!ctx?.tiers) return
    const fresh = ctx.tiers.map((tier) => shuffleArray(tier))
    setOrders(fresh)
    setEdges([])
    saveJson(STORAGE_TIER_COMPARE, fresh)
    saveJson(STORAGE_EDGES, [])
    saveJson(STORAGE_NAMES_FP, tiersFingerprint(ctx))
    saveJson(STORAGE_ALGO, 'v4')
    setCard(null)
    setExpanded(null)
    setArgText('')
    setArgPanelOpen(false)
    prevPairRef.current = null
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }

  if (!ctx || ctx._needsTiering) {
    return (
      <main className="process-page" aria-label="PR Maker — Data Explorer">
        <div className="process-page-inner">
          <h2 className="panel-title">PR Maker</h2>
          <p className="process-subtitle" style={{ marginTop: 12 }}>
            {ctx?._needsTiering ? 'Redirecting to tiering…' : 'No context found. Please go through the candidate selection first.'}
          </p>
          {!ctx?._needsTiering ? (
            <Link to="/pr-maker/candidates" className="pr-maker-back-link">← Back to candidates</Link>
          ) : null}
        </div>
      </main>
    )
  }

  function handleContinueToFinal() {
    const ordered = orderedFromRankingAndEdges(fullRanking, edges, ctx.selectedNames)
    dlog('info', 'PRMaker/Ranking', `Continue to final — ${ordered.length} players ranked`)
    const payload = { ordered, edges }
    try { sessionStorage.setItem('prMakerFinalSnapshot', JSON.stringify(payload)) } catch {}
    // Prefetch the final CSV in the background while the user reads the final
    // page — the warm caches make this server-cheap, and the export button
    // becomes instant. Stale prefetches are cleared first.
    try { sessionStorage.removeItem('prMakerPrefetchedCsv') } catch {}
    fetch('/api/pr-maker/final-export', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        start: ctx.startDate, end: ctx.endDate,
        eventSlugs: ctx.eventSlugs, mergeRules: ctx.mergeRules || [],
        ranking: ordered.map((p) => ({ name: p.name, copelandScore: p.score })),
      }),
    })
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => {
        if (d && d.csv) {
          try {
            sessionStorage.setItem('prMakerPrefetchedCsv', d.csv)
            dlog('info', 'PRMaker/Ranking', 'Final CSV prefetched and stored for instant export')
          } catch {}
        }
      })
      .catch(() => {})
    navigate('/pr-maker/final', { state: payload })
  }

  if (isDone) {
    const ordered = orderedFromRankingAndEdges(fullRanking, edges, ctx.selectedNames)
    return (
      <main className="process-page final-page" aria-label="PR Maker — Results">
        <div className="process-page-inner final-inner">
          <header className="final-hero">
            <h2 className="panel-title">PR Maker</h2>
            <p className="process-subtitle">Comparisons complete</p>
          </header>

          <RankingListPanel ordered={ordered} />

          <div className="compare-done-actions">
            <button type="button" className="compare-done-btn compare-done-btn--primary" onClick={handleContinueToFinal}>
              Continue to final list
            </button>
            <button type="button" className="compare-done-btn compare-done-btn--secondary" onClick={handleRestart}>
              Restart comparisons
            </button>
            <Link to="/pr-maker/tiering" className="compare-done-back">← Back to tiering</Link>
          </div>
        </div>
      </main>
    )
  }

  if (!isDone && !currentPair) {
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
          <p className="process-subtitle">
            Comparison · Tier {Math.min(tierIndex + 1, tierCount)} of {tierCount}
          </p>

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
              <p className="compare-arg-loading-title">Calling local Gemma model (one request, in-region stats only)…</p>
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

// Memoized: OOR stream progress updates loadPhase several times per second and
// would otherwise re-render this whole stat card on every NDJSON line.
const ComparisonBody = memo(function ComparisonBody({ card, expanded, pA, pB }) {
  const inA = card.in_region_summary?.[pA] || {}
  const inB = card.in_region_summary?.[pB] || {}
  const outA = card.out_region_summary?.[pA] || {}
  const outB = card.out_region_summary?.[pB] || {}
  const h2h = card.head_to_head_in_region || {}
  const ltr = card.loss_to_tournament_ratio || {}
  const elo = card.elo || {}

  const h2hA = h2h[pA] ?? 0
  const h2hB = h2h[pB] ?? 0
  const hasBeaten = useMemo(() => resolveHasBeaten(expanded), [expanded])
  const hasLostTo = useMemo(() => resolveHasLostTo(expanded), [expanded])
  const hasBeatenOOR = useMemo(
    () => resolveHasBeatenOOR(expanded, card, pA, pB),
    [expanded, card, pA, pB],
  )
  const hasLostToOOR = useMemo(
    () => resolveHasLostToOOR(expanded, card, pA, pB),
    [expanded, card, pA, pB],
  )
  const tournaments = useMemo(() => resolveTournamentsAttended(expanded), [expanded])
  const tourneyCountA = useMemo(
    () => tournaments.filter((t) => t.p1).length,
    [tournaments],
  )
  const tourneyCountB = useMemo(
    () => tournaments.filter((t) => t.p2).length,
    [tournaments],
  )

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
          {tournaments.length > 0 ? (
            <StatRow label="Tournaments" valA={tourneyCountA} valB={tourneyCountB} />
          ) : null}
        </tbody>
      </table>

      <OpponentRecordSection
        label="Has Beaten"
        rows={hasBeaten}
        pA={pA}
        pB={pB}
        commonMode="wins"
      />

      <OpponentRecordSection
        label="Has Lost To"
        rows={hasLostTo}
        pA={pA}
        pB={pB}
        commonMode="losses"
      />

      <TournamentCompareSection tournaments={tournaments} pA={pA} pB={pB} />

      <div className="compare-shared-lists-grid">
        <TagListCol title="Shared wins" items={card.shared_wins} />
        <TagListCol title="Shared losses" items={card.shared_losses} />
        <TagListCol title={`${pA} unique losses`} items={card.unique_losses?.[pA]} />
        <TagListCol title={`${pB} unique losses`} items={card.unique_losses?.[pB]} />
      </div>

      <OpponentRecordSection
        label="OOR Has Beaten"
        rows={hasBeatenOOR}
        pA={pA}
        pB={pB}
        commonMode="wins"
      />

      <OpponentRecordSection
        label="OOR Has Lost To"
        rows={hasLostToOOR}
        pA={pA}
        pB={pB}
        commonMode="losses"
      />
    </div>
  )
})

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

/**
 * Unified Has Beaten / Has Lost To table with optional common-opponents filter.
 * commonMode "wins" = both beat them; "losses" = both lost to them.
 */
function OpponentRecordSection({ label, rows, pA, pB, commonMode }) {
  const [commonOnly, setCommonOnly] = useState(false)
  const visible = useMemo(() => {
    if (!commonOnly) return rows
    return rows.filter((r) => isCommonOpponent(r, commonMode))
  }, [rows, commonOnly, commonMode])

  if (!rows.length) return null

  return (
    <CollapsibleSection title={`${label} (${visible.length})`} defaultOpen>
      <label className="compare-common-filter">
        <input
          type="checkbox"
          checked={commonOnly}
          onChange={(e) => setCommonOnly(e.target.checked)}
        />
        Common opponents only
      </label>
      {visible.length === 0 ? (
        <p className="compare-opp-empty">No common opponents.</p>
      ) : (
        <table className="compare-opp-table">
          <thead>
            <tr>
              <th>Opponent</th>
              <th className="compare-player-a">{pA}</th>
              <th className="compare-player-b">{pB}</th>
              <th>Opp ELO</th>
            </tr>
          </thead>
          <tbody>
            {visible.map((r) => {
              const p1Missing = r.p1Wins == null
              const p2Missing = r.p2Wins == null
              const uniqueStar = p1Missing !== p2Missing
                ? (p1Missing ? 'b' : 'a')
                : null
              const edge = (!p1Missing && !p2Missing)
                ? recordEdge(r.p1Wins, r.p1Losses, r.p2Wins, r.p2Losses)
                : null
              const rowClass = edge === 'a'
                ? 'compare-opp-row--a'
                : edge === 'b'
                  ? 'compare-opp-row--b'
                  : ''
              return (
                <tr key={r.opponent} className={rowClass}>
                  <td>
                    {uniqueStar ? (
                      <span
                        className={`compare-unique-star compare-unique-star--${uniqueStar}`}
                        title={uniqueStar === 'a' ? `${pA} only` : `${pB} only`}
                        aria-label={uniqueStar === 'a' ? `${pA} only` : `${pB} only`}
                      >
                        ★
                      </span>
                    ) : null}
                    {r.opponent}
                  </td>
                  <td>{p1Missing ? '--' : `${r.p1Wins}–${r.p1Losses}`}</td>
                  <td>{p2Missing ? '--' : `${r.p2Wins}–${r.p2Losses}`}</td>
                  <td>{r.oppElo}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      )}
    </CollapsibleSection>
  )
}

function isCommonOpponent(r, commonMode) {
  if (commonMode === 'losses') {
    return (r.p1Losses ?? 0) > 0 && (r.p2Losses ?? 0) > 0
  }
  return (r.p1Wins ?? 0) > 0 && (r.p2Wins ?? 0) > 0
}

/** Better record = more wins; if tied wins, fewer losses. Returns 'a' | 'b' | null (tie). */
function recordEdge(w1, l1, w2, l2) {
  if (w1 !== w2) return w1 > w2 ? 'a' : 'b'
  if (l1 !== l2) return l1 < l2 ? 'a' : 'b'
  return null
}

/** Prefer API hasBeaten; fall back to merging legacy shared/unique win lists. */
function resolveHasBeaten(expanded) {
  if (!expanded) return []
  if (Array.isArray(expanded.hasBeaten)) {
    return expanded.hasBeaten
  }
  const byOpp = new Map()
  for (const r of expanded.sharedWins || []) {
    byOpp.set(r.opponent, {
      opponent: r.opponent,
      p1Wins: r.p1Wins,
      p1Losses: r.p1Losses,
      p2Wins: r.p2Wins,
      p2Losses: r.p2Losses,
      oppElo: r.oppElo,
    })
  }
  for (const r of expanded.p1UniqueWins || []) {
    byOpp.set(r.opponent, {
      opponent: r.opponent,
      p1Wins: r.wins,
      p1Losses: r.losses,
      p2Wins: null,
      p2Losses: null,
      oppElo: r.oppElo,
    })
  }
  for (const r of expanded.p2UniqueWins || []) {
    byOpp.set(r.opponent, {
      opponent: r.opponent,
      p1Wins: null,
      p1Losses: null,
      p2Wins: r.wins,
      p2Losses: r.losses,
      oppElo: r.oppElo,
    })
  }
  return [...byOpp.values()].sort((a, b) => (b.oppElo ?? 0) - (a.oppElo ?? 0))
}

/** Prefer API hasLostTo; fall back to sharedLosses (common only). */
function resolveHasLostTo(expanded) {
  if (!expanded) return []
  if (Array.isArray(expanded.hasLostTo)) {
    return expanded.hasLostTo
  }
  return (expanded.sharedLosses || []).map((r) => ({
    opponent: r.opponent,
    p1Wins: r.p1Wins,
    p1Losses: r.p1Losses,
    p2Wins: r.p2Wins,
    p2Losses: r.p2Losses,
    oppElo: r.oppElo,
  }))
}

/** Merge notable OOR [name, count] lists into per-opponent win/loss maps. */
function notableListsToRecords(winsList, lossesList) {
  const byOpp = new Map()
  for (const item of winsList || []) {
    const name = Array.isArray(item) ? item[0] : String(item)
    const count = Array.isArray(item) ? (Number(item[1]) || 1) : 1
    if (!name) continue
    const rec = byOpp.get(name) || { wins: 0, losses: 0 }
    rec.wins += count
    byOpp.set(name, rec)
  }
  for (const item of lossesList || []) {
    const name = Array.isArray(item) ? item[0] : String(item)
    const count = Array.isArray(item) ? (Number(item[1]) || 1) : 1
    if (!name) continue
    const rec = byOpp.get(name) || { wins: 0, losses: 0 }
    rec.losses += count
    byOpp.set(name, rec)
  }
  return byOpp
}

function mergeOorRecordTables(recA, recB, mode) {
  const names = new Set([...recA.keys(), ...recB.keys()])
  const rows = []
  for (const opponent of names) {
    const a = recA.get(opponent)
    const b = recB.get(opponent)
    const aWins = a?.wins ?? 0
    const aLosses = a?.losses ?? 0
    const bWins = b?.wins ?? 0
    const bLosses = b?.losses ?? 0
    if (mode === 'wins' && aWins <= 0 && bWins <= 0) continue
    if (mode === 'losses' && aLosses <= 0 && bLosses <= 0) continue
    const aPlayed = aWins > 0 || aLosses > 0
    const bPlayed = bWins > 0 || bLosses > 0
    rows.push({
      opponent,
      p1Wins: aPlayed ? aWins : null,
      p1Losses: aPlayed ? aLosses : null,
      p2Wins: bPlayed ? bWins : null,
      p2Losses: bPlayed ? bLosses : null,
      oppElo: null,
    })
  }
  return rows.sort((x, y) => String(x.opponent).localeCompare(String(y.opponent)))
}

function resolveHasBeatenOOR(expanded, card, pA, pB) {
  if (Array.isArray(expanded?.hasBeatenOOR)) return expanded.hasBeatenOOR
  const outA = card?.out_region_summary?.[pA] || {}
  const outB = card?.out_region_summary?.[pB] || {}
  return mergeOorRecordTables(
    notableListsToRecords(outA.notable_wins, outA.notable_losses),
    notableListsToRecords(outB.notable_wins, outB.notable_losses),
    'wins',
  )
}

function resolveHasLostToOOR(expanded, card, pA, pB) {
  if (Array.isArray(expanded?.hasLostToOOR)) return expanded.hasLostToOOR
  const outA = card?.out_region_summary?.[pA] || {}
  const outB = card?.out_region_summary?.[pB] || {}
  return mergeOorRecordTables(
    notableListsToRecords(outA.notable_wins, outA.notable_losses),
    notableListsToRecords(outB.notable_wins, outB.notable_losses),
    'losses',
  )
}

/**
 * Prefer API tournamentsAttended (union of in-region + OOR with set runs).
 * Legacy tournamentsBothAttended is shared-only and has no sets — avoid using it
 * as a silent stand-in for the full list.
 */
function resolveTournamentsAttended(expanded) {
  if (!expanded) return []
  if (Array.isArray(expanded.tournamentsAttended)) {
    return expanded.tournamentsAttended
  }
  return []
}

function formatPlacement(n) {
  if (n == null || n === '') return '--'
  const num = Number(n)
  if (!Number.isFinite(num)) return '--'
  const j = num % 10
  const k = num % 100
  let suf = 'TH'
  if (j === 1 && k !== 11) suf = 'ST'
  else if (j === 2 && k !== 12) suf = 'ND'
  else if (j === 3 && k !== 13) suf = 'RD'
  return `${num}${suf}`
}

function formatTourneysDate(startAt) {
  if (!startAt) return ''
  try {
    return new Date(startAt * 1000).toLocaleDateString(undefined, {
      month: 'short',
      day: 'numeric',
      year: '2-digit',
    })
  } catch {
    return ''
  }
}

function tourneyRegionBadges(t, side = 'both') {
  const regions = new Set()
  if (side !== 'b' && t.p1?.region) regions.add(t.p1.region)
  if (side !== 'a' && t.p2?.region) regions.add(t.p2.region)
  if (regions.size === 0) return null
  return (
    <>
      {[...regions].map((region) => (
        <span
          key={region}
          className={`compare-tourney-region compare-tourney-region--${region === 'out' ? 'oor' : 'in'}`}
        >
          {region === 'out' ? 'OOR' : 'In'}
        </span>
      ))}
    </>
  )
}

function tournamentSlugFromEventSlug(eventSlug) {
  const parts = String(eventSlug || '').replace(/^\/+|\/+$/g, '').split('/')
  if (parts[0] === 'tournament' && parts[1]) return `tournament/${parts[1]}`
  return parts[0] ? parts.join('/') : ''
}

function isSharedTourney(t) {
  return Boolean(t.shared || (t.p1 && t.p2))
}

const tournamentIconCache = new Map()

function useTournamentIcons(tournaments) {
  const seeded = useMemo(() => {
    const next = {}
    for (const t of tournaments) {
      const slug = tournamentSlugFromEventSlug(t.eventSlug)
      if (slug && t.iconUrl) {
        next[slug] = t.iconUrl
        tournamentIconCache.set(slug, t.iconUrl)
      }
    }
    return next
  }, [tournaments])
  const [fetched, setFetched] = useState({})

  useEffect(() => {
    const slugs = [...new Set(
      tournaments.map((t) => tournamentSlugFromEventSlug(t.eventSlug)).filter(Boolean),
    )]
    if (!slugs.length) return undefined
    let cancelled = false

    async function load() {
      const collected = { ...seeded }
      for (const slug of slugs) {
        if (tournamentIconCache.has(slug) && collected[slug] == null) {
          collected[slug] = tournamentIconCache.get(slug)
        }
      }
      let pending = slugs.filter((slug) => !Object.prototype.hasOwnProperty.call(collected, slug))
      if (!cancelled) setFetched({ ...collected })
      while (pending.length && !cancelled) {
        let data
        try {
          const res = await fetch('/api/tournament-icons', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ slugs: pending }),
          })
          data = await res.json()
        } catch {
          break
        }
        const icons = data.icons || {}
        for (const [slug, url] of Object.entries(icons)) {
          tournamentIconCache.set(slug, url)
          collected[slug] = url
        }
        if (!cancelled) setFetched({ ...collected })
        const nextPending = (data.pending || []).filter(
          (slug) => !Object.prototype.hasOwnProperty.call(collected, slug),
        )
        if (!nextPending.length || nextPending.length >= pending.length) break
        pending = nextPending
      }
    }

    load()
    return () => { cancelled = true }
  }, [tournaments, seeded])

  return { ...seeded, ...fetched }
}

function TourneyIcon({ name, iconUrl }) {
  const initial = (name || '?').trim().charAt(0).toUpperCase() || 'T'
  if (iconUrl) {
    return <img src={iconUrl} alt="" className="compare-tourney-icon" />
  }
  return <div className="compare-tourney-icon fallback" aria-hidden="true">{initial}</div>
}

function TourneyPlace({ sideData, align, uniqueStar, uniqueLabel }) {
  return (
    <div className={`compare-tourney-place compare-tourney-place--${align}`}>
      {sideData ? (
        <>
          <span className="compare-tourney-place-main">
            {uniqueStar ? (
              <span
                className={`compare-unique-star compare-unique-star--${uniqueStar}`}
                title={uniqueLabel}
                aria-label={uniqueLabel}
              >
                ★
              </span>
            ) : null}
            <span className="compare-tourney-place-num">{formatPlacement(sideData.place)}</span>
          </span>
          <span className="compare-tourney-place-wl">{sideData.wins}–{sideData.losses}</span>
        </>
      ) : (
        <span className="compare-tourney-place-missing">--</span>
      )}
    </div>
  )
}

/** Better placement = lower place number; W–L as tiebreaker. */
function placementEdge(sideA, sideB) {
  if (!sideA || !sideB) return null
  const a = sideA.place
  const b = sideB.place
  if (a == null && b == null) {
    return recordEdge(sideA.wins ?? 0, sideA.losses ?? 0, sideB.wins ?? 0, sideB.losses ?? 0)
  }
  if (a == null) return 'b'
  if (b == null) return 'a'
  if (a !== b) return a < b ? 'a' : 'b'
  return recordEdge(sideA.wins ?? 0, sideA.losses ?? 0, sideB.wins ?? 0, sideB.losses ?? 0)
}

function TournamentCompareSection({ tournaments, pA, pB }) {
  const [commonOnly, setCommonOnly] = useState(false)
  const [dateView, setDateView] = useState(false)
  const icons = useTournamentIcons(tournaments)
  const shared = useMemo(() => tournaments.filter(isSharedTourney), [tournaments])
  const onlyA = useMemo(() => tournaments.filter((t) => t.p1 && !t.p2), [tournaments])
  const onlyB = useMemo(() => tournaments.filter((t) => t.p2 && !t.p1), [tournaments])
  const dateList = commonOnly ? shared : tournaments
  const shownCount = (dateView || commonOnly) ? dateList.length : tournaments.length

  if (!tournaments.length) return null

  return (
    <CollapsibleSection title={`Tournaments (${shownCount})`} defaultOpen>
      <div className="compare-tourney-filters">
        <label className="compare-common-filter">
          <input
            type="checkbox"
            checked={commonOnly}
            onChange={(e) => setCommonOnly(e.target.checked)}
          />
          Common tournaments only
        </label>
        <label className="compare-common-filter">
          <input
            type="checkbox"
            checked={dateView}
            onChange={(e) => setDateView(e.target.checked)}
          />
          Date view
        </label>
      </div>

      {dateView ? (
        dateList.length === 0 ? (
          <p className="compare-opp-empty">{commonOnly ? 'No common tournaments.' : 'No tournaments.'}</p>
        ) : (
          <div className="compare-tourney-list">
            {dateList.map((t) => (
              <TournamentCompareRow
                key={t.eventSlug || `${t.name}-${t.bracket}`}
                t={t}
                pA={pA}
                pB={pB}
                side="both"
                blankMissing
                iconUrl={icons[tournamentSlugFromEventSlug(t.eventSlug)]}
              />
            ))}
          </div>
        )
      ) : (
        <>
          <section className="compare-tourney-shared" aria-label="Common tournaments">
            <h4 className="compare-tourney-col-title">Common tournaments ({shared.length})</h4>
            {shared.length === 0 ? (
              <p className="compare-opp-empty">No common tournaments.</p>
            ) : (
              <div className="compare-tourney-list">
                {shared.map((t) => (
                  <TournamentCompareRow
                    key={t.eventSlug || `${t.name}-${t.bracket}`}
                    t={t}
                    pA={pA}
                    pB={pB}
                    side="both"
                    iconUrl={icons[tournamentSlugFromEventSlug(t.eventSlug)]}
                  />
                ))}
              </div>
            )}
          </section>

          {!commonOnly ? (
            <div className="compare-tourney-columns">
              <section className="compare-tourney-col compare-tourney-col--a" aria-label={`${pA} tournaments`}>
                <h4 className="compare-tourney-col-title compare-player-a">{pA}</h4>
                {onlyA.length === 0 ? (
                  <p className="compare-opp-empty">No unique tournaments.</p>
                ) : (
                  <div className="compare-tourney-list">
                    {onlyA.map((t) => (
                      <TournamentCompareRow
                        key={t.eventSlug || `${t.name}-${t.bracket}`}
                        t={t}
                        pA={pA}
                        pB={pB}
                        side="a"
                        iconUrl={icons[tournamentSlugFromEventSlug(t.eventSlug)]}
                      />
                    ))}
                  </div>
                )}
              </section>
              <section className="compare-tourney-col compare-tourney-col--b" aria-label={`${pB} tournaments`}>
                <h4 className="compare-tourney-col-title compare-player-b">{pB}</h4>
                {onlyB.length === 0 ? (
                  <p className="compare-opp-empty">No unique tournaments.</p>
                ) : (
                  <div className="compare-tourney-list">
                    {onlyB.map((t) => (
                      <TournamentCompareRow
                        key={t.eventSlug || `${t.name}-${t.bracket}`}
                        t={t}
                        pA={pA}
                        pB={pB}
                        side="b"
                        iconUrl={icons[tournamentSlugFromEventSlug(t.eventSlug)]}
                      />
                    ))}
                  </div>
                )}
              </section>
            </div>
          ) : null}
        </>
      )}
    </CollapsibleSection>
  )
}

function TournamentCompareRow({ t, pA, pB, side = 'both', iconUrl, blankMissing = false }) {
  const [open, setOpen] = useState(false)
  const solo = side !== 'both'
  const uniqueStar = blankMissing && Boolean(t.p1) !== Boolean(t.p2)
    ? (t.p1 ? 'a' : 'b')
    : null
  const edge = (!solo && t.p1 && t.p2) ? placementEdge(t.p1, t.p2) : null
  const rowClass = [
    'compare-tourney-card',
    open ? 'compare-tourney-card--open' : '',
    solo && side === 'a' ? 'compare-tourney-card--a' : '',
    solo && side === 'b' ? 'compare-tourney-card--b' : '',
    !solo && edge === 'a' ? 'compare-tourney-card--a' : '',
    !solo && edge === 'b' ? 'compare-tourney-card--b' : '',
    blankMissing && uniqueStar === 'a' ? 'compare-tourney-card--a' : '',
    blankMissing && uniqueStar === 'b' ? 'compare-tourney-card--b' : '',
  ].filter(Boolean).join(' ')
  const dateLabel = formatTourneysDate(t.startAt)
  const bracketLabel = t.bracket && t.bracket.toLowerCase() !== t.name?.toLowerCase()
    ? t.bracket
    : null
  const regionBadges = tourneyRegionBadges(t, side)
  const showP1 = side !== 'b'
  const showP2 = side !== 'a'

  return (
    <div className={rowClass}>
      <button
        type="button"
        className={`compare-tourney-header${solo ? ' compare-tourney-header--solo' : ''}`}
        aria-expanded={open}
        onClick={() => setOpen((o) => !o)}
      >
        {showP1 && !solo ? (
          <TourneyPlace
            sideData={t.p1}
            align="a"
            uniqueStar={uniqueStar === 'a' ? 'a' : null}
            uniqueLabel={uniqueStar === 'a' ? `${pA} only` : undefined}
          />
        ) : null}

        <div className="compare-tourney-mid">
          <div className="compare-tourney-title-wrap">
            <TourneyIcon name={t.name} iconUrl={iconUrl} />
            <div className="compare-tourney-title-block">
              <h3 className="compare-tourney-title">{t.name}</h3>
              {bracketLabel ? (
                <p className="compare-tourney-meta">
                  <span>Event:</span> {bracketLabel}
                </p>
              ) : null}
              {dateLabel ? (
                <p className="compare-tourney-meta">
                  <span>Date:</span> {dateLabel}
                </p>
              ) : null}
              {regionBadges ? (
                <p className="compare-tourney-meta compare-tourney-meta--badges">{regionBadges}</p>
              ) : null}
            </div>
          </div>
        </div>

        {solo ? <TourneyPlace sideData={side === 'a' ? t.p1 : t.p2} align="b" /> : null}
        {showP2 && !solo ? (
          <TourneyPlace
            sideData={t.p2}
            align="b"
            uniqueStar={uniqueStar === 'b' ? 'b' : null}
            uniqueLabel={uniqueStar === 'b' ? `${pB} only` : undefined}
          />
        ) : null}

        <span className="compare-tourney-expand" aria-hidden="true">{open ? '−' : '+'}</span>
      </button>

      {open ? (
        <div className={`compare-tourney-runs${solo && !blankMissing ? ' compare-tourney-runs--solo' : ''}`}>
          {showP1 && (t.p1 || blankMissing) ? (
            <TournamentRunCol label={pA} playerClass="compare-player-a" side={t.p1} />
          ) : null}
          {showP2 && (t.p2 || blankMissing) ? (
            <TournamentRunCol label={pB} playerClass="compare-player-b" side={t.p2} />
          ) : null}
          {!blankMissing && !((showP1 && t.p1) || (showP2 && t.p2)) ? (
            <p className="compare-opp-empty">No run data.</p>
          ) : null}
        </div>
      ) : null}
    </div>
  )
}

function TournamentRunCol({ label, playerClass, side }) {
  if (!side) {
    return (
      <div className="compare-tourney-run compare-tourney-run--empty">
        <div className={`compare-tourney-run-label ${playerClass}`}>{label}</div>
      </div>
    )
  }
  const sets = side.sets || []
  return (
    <div className="compare-tourney-run">
      <div className={`compare-tourney-run-label ${playerClass}`}>{label}</div>
      {sets.length === 0 ? (
        <p className="compare-opp-empty">No sets recorded.</p>
      ) : (
        <ul className="compare-set-list">
          {sets.map((s, i) => {
            const hasScore = s.playerScore != null && s.opponentScore != null
            return (
              <li
                key={s.setId || `${s.opponent}-${i}`}
                className={`compare-set-row ${s.won ? 'compare-set-row--win' : 'compare-set-row--loss'}`}
              >
                <span className="compare-set-result">{s.won ? 'W' : 'L'}</span>
                <span className="compare-set-opponent">{s.opponent}</span>
                <span className="compare-set-score">
                  {hasScore ? (
                    <>
                      <span className={`compare-set-box ${s.won ? 'compare-set-box--win' : ''}`}>
                        {s.playerScore}
                      </span>
                      <span className={`compare-set-box ${s.won ? '' : 'compare-set-box--loss'}`}>
                        {s.opponentScore}
                      </span>
                    </>
                  ) : (
                    <span className="compare-set-score-missing">--</span>
                  )}
                </span>
              </li>
            )
          })}
        </ul>
      )}
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

