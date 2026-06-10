import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { useDebugLog } from '../debug/DebugContext.jsx'

const DRAFT_KEY = 'prMakerTieringDraft'
const TIERING_CTX_KEY = 'prMakerTieringContext'
const COMPARE_KEYS = [
  'prMakerCompareInsert',
  'prMakerCompareEdges',
  'prMakerCompareAlgo',
  'prMakerCompareNamesFp',
  'prMakerTierCompare',
]

let nextTierId = 1
function newTierId() {
  nextTierId += 1
  return `tier-${nextTierId}`
}

function namesFingerprint(names) {
  return JSON.stringify([...names].sort())
}

function loadJson(key) {
  try {
    return JSON.parse(sessionStorage.getItem(key))
  } catch {
    return null
  }
}

function saveJson(key, val) {
  try {
    sessionStorage.setItem(key, JSON.stringify(val))
  } catch {
    /* ignore quota errors */
  }
}

function loadTieringContext(locationState) {
  if (locationState?.selectedNames?.length) return locationState
  return loadJson(TIERING_CTX_KEY)
}

function loadInitialTieringState(selectedNames) {
  const fp = namesFingerprint(selectedNames)
  const draft = loadJson(DRAFT_KEY)
  if (draft?.namesFp === fp && Array.isArray(draft.pool) && Array.isArray(draft.tiers)) {
    const selectedSet = new Set(selectedNames)
    const draftPool = draft.pool.filter((n) => selectedSet.has(n))
    const draftTiers = draft.tiers.map((t) => ({
      id: t.id || newTierId(),
      names: (t.names || []).filter((n) => selectedSet.has(n)),
    }))
    const assigned = new Set([...draftPool, ...draftTiers.flatMap((t) => t.names)])
    const missing = selectedNames.filter((n) => !assigned.has(n))
    return {
      pool: [...draftPool, ...missing],
      tiers: draftTiers.length > 0 ? draftTiers : [{ id: newTierId(), names: [] }],
    }
  }
  return {
    pool: [...selectedNames],
    tiers: [{ id: newTierId(), names: [] }],
  }
}

function PlayerChip({ name, selected, onSelect, onDragStart, onDragEnd }) {
  return (
    <button
      type="button"
      className={`tiering-chip${selected ? ' tiering-chip--selected' : ''}`}
      draggable
      onDragStart={(e) => onDragStart(e, name)}
      onDragEnd={onDragEnd}
      onClick={(e) => {
        e.stopPropagation()
        onSelect(name)
      }}
      aria-pressed={selected}
    >
      {name}
    </button>
  )
}

function DropZone({
  zoneId,
  label,
  badge,
  names,
  selectedChip,
  dragOver,
  onDragOver,
  onDragLeave,
  onDrop,
  onSelectChip,
  onDragStart,
  onDragEnd,
  onRemoveTier,
  canRemove,
  emptyHint,
}) {
  return (
    <section
      className={`tiering-drop-zone${dragOver === zoneId ? ' tiering-drop-zone--over' : ''}`}
      onDragOver={(e) => onDragOver(e, zoneId)}
      onDragLeave={onDragLeave}
      onDrop={(e) => onDrop(e, zoneId)}
      onClick={() => {
        if (selectedChip) onDrop(null, zoneId, selectedChip)
      }}
    >
      <div className="tiering-drop-zone-header">
        {badge ? <span className="tiering-tier-badge">{badge}</span> : null}
        <span className="tiering-drop-zone-label">{label}</span>
        {canRemove ? (
          <button
            type="button"
            className="tiering-remove-tier-btn"
            onClick={(e) => {
              e.stopPropagation()
              onRemoveTier()
            }}
          >
            Remove tier
          </button>
        ) : null}
      </div>
      <div className="tiering-drop-zone-body">
        {names.length === 0 ? (
          <p className="tiering-drop-zone-empty">{emptyHint}</p>
        ) : (
          names.map((name) => (
            <PlayerChip
              key={name}
              name={name}
              selected={selectedChip === name}
              onSelect={onSelectChip}
              onDragStart={onDragStart}
              onDragEnd={onDragEnd}
            />
          ))
        )}
      </div>
    </section>
  )
}

function TieringWorkspace({ ctx }) {
  const dlog = useDebugLog()
  const navigate = useNavigate()
  const initial = useMemo(() => loadInitialTieringState(ctx.selectedNames), [ctx.selectedNames])
  const namesFp = useMemo(() => namesFingerprint(ctx.selectedNames), [ctx.selectedNames])

  const [pool, setPool] = useState(initial.pool)
  const [tiers, setTiers] = useState(initial.tiers)
  const [selectedChip, setSelectedChip] = useState(null)
  const [dragOver, setDragOver] = useState(null)
  const dragNameRef = useRef(null)
  const loggedInitRef = useRef(false)

  useEffect(() => {
    if (loggedInitRef.current) return
    loggedInitRef.current = true
    dlog(
      'info',
      'PRMaker/Tiering',
      `Ready — ${initial.pool.length} in pool, ${initial.tiers.length} tier(s)`,
    )
  }, [dlog, initial.pool.length, initial.tiers.length])

  useEffect(() => {
    saveJson(DRAFT_KEY, { namesFp, pool, tiers })
  }, [namesFp, pool, tiers])

  function findNameLocation(name) {
    if (pool.includes(name)) return { zone: 'pool', tierIdx: -1 }
    const tierIdx = tiers.findIndex((t) => t.names.includes(name))
    if (tierIdx >= 0) return { zone: 'tier', tierIdx }
    return null
  }

  function moveName(name, destZoneId) {
    const loc = findNameLocation(name)
    if (!loc) return

    let nextPool = [...pool]
    let nextTiers = tiers.map((t) => ({ ...t, names: [...t.names] }))

    if (loc.zone === 'pool') {
      nextPool = nextPool.filter((n) => n !== name)
    } else {
      nextTiers[loc.tierIdx].names = nextTiers[loc.tierIdx].names.filter((n) => n !== name)
    }

    if (destZoneId === 'pool') {
      if (!nextPool.includes(name)) nextPool.push(name)
    } else if (destZoneId.startsWith('tier-')) {
      const idx = nextTiers.findIndex((t) => t.id === destZoneId)
      if (idx >= 0 && !nextTiers[idx].names.includes(name)) {
        nextTiers[idx].names.push(name)
      }
    }

    setPool(nextPool)
    setTiers(nextTiers)
    setSelectedChip(null)
    dlog('info', 'PRMaker/Tiering', `Moved ${name} → ${destZoneId}`)
  }

  const handleDragStart = useCallback((e, name) => {
    dragNameRef.current = name
    e.dataTransfer.effectAllowed = 'move'
    e.dataTransfer.setData('text/plain', name)
    setSelectedChip(null)
  }, [])

  const handleDragEnd = useCallback(() => {
    dragNameRef.current = null
    setDragOver(null)
  }, [])

  const handleDragOver = useCallback((e, zoneId) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
    setDragOver(zoneId)
  }, [])

  const handleDragLeave = useCallback(() => {
    setDragOver(null)
  }, [])

  function handleDrop(e, zoneId, nameOverride) {
    e?.preventDefault?.()
    const name = nameOverride || dragNameRef.current || e?.dataTransfer?.getData('text/plain')
    if (!name) return
    moveName(name, zoneId)
    dragNameRef.current = null
    setDragOver(null)
  }

  function handleSelectChip(name) {
    setSelectedChip((prev) => (prev === name ? null : name))
  }

  function handleAddTier() {
    const next = [...tiers, { id: newTierId(), names: [] }]
    setTiers(next)
    dlog('info', 'PRMaker/Tiering', `Added tier — now ${next.length} tiers`)
  }

  function handleRemoveTier(tierIdx) {
    const tier = tiers[tierIdx]
    if (!tier || tier.names.length > 0) return
    if (tiers.length <= 1) return
    setTiers(tiers.filter((_, i) => i !== tierIdx))
    dlog('info', 'PRMaker/Tiering', `Removed empty tier at index ${tierIdx}`)
  }

  const assignedCount = useMemo(
    () => tiers.reduce((s, t) => s + t.names.length, 0),
    [tiers],
  )
  const totalCount = ctx.selectedNames.length
  const nonEmptyTierCount = tiers.filter((t) => t.names.length > 0).length
  const canProceed = pool.length === 0 && assignedCount === totalCount && totalCount > 0

  function clearCompareSession() {
    for (const key of COMPARE_KEYS) {
      try {
        sessionStorage.removeItem(key)
      } catch {
        /* ignore */
      }
    }
  }

  function handleProceed() {
    if (!canProceed) return
    const tierNames = tiers.map((t) => [...t.names])
    dlog(
      'info',
      'PRMaker/Tiering',
      `Proceeding — ${totalCount} candidates in ${nonEmptyTierCount} non-empty tier(s)`,
    )
    const payload = {
      startDate: ctx.startDate,
      endDate: ctx.endDate,
      eventSlugs: ctx.eventSlugs,
      mergeRules: ctx.mergeRules || [],
      selectedNames: ctx.selectedNames,
      tiers: tierNames,
    }
    clearCompareSession()
    saveJson('prMakerRankingContext', payload)
    navigate('/pr-maker/ranking', { state: payload })
  }

  return (
    <>
      <main className="process-page tiering-page" aria-label="PR Maker — Tiering">
        <div className="process-page-inner">
          <header className="process-header">
            <h2 className="panel-title">PR Maker</h2>
            <p className="process-subtitle">Tiering</p>
            <p className="candidates-scope-hint">
              Drag candidates into tiers (Tier 1 = highest). Comparisons run only within each tier.
              {' · '}
              {ctx.startDate} — {ctx.endDate}
              {' · '}
              <strong>{ctx.eventSlugs?.length ?? 0}</strong> event
              {(ctx.eventSlugs?.length ?? 0) === 1 ? '' : 's'}
            </p>
          </header>

          <div className="tiering-toolbar">
            <button type="button" className="tiering-add-tier-btn" onClick={handleAddTier}>
              <span className="tiering-add-tier-icon" aria-hidden="true">+</span>
              Add tier
            </button>
            {selectedChip ? (
              <p className="tiering-select-hint">
                Selected: <strong>{selectedChip}</strong> — click a tier or Unassigned to move
              </p>
            ) : (
              <p className="tiering-select-hint tiering-select-hint--muted">
                Drag players into tiers, or click a name then click a destination
              </p>
            )}
          </div>

          <div className="tiering-layout">
            <div className="tiering-tiers-column">
              {tiers.map((tier, idx) => (
                <DropZone
                  key={tier.id}
                  zoneId={tier.id}
                  badge={`Tier ${idx + 1}`}
                  label={idx === 0 ? 'Highest rank' : idx === tiers.length - 1 ? 'Lower rank' : ''}
                  names={tier.names}
                  selectedChip={selectedChip}
                  dragOver={dragOver}
                  onDragOver={handleDragOver}
                  onDragLeave={handleDragLeave}
                  onDrop={handleDrop}
                  onSelectChip={handleSelectChip}
                  onDragStart={handleDragStart}
                  onDragEnd={handleDragEnd}
                  canRemove={tiers.length > 1 && tier.names.length === 0}
                  onRemoveTier={() => handleRemoveTier(idx)}
                  emptyHint="Drop players here"
                />
              ))}
            </div>

            <aside className="tiering-pool-column">
              <DropZone
                zoneId="pool"
                label="Unassigned"
                badge={pool.length > 0 ? String(pool.length) : null}
                names={pool}
                selectedChip={selectedChip}
                dragOver={dragOver}
                onDragOver={handleDragOver}
                onDragLeave={handleDragLeave}
                onDrop={handleDrop}
                onSelectChip={handleSelectChip}
                onDragStart={handleDragStart}
                onDragEnd={handleDragEnd}
                emptyHint="All candidates assigned"
              />
            </aside>
          </div>

          <Link to="/pr-maker/candidates" className="pr-maker-back-link tiering-back-link">
            ← Back to candidates
          </Link>
        </div>
      </main>

      <div className="process-bottom-fade" aria-hidden="true" />
      <div className="process-bottom-bar">
        <span className="tiering-bottom-status">
          {pool.length > 0
            ? `${pool.length} unassigned — assign all before continuing`
            : `${assignedCount} candidates in ${nonEmptyTierCount} tier${nonEmptyTierCount === 1 ? '' : 's'}`}
        </span>
        <button
          type="button"
          className="process-ingest-btn"
          disabled={!canProceed}
          onClick={handleProceed}
        >
          Proceed to comparisons — {totalCount} candidate{totalCount === 1 ? '' : 's'}
          {nonEmptyTierCount > 0 ? ` in ${nonEmptyTierCount} tier${nonEmptyTierCount === 1 ? '' : 's'}` : ''}
        </button>
      </div>
    </>
  )
}

export default function PRMakerTieringPage() {
  const location = useLocation()
  const ctx = useMemo(() => loadTieringContext(location.state), [location.state])
  const ctxKey = ctx?.selectedNames?.length ? namesFingerprint(ctx.selectedNames) : null

  useEffect(() => {
    if (ctx?.selectedNames?.length) {
      saveJson(TIERING_CTX_KEY, ctx)
    }
  }, [ctx])

  if (!ctx?.selectedNames?.length) {
    return (
      <main className="process-page" aria-label="PR Maker — Tiering">
        <div className="process-page-inner">
          <h2 className="panel-title">PR Maker</h2>
          <p className="process-subtitle" style={{ marginTop: 12 }}>
            No candidates found. Please select candidates first.
          </p>
          <Link to="/pr-maker/candidates" className="pr-maker-back-link">← Back to candidates</Link>
        </div>
      </main>
    )
  }

  return <TieringWorkspace key={ctxKey} ctx={ctx} />
}
