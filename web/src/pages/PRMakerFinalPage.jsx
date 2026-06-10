import { useMemo, useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { useDebugLog } from '../debug/DebugContext.jsx'
import RankingListPanel from '../components/RankingListPanel.jsx'

function loadJson(key) {
  try { return JSON.parse(sessionStorage.getItem(key)) } catch { return null }
}

export default function PRMakerFinalPage() {
  const dlog = useDebugLog()
  const snapshot = useMemo(() => loadJson('prMakerFinalSnapshot'), [])
  const ctx = useMemo(() => loadJson('prMakerRankingContext'), [])

  const ordered = snapshot?.ordered || []

  const [csvLoading, setCsvLoading] = useState(false)
  const [csvError, setCsvError] = useState('')
  const prefetchedRef = useRef(null)

  useEffect(() => {
    dlog('info', 'PRMaker/Final', `Mount — ${ordered.length} ranked players`)
    try {
      const csv = sessionStorage.getItem('prMakerPrefetchedCsv')
      if (csv) {
        prefetchedRef.current = csv
        dlog('info', 'PRMaker/Final', 'Prefetched CSV found in sessionStorage')
      } else {
        dlog('info', 'PRMaker/Final', 'No prefetched CSV — will fetch on demand')
      }
    } catch {}
  }, [])

  function downloadCsvString(csvText) {
    const blob = new Blob([csvText], { type: 'text/csv;charset=utf-8;' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'norcal-pr-export.csv'
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  }

  async function handleExportCsv() {
    if (!ctx || !ordered.length) return
    if (!prefetchedRef.current) {
      try {
        const csv = sessionStorage.getItem('prMakerPrefetchedCsv')
        if (csv) prefetchedRef.current = csv
      } catch {}
    }
    if (prefetchedRef.current) {
      dlog('info', 'PRMaker/Final', 'Exporting CSV from prefetched data (instant)')
      downloadCsvString(prefetchedRef.current)
      return
    }
    dlog('info', 'PRMaker/Final', 'Fetching CSV from server (not prefetched)')
    setCsvLoading(true)
    setCsvError('')
    try {
      const res = await fetch('/api/pr-maker/final-export', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          start: ctx.startDate,
          end: ctx.endDate,
          eventSlugs: ctx.eventSlugs,
          mergeRules: ctx.mergeRules || [],
          ranking: ordered.map(p => ({ name: p.name, copelandScore: p.score })),
        }),
      })
      const data = await res.json()
      if (!res.ok) {
        setCsvError(data.error || 'Export failed')
        return
      }
      downloadCsvString(data.csv)
    } catch (err) {
      setCsvError(`Error: ${err.message}`)
    } finally {
      setCsvLoading(false)
    }
  }

  if (!ordered.length) {
    return (
      <main className="process-page" aria-label="PR Maker — Final">
        <div className="process-page-inner">
          <h2 className="panel-title">PR Maker</h2>
          <p className="process-subtitle" style={{ marginTop: 12 }}>
            No ranking data found. Please complete the comparison flow first.
          </p>
          <Link to="/pr-maker/ranking" className="pr-maker-back-link">← Back to comparisons</Link>
        </div>
      </main>
    )
  }

  return (
    <main className="process-page final-page" aria-label="PR Maker — Final Rankings">
      <div className="process-page-inner final-inner">
        <header className="final-hero">
          <h2 className="panel-title">PR Maker</h2>
          <p className="process-subtitle">Final List of Rankings</p>
        </header>

        <RankingListPanel ordered={ordered} />

        <div className="final-export-row">
          <div className="final-export-card final-export-card--csv">
            <h4 className="final-export-label">Full data export</h4>
            <p className="final-export-desc">
              Download a CSV with derived statistics for every candidate — ELO, attendance, notable results, and more.
            </p>
            <button
              type="button"
              className="final-csv-btn"
              onClick={handleExportCsv}
              disabled={csvLoading}
            >
              {csvLoading ? 'Generating…' : 'Download CSV'}
            </button>
            {csvError ? <p className="final-csv-error">{csvError}</p> : null}
          </div>
        </div>

        <div className="final-back-row">
          <Link to="/pr-maker/ranking" className="pr-maker-back-link">← Back to comparisons</Link>
          <Link to="/pr-maker/candidates" className="pr-maker-back-link">← Back to candidates</Link>
        </div>
      </div>
    </main>
  )
}
