import { useEffect, useRef } from 'react'
import { useDebug } from './DebugContext.jsx'

export default function DebugLogShelf() {
  const { enabled, shelfOpen, setShelfOpen, entries, clear } = useDebug()
  const endRef = useRef(null)
  const panelRef = useRef(null)

  useEffect(() => {
    if (shelfOpen && endRef.current) {
      endRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [entries.length, shelfOpen])

  useEffect(() => {
    if (!shelfOpen) return
    function onKey(e) {
      if (e.key === 'Escape') setShelfOpen(false)
    }
    document.addEventListener('keydown', onKey)
    return () => document.removeEventListener('keydown', onKey)
  }, [shelfOpen, setShelfOpen])

  if (!enabled) return null

  async function handleCopy() {
    const text = entries
      .map((e) => `[${e.ts}] [${e.level}] [${e.source}] ${e.message}${e.detail ? ' | ' + e.detail : ''}`)
      .join('\n')
    try {
      await navigator.clipboard.writeText(text)
    } catch {
      const ta = document.createElement('textarea')
      ta.value = text
      document.body.appendChild(ta)
      ta.select()
      document.execCommand('copy')
      document.body.removeChild(ta)
    }
  }

  return (
    <>
      <button
        type="button"
        className={`debug-shelf-tab${shelfOpen ? ' debug-shelf-tab--open' : ''}`}
        onClick={() => setShelfOpen((o) => !o)}
        aria-label={shelfOpen ? 'Close debug log' : 'Open debug log'}
        aria-expanded={shelfOpen}
      >
        <span className="debug-shelf-tab-arrow">{shelfOpen ? '‹' : '›'}</span>
      </button>

      <div
        ref={panelRef}
        className={`debug-shelf${shelfOpen ? ' debug-shelf--open' : ''}`}
        role="log"
        aria-live="polite"
        aria-hidden={!shelfOpen}
      >
        <div className="debug-shelf-header">
          <span className="debug-shelf-title">Debug Log</span>
          <span className="debug-shelf-count">{entries.length}</span>
          <div className="debug-shelf-actions">
            <button type="button" className="debug-shelf-action-btn" onClick={handleCopy}>Copy</button>
            <button type="button" className="debug-shelf-action-btn" onClick={clear}>Clear</button>
          </div>
        </div>
        <div className="debug-shelf-scroll">
          {entries.map((e, i) => (
            <div key={i} className={`debug-log-line debug-log-line--${e.level}`}>
              <span className="debug-log-ts">{e.ts}</span>
              <span className="debug-log-source">{e.source}</span>
              <span className="debug-log-msg">{e.message}</span>
              {e.detail ? <span className="debug-log-detail">{e.detail}</span> : null}
            </div>
          ))}
          <div ref={endRef} />
        </div>
      </div>
    </>
  )
}
