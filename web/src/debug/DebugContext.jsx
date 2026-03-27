import { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react'

const MAX_ENTRIES = 2000
const LS_KEY = 'norcalSmashDebug'

const DebugContext = createContext({
  enabled: false,
  setEnabled: () => {},
  shelfOpen: false,
  setShelfOpen: () => {},
  entries: [],
  log: () => {},
  clear: () => {},
})

function ts() {
  const d = new Date()
  return (
    String(d.getHours()).padStart(2, '0') + ':' +
    String(d.getMinutes()).padStart(2, '0') + ':' +
    String(d.getSeconds()).padStart(2, '0') + '.' +
    String(d.getMilliseconds()).padStart(3, '0')
  )
}

function summarizeBody(body) {
  if (!body) return null
  if (typeof body === 'string') {
    try {
      const parsed = JSON.parse(body)
      const keys = Object.keys(parsed)
      if (keys.length <= 6) return JSON.stringify(parsed)
      return `{${keys.slice(0, 6).join(', ')}… (${keys.length} keys)}`
    } catch { return body.length > 400 ? body.slice(0, 400) + '…' : body }
  }
  if (body instanceof FormData) return '[FormData]'
  if (body instanceof URLSearchParams) return body.toString()
  return '[body]'
}

function summarizeJson(url, data) {
  if (typeof data !== 'object' || data === null) return String(data)
  if (url.includes('/api/elo') && Array.isArray(data.rankings)) {
    return `{rankings: ${data.rankings.length} players, missingRanges: ${(data.missingRanges || []).length}}`
  }
  if (Array.isArray(data.events)) return `{events: ${data.events.length}}`
  if (data.rankings) return `{rankings: ${data.rankings.length}}`
  const keys = Object.keys(data)
  const compact = JSON.stringify(data)
  if (compact.length < 300) return compact
  return `{${keys.slice(0, 6).join(', ')}… (${keys.length} keys)}`
}

export function DebugProvider({ children }) {
  const [enabled, setEnabledRaw] = useState(() => {
    try { return localStorage.getItem(LS_KEY) === '1' } catch { return false }
  })
  const [shelfOpen, setShelfOpen] = useState(false)
  const [entries, setEntries] = useState([])

  const enabledRef = useRef(enabled)
  const originalFetchRef = useRef(null)
  const patchedRef = useRef(false)
  const logRef = useRef(null)
  const serverDebugSeqRef = useRef(0)

  const pushEntry = useCallback((entry) => {
    setEntries((prev) => {
      const next = [...prev, entry]
      return next.length > MAX_ENTRIES ? next.slice(next.length - MAX_ENTRIES) : next
    })
  }, [])

  const log = useCallback((level, source, message, detail, tsOverride) => {
    if (!enabledRef.current) return
    const entry = {
      ts: tsOverride || ts(),
      level: level || 'info',
      source: source || '',
      message,
      detail: detail || '',
    }
    pushEntry(entry)
    console.debug(`[NorCalDebug] [${entry.ts}] [${entry.source}] ${entry.message}${entry.detail ? ' | ' + entry.detail : ''}`)
  }, [pushEntry])

  logRef.current = log

  const clear = useCallback(() => setEntries([]), [])

  function setEnabled(val) {
    setEnabledRaw(val)
    enabledRef.current = val
    try { localStorage.setItem(LS_KEY, val ? '1' : '0') } catch {}
    if (!val) setShelfOpen(false)
  }

  useEffect(() => {
    enabledRef.current = enabled

    if (enabled && !patchedRef.current) {
      if (!originalFetchRef.current) originalFetchRef.current = window.fetch
      const orig = originalFetchRef.current

      window.fetch = async function debugFetch(input, init) {
        if (!enabledRef.current) return orig.call(window, input, init)
        const method = (init?.method || 'GET').toUpperCase()
        const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input?.url || String(input)
        if (url.includes('/api/debug/server-events')) {
          return orig.call(window, input, init)
        }
        const bodyPreview = summarizeBody(init?.body)
        const startMs = performance.now()
        logRef.current('info', 'fetch', `→ ${method} ${url}`, bodyPreview ? `body: ${bodyPreview}` : '')
        try {
          const res = await orig.call(window, input, init)
          const elapsed = (performance.now() - startMs).toFixed(0)
          let detail = `${res.status} ${res.statusText} · ${elapsed}ms`
          try {
            const clone = res.clone()
            const ct = res.headers.get('content-type') || ''
            if (ct.includes('json') && !ct.includes('ndjson')) {
              const json = await clone.json()
              detail += ' · ' + summarizeJson(url, json)
            } else if (ct.includes('ndjson')) {
              detail += ' · NDJSON stream (body not parsed in debug log)'
            }
          } catch {}
          logRef.current(res.ok ? 'info' : 'warn', 'fetch', `← ${method} ${url}`, detail)
          return res
        } catch (err) {
          const elapsed = (performance.now() - startMs).toFixed(0)
          if (err.name === 'AbortError') {
            logRef.current('warn', 'fetch', `✕ ${method} ${url} (aborted)`, `${elapsed}ms`)
          } else {
            logRef.current('error', 'fetch', `✕ ${method} ${url}`, `${err.message} · ${elapsed}ms`)
          }
          throw err
        }
      }
      patchedRef.current = true
    }

    if (!enabled && patchedRef.current && originalFetchRef.current) {
      window.fetch = originalFetchRef.current
      patchedRef.current = false
    }
  }, [enabled])

  useEffect(() => {
    if (!enabled) return
    const orig = originalFetchRef.current || window.fetch
    let cancelled = false
    const poll = async () => {
      if (cancelled || !enabledRef.current) return
      try {
        const res = await orig(`/api/debug/server-events?since=${serverDebugSeqRef.current}`)
        if (!res.ok || cancelled || !enabledRef.current) return
        const data = await res.json()
        const evs = Array.isArray(data.events) ? data.events : []
        let maxSeen = serverDebugSeqRef.current
        for (const e of evs) {
          if (typeof e.seq === 'number' && e.seq > maxSeen) maxSeen = e.seq
          logRef.current(
            e.level || 'info',
            e.source || 'server',
            e.message || '',
            e.detail || '',
            e.ts || '',
          )
        }
        serverDebugSeqRef.current = maxSeen
      } catch {
        /* ignore poll errors */
      }
    }
    poll()
    const id = window.setInterval(poll, 1500)
    return () => {
      cancelled = true
      window.clearInterval(id)
    }
  }, [enabled])

  return (
    <DebugContext.Provider value={{ enabled, setEnabled, shelfOpen, setShelfOpen, entries, log, clear }}>
      {children}
    </DebugContext.Provider>
  )
}

export function useDebug() {
  return useContext(DebugContext)
}

export function useDebugLog() {
  const { log } = useContext(DebugContext)
  return log
}
