import { useMemo, useState } from 'react'

export function formatPlainRanking(ordered) {
  if (!ordered.length) return ''
  return ordered
    .map((p, i) => `${i + 1}. ${p.name} — ${p.score} H2H win${p.score === 1 ? '' : 's'}`)
    .join('\n')
}

async function copyText(text) {
  try {
    await navigator.clipboard.writeText(text)
    return true
  } catch {
    const ta = document.createElement('textarea')
    ta.value = text
    document.body.appendChild(ta)
    ta.select()
    const ok = document.execCommand('copy')
    document.body.removeChild(ta)
    return ok
  }
}

export default function RankingListPanel({ ordered }) {
  const [copied, setCopied] = useState(false)
  const copyTextValue = useMemo(() => formatPlainRanking(ordered), [ordered])

  async function handleCopy() {
    const ok = await copyText(copyTextValue)
    if (ok) {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    }
  }

  if (!ordered.length) return null

  return (
    <div className="pr-rank-panel">
      <div className="pr-rank-toolbar">
        <span className="pr-rank-count">{ordered.length} players</span>
        <button type="button" className="pr-rank-copy-btn" onClick={handleCopy}>
          {copied ? 'Copied!' : 'Copy'}
        </button>
      </div>

      <table className="pr-rank-table">
        <thead>
          <tr>
            <th className="pr-rank-th pr-rank-th--num">#</th>
            <th className="pr-rank-th">Player</th>
            <th className="pr-rank-th pr-rank-th--score">H2H wins</th>
          </tr>
        </thead>
        <tbody>
          {ordered.map((p, i) => (
            <tr key={p.name} className="pr-rank-tr">
              <td className="pr-rank-td pr-rank-td--num">{i + 1}</td>
              <td className="pr-rank-td pr-rank-td--name">{p.name}</td>
              <td className="pr-rank-td pr-rank-td--score">{p.score}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
