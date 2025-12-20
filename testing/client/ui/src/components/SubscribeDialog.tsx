import { useEffect, useState } from 'react'

export function SubscribeDialog(props: {
  open: boolean
  onClose: () => void
  onSubscribe: (topic: string) => Promise<void>
}) {
  const { open, onClose, onSubscribe } = props
  const [topic, setTopic] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open) return
    setTopic('')
    setError(null)
    setBusy(false)
  }, [open])

  if (!open) return null

  return (
    <div
      className="dialogBackdrop"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose()
      }}
      role="dialog"
      aria-modal="true"
    >
      <div className="dialog">
        <h3 className="dialogTitle">Subscribe to topic</h3>
        <div>
          <input
            className="input"
            placeholder="e.g. orders, chat, my_topic"
            value={topic}
            onChange={(e) => setTopic(e.target.value)}
            autoFocus
          />
          <div className="helperText">Prefer simple topic names (no spaces).</div>
          {error ? <div className="helperText" style={{ color: 'var(--danger)' }}>{error}</div> : null}
        </div>
        <div className="dialogRow">
          <button className="button" onClick={onClose} disabled={busy}>
            Cancel
          </button>
          <button
            className="button buttonPrimary"
            onClick={async () => {
              const t = topic.trim()
              if (!t) {
                setError('Topic is required.')
                return
              }
              setBusy(true)
              setError(null)
              try {
                await onSubscribe(t)
                onClose()
              } catch (e: any) {
                setError(e?.message || 'Failed to subscribe.')
              } finally {
                setBusy(false)
              }
            }}
            disabled={busy}
          >
            Subscribe
          </button>
        </div>
      </div>
    </div>
  )
}


