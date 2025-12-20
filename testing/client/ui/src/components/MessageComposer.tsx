import { useEffect, useRef, useState } from 'react'

export function MessageComposer(props: { disabled?: boolean; onSend: (text: string) => Promise<void> }) {
  const { disabled, onSend } = props
  const [text, setText] = useState('')
  const [busy, setBusy] = useState(false)
  const textareaRef = useRef<HTMLTextAreaElement | null>(null)

  useEffect(() => {
    if (!disabled) textareaRef.current?.focus()
  }, [disabled])

  const canSend = !disabled && !busy && text.trim().length > 0

  return (
    <div className="composer">
      <textarea
        ref={textareaRef}
        className="input"
        placeholder={disabled ? 'Subscribe/select a topic to start chatting…' : 'Type a message…'}
        value={text}
        onChange={(e) => setText(e.target.value)}
        disabled={disabled || busy}
        rows={2}
        onKeyDown={async (e) => {
          if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
            e.preventDefault()
            if (!canSend) return
            setBusy(true)
            try {
              await onSend(text)
              setText('')
            } finally {
              setBusy(false)
            }
          }
        }}
      />
      <button
        className="button buttonPrimary"
        disabled={!canSend}
        title="Send (Ctrl/Cmd+Enter)"
        onClick={async () => {
          if (!canSend) return
          setBusy(true)
          try {
            await onSend(text)
            setText('')
          } finally {
            setBusy(false)
          }
        }}
      >
        Send
      </button>
    </div>
  )
}


