import type { ChatMessage } from '../api/types'

function formatTime(ts: number): string {
  try {
    return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  } catch {
    return ''
  }
}

export function MessageList(props: { messages: ChatMessage[] }) {
  const { messages } = props

  return (
    <div className="messages">
      {messages.length === 0 ? (
        <div className="muted">No messages yet. Broadcast something to get started.</div>
      ) : null}
      {messages.map((m) => (
        <div className={`messageRow ${m.direction === 'system' ? 'messageRowSystem' : ''}`} key={m.id}>
          <div
            className={`bubble ${m.direction === 'out' ? 'bubbleMine' : ''} ${
              m.direction === 'system' ? 'bubbleSystem' : ''
            }`}
          >
            <div className="bubbleHeader">
              <div className="peer" title={m.peer}>
                {m.direction === 'out' ? 'You' : m.peer}
              </div>
              <div className="time">{formatTime(m.receivedAt)}</div>
            </div>
            <div className="bubbleBody">{m.text}</div>
          </div>
        </div>
      ))}
    </div>
  )
}


