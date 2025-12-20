import type { TopicName } from '../api/types'
import { TopicList } from './TopicList'

export function Sidebar(props: {
  topics: TopicName[]
  selected?: TopicName
  onSelect: (topic: TopicName) => void
  onUnsubscribe: (topic: TopicName) => void
  onOpenSubscribe: () => void
  baseUrl: string
  streamStatus: string
  peerId?: string
}) {
  const { topics, selected, onSelect, onUnsubscribe, onOpenSubscribe, baseUrl, streamStatus, peerId } = props

  return (
    <aside className="sidebar">
      <div className="sidebarHeader">
        <div className="brand">Tatanka Test UI</div>
        <button className="button buttonPrimary" onClick={onOpenSubscribe} title="Subscribe">
          + Topic
        </button>
      </div>

      <div className="muted" style={{ fontSize: 12 }}>
        <div className="kvRow">
          <span className="kvKey">Peer ID:</span>
          <span className="kvValue mono truncate" title={peerId || ''}>
            {peerId || ''}
          </span>
          <button
            className="iconButton"
            title="Copy peer ID"
            onClick={async () => {
              if (!peerId) return
              try {
                await navigator.clipboard.writeText(peerId)
              } catch {
                // ignore
              }
            }}
            disabled={!peerId}
          >
            ⧉
          </button>
        </div>
        <br />
        Event stream: <span>{streamStatus}</span>
      </div>

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' }}>
        <div className="muted" style={{ fontSize: 12 }}>
          Subscribed topics
        </div>
        <div className="muted" style={{ fontSize: 12 }}>
          {topics.length}
        </div>
      </div>

      <TopicList topics={topics} selected={selected} onSelect={onSelect} onUnsubscribe={onUnsubscribe} />
    </aside>
  )
}


