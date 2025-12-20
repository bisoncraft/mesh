import type { TopicName } from '../api/types'

export function TopicList(props: {
  topics: TopicName[]
  selected?: TopicName
  onSelect: (topic: TopicName) => void
  onUnsubscribe: (topic: TopicName) => void
}) {
  const { topics, selected, onSelect, onUnsubscribe } = props
  return (
    <div className="topicList">
      {topics.length === 0 ? (
        <div className="muted" style={{ padding: 10 }}>
          No subscriptions yet.
        </div>
      ) : null}
      {topics.map((t) => (
        <div
          key={t}
          className={`topicItem ${selected === t ? 'topicItemActive' : ''}`}
          onClick={() => onSelect(t)}
          title={t}
        >
          <div className="topicName">{t}</div>
          <button
            className="iconButton"
            title="Unsubscribe"
            onClick={(e) => {
              e.preventDefault()
              e.stopPropagation()
              onUnsubscribe(t)
            }}
          >
            ×
          </button>
        </div>
      ))}
    </div>
  )
}


