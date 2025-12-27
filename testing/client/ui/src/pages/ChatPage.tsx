import { useCallback, useEffect, useMemo, useState } from 'react'
import { broadcast, getEvents, getIdentity, getSubscriptions, subscribe, unsubscribe } from '../api/testclient'
import type { ClientEvent } from '../api/types'
import { Chat } from '../components/Chat'
import { Sidebar } from '../components/Sidebar'
import { SubscribeDialog } from '../components/SubscribeDialog'
import { useChat } from '../hooks/useChat'
import { useEventStream } from '../hooks/useEventStream'
import { useTopics } from '../hooks/useTopics'
import { toBase64Utf8 } from '../utils/base64'

export function ChatPage() {
  const { topics, selected, setSelected, addTopic, removeTopic, replaceTopics } = useTopics()
  const { applyServerEvent, loadServerEvents, getMessages } = useChat()

  const [subscribeOpen, setSubscribeOpen] = useState(false)
  const [lastError, setLastError] = useState<string | null>(null)
  const [afterId, setAfterId] = useState<number>(0)
  const [peerId, setPeerId] = useState<string>('')
  const [connectedNode, setConnectedNode] = useState<string>('')

  useEffect(() => {
    let cancelled = false
    let timer: number | undefined

    const loadIdentity = async () => {
      try {
        const id = await getIdentity()
        if (cancelled) return
        setPeerId(id.peer_id)
        setConnectedNode(id.connected_node_peer_id || '')
      } catch {
        if (!cancelled) {
          setPeerId('')
          setConnectedNode('')
        }
      }
    }

    void loadIdentity()
    timer = window.setInterval(loadIdentity, 1000)

    return () => {
      cancelled = true
      if (timer !== undefined) {
        window.clearInterval(timer)
      }
    }
  }, [])

  // Sync the sidebar "subscribed topics" to the server's current subscriptions.
  // This prevents stale localStorage topics from persisting across test-client restarts.
  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const serverTopics = await getSubscriptions()
        if (cancelled) return
        replaceTopics(serverTopics)
      } catch {
        // If the server isn't reachable yet, do nothing and keep local state.
      }
    })()
    return () => {
      cancelled = true
    }
  }, [replaceTopics])

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const events = await getEvents()
        if (cancelled) return
        loadServerEvents(events, topics)
        const max = Math.max(0, ...events.map((e) => e.id ?? 0))
        setAfterId(max)
      } catch (e: any) {
        if (!cancelled) setLastError(e?.message || 'Failed to load events')
      }
    })()
    return () => {
      cancelled = true
    }
  }, [loadServerEvents, topics])

  const onServerEvent = useCallback(
    (evt: ClientEvent) => {
      applyServerEvent(evt, topics)
    },
    [applyServerEvent, topics],
  )

  const { status: streamStatus, baseUrl } = useEventStream({ afterId, onEvent: onServerEvent })

  const messages = useMemo(() => getMessages(selected), [getMessages, selected])

  return (
    <div className="shell">
      <Sidebar
        topics={topics}
        selected={selected}
        onSelect={setSelected}
        onUnsubscribe={async (topic) => {
          setLastError(null)
          try {
            await unsubscribe(topic)
            removeTopic(topic)
          } catch (e: any) {
            setLastError(e?.message || 'Failed to unsubscribe')
          }
        }}
        onOpenSubscribe={() => setSubscribeOpen(true)}
        baseUrl={baseUrl}
        streamStatus={streamStatus}
        peerId={peerId}
        connectedNode={connectedNode}
      />

      <main className="main">
        <div className="topbar">
          <div className="chatTitle">
            <strong>{selected ? selected : 'No topic selected'}</strong>
            <span className="muted" style={{ fontSize: 12 }}>
              {selected ? null : 'Subscribe to a topic to begin'}
            </span>
          </div>
          <div className="muted" style={{ fontSize: 12, textAlign: 'right' }}>
            {lastError ? <span style={{ color: 'var(--danger)' }}>{lastError}</span> : <span>&nbsp;</span>}
          </div>
        </div>

        <Chat
          topic={selected}
          messages={messages}
          onSend={async (text) => {
            if (!selected) return
            setLastError(null)
            await broadcast(selected, toBase64Utf8(text))
          }}
        />
      </main>

      <SubscribeDialog
        open={subscribeOpen}
        onClose={() => setSubscribeOpen(false)}
        onSubscribe={async (topic) => {
          setLastError(null)
          await subscribe(topic)
          addTopic(topic)
        }}
      />
    </div>
  )
}


