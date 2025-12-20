import { useCallback, useMemo, useRef, useState } from 'react'
import type { ChatMessage, ClientEvent, TopicName } from '../api/types'
import { fromBase64Utf8 } from '../utils/base64'

type ChatState = Record<TopicName, ChatMessage[]>

function makeId(): string {
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`
}

export function useChat() {
  const [messagesByTopic, setMessagesByTopic] = useState<ChatState>({})
  const seenServerEventIds = useRef<Set<number>>(new Set())

  const applyServerEvent = useCallback((evt: ClientEvent, subscribedTopics: TopicName[]) => {
    if (typeof evt.id !== 'number') return
    if (seenServerEventIds.current.has(evt.id)) return
    seenServerEventIds.current.add(evt.id)

    const topic = evt.topic
    if (!topic) return
    if (subscribedTopics.length > 0 && !subscribedTopics.includes(topic)) return

    const peer = evt.peer || 'peer'
    const data = evt.data_b64

    if (evt.type === 'data' || evt.type === 'broadcast') {
      const text = data ? safeDecodeBase64(data) : ''
      const dir: ChatMessage['direction'] = evt.type === 'broadcast' ? 'out' : 'in'
      const msg: ChatMessage = {
        id: makeId(),
        topic,
        peer: dir === 'out' ? 'me' : peer,
        text,
        receivedAt: parseAt(evt.at) ?? Date.now(),
        direction: dir,
        rawBase64: data,
        serverEventId: evt.id,
      }
      setMessagesByTopic((prev) => ({ ...prev, [topic]: [...(prev[topic] ?? []), msg] }))
      return
    }

    if (evt.type === 'peer_subscribed' || evt.type === 'peer_unsubscribed') {
      const verb = evt.type === 'peer_subscribed' ? 'subscribed' : 'unsubscribed'
      const msg: ChatMessage = {
        id: makeId(),
        topic,
        peer: 'system',
        text: `${peer} ${verb}`,
        receivedAt: parseAt(evt.at) ?? Date.now(),
        direction: 'system',
        serverEventId: evt.id,
      }
      setMessagesByTopic((prev) => ({ ...prev, [topic]: [...(prev[topic] ?? []), msg] }))
    }
  }, [])

  const loadServerEvents = useCallback((events: ClientEvent[], subscribedTopics: TopicName[]) => {
    const next: ChatState = {}
    const seen = new Set<number>()

    for (const evt of events) {
      if (typeof evt.id !== 'number') continue
      if (seen.has(evt.id)) continue
      seen.add(evt.id)

      const topic = evt.topic
      if (!topic) continue
      if (subscribedTopics.length > 0 && !subscribedTopics.includes(topic)) continue

      if (evt.type === 'data' || evt.type === 'broadcast') {
        const peer = evt.peer || 'peer'
        const data = evt.data_b64
        const text = data ? safeDecodeBase64(data) : ''
        const dir: ChatMessage['direction'] = evt.type === 'broadcast' ? 'out' : 'in'
        const msg: ChatMessage = {
          id: makeId(),
          topic,
          peer: dir === 'out' ? 'me' : peer,
          text,
          receivedAt: parseAt(evt.at) ?? Date.now(),
          direction: dir,
          rawBase64: data,
          serverEventId: evt.id,
        }
        next[topic] = [...(next[topic] ?? []), msg]
        continue
      }

      if (evt.type === 'peer_subscribed' || evt.type === 'peer_unsubscribed') {
        const peer = evt.peer || 'peer'
        const verb = evt.type === 'peer_subscribed' ? 'subscribed' : 'unsubscribed'
        const msg: ChatMessage = {
          id: makeId(),
          topic,
          peer: 'system',
          text: `${peer} ${verb}`,
          receivedAt: parseAt(evt.at) ?? Date.now(),
          direction: 'system',
          serverEventId: evt.id,
        }
        next[topic] = [...(next[topic] ?? []), msg]
      }
    }

    seenServerEventIds.current = seen
    setMessagesByTopic(next)
  }, [])

  const getMessages = useCallback((topic?: TopicName) => (topic ? messagesByTopic[topic] ?? [] : []), [
    messagesByTopic,
  ])

  const totalCount = useMemo(
    () => Object.values(messagesByTopic).reduce((acc, xs) => acc + xs.length, 0),
    [messagesByTopic],
  )

  return { applyServerEvent, loadServerEvents, getMessages, totalCount }
}

function safeDecodeBase64(dataBase64: string): string {
  try {
    return fromBase64Utf8(dataBase64)
  } catch {
    return `[base64] ${dataBase64}`
  }
}

function parseAt(at?: string): number | null {
  if (!at) return null
  const ms = Date.parse(at)
  return Number.isFinite(ms) ? ms : null
}


