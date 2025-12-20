export type TopicName = string

export type ClientEventType =
  | 'data'
  | 'broadcast'
  | 'subscribed'
  | 'unsubscribed'
  | 'peer_subscribed'
  | 'peer_unsubscribed'

export type ClientEvent = {
  id: number
  at: string
  type: ClientEventType
  topic?: TopicName
  peer?: string
  data_b64?: string
  message?: string
}

export type ChatMessage = {
  id: string
  topic: TopicName
  peer: string
  text: string
  receivedAt: number
  direction: 'in' | 'out' | 'system'
  rawBase64?: string
  serverEventId?: number
}


