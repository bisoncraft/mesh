import type { ClientEvent, TopicName } from './types'

export function getBaseUrl(): string {
  return (import.meta as any).env?.VITE_TESTCLIENT_URL || ''
}

export type IdentityResponse = { peer_id: string; connected_node_peer_id?: string }
export type SubscriptionsResponse = string[]

async function postJson(path: string, body: unknown): Promise<void> {
  const res = await fetch(`${getBaseUrl()}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  if (!res.ok) {
    let msg = `HTTP ${res.status}`
    try {
      const j = (await res.json()) as any
      if (typeof j?.error === 'string') msg = j.error
    } catch {}
    throw new Error(msg)
  }
}

export async function subscribe(topic: TopicName): Promise<void> {
  await postJson('/subscribe', { topic })
}

export async function unsubscribe(topic: TopicName): Promise<void> {
  await postJson('/unsubscribe', { topic })
}

export async function broadcast(topic: TopicName, dataBase64: string): Promise<void> {
  await postJson('/broadcast', { topic, data: dataBase64 })
}

export async function getEvents(afterId?: number): Promise<ClientEvent[]> {
  const url = new URL(`${getBaseUrl()}/events`, window.location.origin)
  if (afterId && afterId > 0) url.searchParams.set('after', String(afterId))
  const res = await fetch(url.toString())
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return (await res.json()) as ClientEvent[]
}

export async function getIdentity(): Promise<IdentityResponse> {
  const url = new URL(`${getBaseUrl()}/identity`, window.location.origin)
  const res = await fetch(url.toString())
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return (await res.json()) as IdentityResponse
}

export async function getSubscriptions(): Promise<SubscriptionsResponse> {
  const url = new URL(`${getBaseUrl()}/subscriptions`, window.location.origin)
  const res = await fetch(url.toString())
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return (await res.json()) as SubscriptionsResponse
}


