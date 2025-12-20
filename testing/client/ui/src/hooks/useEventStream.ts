import { useEffect, useMemo, useRef, useState } from 'react'
import { getBaseUrl } from '../api/testclient'
import type { ClientEvent } from '../api/types'

export type EventStreamStatus = 'connecting' | 'open' | 'closed' | 'error'

export function useEventStream(opts: { afterId?: number; onEvent: (evt: ClientEvent) => void }) {
  const { afterId, onEvent } = opts
  const [status, setStatus] = useState<EventStreamStatus>('connecting')
  const baseUrl = useMemo(() => getBaseUrl(), [])
  const onEventRef = useRef(onEvent)
  onEventRef.current = onEvent

  useEffect(() => {
    const url = new URL(`${baseUrl}/stream/events`, window.location.origin)
    if (afterId && afterId > 0) url.searchParams.set('after', String(afterId))
    const es = new EventSource(url.toString())
    setStatus('connecting')

    es.onopen = () => setStatus('open')
    es.onerror = () => setStatus('error')

    const handler = (e: MessageEvent) => {
      try {
        const parsed = JSON.parse(e.data ?? '{}') as ClientEvent
        if (typeof parsed?.id === 'number') onEventRef.current(parsed)
      } catch {
        // ignore
      }
    }
    es.addEventListener('tatanka', handler as any)

    return () => {
      es.close()
      setStatus('closed')
    }
  }, [baseUrl, afterId])

  return { status, baseUrl }
}


