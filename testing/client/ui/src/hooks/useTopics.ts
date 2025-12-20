import { useCallback, useMemo, useState } from 'react'
import type { TopicName } from '../api/types'

type TopicsState = {
  topics: TopicName[]
  selected?: TopicName
}

export function useTopics() {
  const [state, setState] = useState<TopicsState>({
    topics: [],
    selected: undefined,
  })

  const topics = state.topics
  const selected = state.selected

  const setSelected = useCallback(
    (topic: TopicName) => setState({ ...state, selected: topic }),
    [setState, state],
  )

  const addTopic = useCallback(
    (topic: TopicName) => {
      const nextTopics = Array.from(new Set([...state.topics, topic])).sort()
      setState({ topics: nextTopics, selected: topic })
    },
    [setState, state.topics],
  )

  const removeTopic = useCallback(
    (topic: TopicName) => {
      const nextTopics = state.topics.filter((t) => t !== topic)
      const nextSelected =
        state.selected === topic ? (nextTopics[0] ? nextTopics[0] : undefined) : state.selected
      setState({ topics: nextTopics, selected: nextSelected })
    },
    [setState, state.topics, state.selected],
  )

  const replaceTopics = useCallback(
    (topics: TopicName[]) => {
      const nextTopics = Array.from(new Set(topics)).sort()
      const nextSelected =
        state.selected && nextTopics.includes(state.selected) ? state.selected : nextTopics[0] || undefined
      setState({ topics: nextTopics, selected: nextSelected })
    },
    [setState, state.selected],
  )

  const hasTopics = useMemo(() => topics.length > 0, [topics.length])

  return { topics, selected, setSelected, addTopic, removeTopic, replaceTopics, hasTopics }
}


