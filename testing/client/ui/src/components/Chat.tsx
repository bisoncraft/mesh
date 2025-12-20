import type { ChatMessage, TopicName } from '../api/types'
import { MessageComposer } from './MessageComposer'
import { MessageList } from './MessageList'

export function Chat(props: { topic?: TopicName; messages: ChatMessage[]; onSend: (text: string) => Promise<void> }) {
  const { topic, messages, onSend } = props
  return (
    <section className="chat">
      <MessageList messages={messages} />
      <MessageComposer disabled={!topic} onSend={onSend} />
    </section>
  )
}


