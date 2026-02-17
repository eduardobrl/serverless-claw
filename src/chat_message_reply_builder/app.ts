import { SQSBatchResponse, SQSHandler, SQSRecord } from 'aws-lambda';
import { PublishCommand, SNSClient } from '@aws-sdk/client-sns';

type DomainEvent = {
  id?: string;
  version?: string;
  source?: string;
  type?: string;
  payload?: {
    message?: {
      chat?: {
        id?: number | string;
      };
    };
  };
};

type ChatMessageReplyRequestedEvent = {
  id: string;
  version: '1.0';
  source: string;
  type: 'chat.message.reply.requested';
  occurred_at: string;
  payload: {
    channel: string;
    chat_id: string | number;
    text: string;
  };
};

const snsClient = new SNSClient({});
const topicArn = process.env.SNS_TOPIC_ARN;

function logInfo(event: string, data: Record<string, unknown> = {}): void {
  console.info({
    level: 'info',
    service: 'chat_message_reply_builder',
    event,
    ...data,
  });
}

function logError(event: string, error: unknown, data: Record<string, unknown> = {}): void {
  console.error({
    level: 'error',
    service: 'chat_message_reply_builder',
    event,
    ...data,
    error: error instanceof Error ? { name: error.name, message: error.message, stack: error.stack } : error,
  });
}

function parseDomainEvent(record: SQSRecord): DomainEvent {
  logInfo('record.parse.started', { messageId: record.messageId });
  const parsedBody = JSON.parse(record.body) as { Message?: string } | DomainEvent;
  const rawMessage = 'Message' in parsedBody ? parsedBody.Message : record.body;
  if (typeof rawMessage !== 'string') {
    throw new Error('Invalid SQS message body: missing SNS Message payload');
  }
  const event = JSON.parse(rawMessage) as DomainEvent;
  logInfo('record.parse.completed', { messageId: record.messageId });
  return event;
}

function buildReplyRequestedEvent(chatId: string | number, source: string): ChatMessageReplyRequestedEvent {
  return {
    id: `chat-reply:${source}:${String(chatId)}:${Date.now()}`,
    version: '1.0',
    source,
    type: 'chat.message.reply.requested',
    occurred_at: new Date().toISOString(),
    payload: {
      channel: source,
      chat_id: chatId,
      text: 'hello world',
    },
  };
}

async function publishReplyRequested(event: ChatMessageReplyRequestedEvent): Promise<void> {
  if (!topicArn) {
    throw new Error('Missing required env var SNS_TOPIC_ARN');
  }

  await snsClient.send(
    new PublishCommand({
      TopicArn: topicArn,
      Message: JSON.stringify(event),
      MessageAttributes: {
        event_type: { DataType: 'String', StringValue: event.type },
        source: { DataType: 'String', StringValue: event.source },
        channel: { DataType: 'String', StringValue: event.payload.channel },
        event_version: { DataType: 'String', StringValue: event.version },
      },
    }),
  );
  logInfo('reply_requested.published', {
    eventId: event.id,
    eventType: event.type,
    channel: event.payload.channel,
    chatId: event.payload.chat_id,
  });
}

export const handler: SQSHandler = async (event): Promise<SQSBatchResponse> => {
  logInfo('handler.started', { recordCount: event.Records.length });
  const failures: SQSBatchResponse['batchItemFailures'] = [];

  for (const record of event.Records) {
    try {
      logInfo('record.processing.started', { messageId: record.messageId });
      const domainEvent = parseDomainEvent(record);
      const source = domainEvent.source;
      const chatId = domainEvent.payload?.message?.chat?.id;

      if (!source || !chatId) {
        logInfo('record.processing.skipped_missing_source_or_chat_id', {
          messageId: record.messageId,
          hasSource: Boolean(source),
          hasChatId: Boolean(chatId),
        });
        continue;
      }

      const replyRequested = buildReplyRequestedEvent(chatId, source);
      await publishReplyRequested(replyRequested);
      logInfo('record.processing.completed', { messageId: record.messageId, source, chatId });
    } catch (error) {
      logError('record.processing.failed', error, { messageId: record.messageId });
      failures.push({ itemIdentifier: record.messageId });
    }
  }

  logInfo('handler.completed', {
    failureCount: failures.length,
    successCount: event.Records.length - failures.length,
  });
  return { batchItemFailures: failures };
};
