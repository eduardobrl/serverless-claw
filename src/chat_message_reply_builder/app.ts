import { SQSBatchResponse, SQSHandler, SQSRecord } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';

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

const ddbClient = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const outboxTableName = process.env.OUTBOX_TABLE_NAME;
const tenantId = process.env.TENANT_ID || 'default';
const outboxTtlHours = Number(process.env.OUTBOX_TTL_HOURS || '168');

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

async function writeReplyRequestedToOutbox(event: ChatMessageReplyRequestedEvent): Promise<void> {
  if (!outboxTableName) {
    throw new Error('Missing required env var OUTBOX_TABLE_NAME');
  }

  const now = new Date();
  const nowUnix = Math.floor(now.getTime() / 1000);
  const ttl = nowUnix + Math.max(1, outboxTtlHours) * 3600;
  const pk = `${tenantId}#${event.source}#chat#${String(event.payload.chat_id)}`;

  await ddbClient.send(
    new PutCommand({
      TableName: outboxTableName,
      Item: {
        PK: pk,
        SK: event.id,
        tenantId,
        source: event.source,
        eventType: event.type,
        eventVersion: event.version,
        payload: event,
        createdAt: now.toISOString(),
        status: 'PENDING',
        ttl,
      },
      ConditionExpression: 'attribute_not_exists(PK) AND attribute_not_exists(SK)',
    }),
  );

  logInfo('reply_requested.outbox_written', {
    eventId: event.id,
    eventType: event.type,
    channel: event.payload.channel,
    chatId: event.payload.chat_id,
    outboxPk: pk,
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
      await writeReplyRequestedToOutbox(replyRequested);
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
