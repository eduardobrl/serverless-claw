import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { createHash } from 'node:crypto';

type TelegramUpdate = Record<string, unknown>;

type DomainEvent = {
  id: string;
  version: '1.0';
  source: 'telegram';
  type: string;
  occurred_at: string;
  payload: TelegramUpdate;
};

const ddbClient = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const expectedSecret = process.env.TELEGRAM_SECRET_TOKEN || '';
const idempotencyTableName = process.env.IDEMPOTENCY_TABLE_NAME;
const outboxTableName = process.env.OUTBOX_TABLE_NAME;
const tenantId = process.env.TENANT_ID || 'default';
const idempotencyTtlHours = Number(process.env.IDEMPOTENCY_TTL_HOURS || '48');
const outboxTtlHours = Number(process.env.OUTBOX_TTL_HOURS || '168');

function response(statusCode: number, body: object): APIGatewayProxyResult {
  return {
    statusCode,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  };
}

function decodeBody(event: APIGatewayProxyEvent): string {
  const body = event.body || '';
  return event.isBase64Encoded ? Buffer.from(body, 'base64').toString('utf-8') : body;
}

function header(event: APIGatewayProxyEvent, key: string): string {
  const headers = event.headers || {};

  for (const [k, v] of Object.entries(headers)) {
    if (k.toLowerCase() === key.toLowerCase()) {
      return v || '';
    }
  }

  return '';
}

function inferEventType(update: TelegramUpdate): string {
  const knownTypes = [
    'message',
    'edited_message',
    'channel_post',
    'edited_channel_post',
    'inline_query',
    'chosen_inline_result',
    'callback_query',
    'shipping_query',
    'pre_checkout_query',
    'poll',
    'poll_answer',
    'my_chat_member',
    'chat_member',
    'chat_join_request',
  ];

  for (const type of knownTypes) {
    if (type in update) {
      return type;
    }
  }

  return 'unknown';
}

function buildEventId(update: TelegramUpdate): string {
  const updateId = update.update_id;
  if (typeof updateId === 'number' || typeof updateId === 'string') {
    return `telegram:${String(updateId)}`;
  }

  const rawHash = createHash('sha256').update(JSON.stringify(update)).digest('hex');
  return `telegram:hash:${rawHash}`;
}

function buildIdempotencyPk(update: TelegramUpdate): string {
  const updateId = update.update_id;
  if (typeof updateId === 'number' || typeof updateId === 'string') {
    return `${tenantId}#telegram#${String(updateId)}`;
  }

  const rawHash = createHash('sha256').update(JSON.stringify(update)).digest('hex');
  return `${tenantId}#telegram#hash:${rawHash}`;
}

function normalize(update: TelegramUpdate): DomainEvent {
  return {
    id: buildEventId(update),
    version: '1.0',
    source: 'telegram',
    type: inferEventType(update),
    occurred_at: new Date().toISOString(),
    payload: update,
  };
}

function isDuplicateTransaction(error: unknown): boolean {
  if ((error as { name?: string }).name !== 'TransactionCanceledException') {
    return false;
  }

  const reasons = (error as { CancellationReasons?: Array<{ Code?: string }> }).CancellationReasons;
  if (!Array.isArray(reasons)) {
    return true;
  }

  return reasons.some((reason) => reason.Code === 'ConditionalCheckFailed');
}

export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  if (!idempotencyTableName || !outboxTableName) {
    console.error('Missing required env vars', {
      hasIdempotencyTableName: Boolean(idempotencyTableName),
      hasOutboxTableName: Boolean(outboxTableName),
    });
    return response(500, { ok: false, error: 'server misconfiguration' });
  }

  if (expectedSecret) {
    const incomingSecret = header(event, 'X-Telegram-Bot-Api-Secret-Token');
    if (incomingSecret !== expectedSecret) {
      return response(401, { ok: false, error: 'invalid secret token' });
    }
  }

  let update: TelegramUpdate;
  try {
    update = JSON.parse(decodeBody(event));
  } catch {
    return response(400, { ok: false, error: 'invalid json' });
  }

  const normalized = normalize(update);
  const idempotencyPk = buildIdempotencyPk(update);
  const now = new Date();
  const nowUnix = Math.floor(now.getTime() / 1000);
  const idempotencyTtl = nowUnix + Math.max(1, idempotencyTtlHours) * 3600;
  const outboxTtl = nowUnix + Math.max(1, outboxTtlHours) * 3600;
  const outboxSk = normalized.id;

  try {
    await ddbClient.send(
      new TransactWriteCommand({
        TransactItems: [
          {
            Put: {
              TableName: idempotencyTableName,
              Item: {
                PK: idempotencyPk,
                firstSeenAt: now.toISOString(),
                ttl: idempotencyTtl,
                status: 'RECEIVED',
              },
              ConditionExpression: 'attribute_not_exists(PK)',
            },
          },
          {
            Put: {
              TableName: outboxTableName,
              Item: {
                PK: idempotencyPk,
                SK: outboxSk,
                tenantId,
                source: normalized.source,
                eventType: normalized.type,
                eventVersion: normalized.version,
                payload: normalized,
                createdAt: now.toISOString(),
                status: 'PENDING',
                ttl: outboxTtl,
              },
              ConditionExpression: 'attribute_not_exists(PK) AND attribute_not_exists(SK)',
            },
          },
        ],
      }),
    );
  } catch (error) {
    if (isDuplicateTransaction(error)) {
      return response(200, { ok: true, duplicate: true });
    }
    throw error;
  }

  return response(200, { ok: true });
};
