import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';

type JsonRpcRequest = {
  jsonrpc?: string;
  id?: string | number | null;
  method?: string;
  params?: unknown;
};

type PublishChatReplyArgs = {
  source?: string;
  chat_id?: string | number;
  text?: string;
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
const serverName = 'serverlessclaw-mcp';
const serverVersion = '1.0.0';

function logInfo(event: string, data: Record<string, unknown> = {}): void {
  console.info({
    level: 'info',
    service: 'mcp_server',
    event,
    ...data,
  });
}

function logError(event: string, error: unknown, data: Record<string, unknown> = {}): void {
  console.error({
    level: 'error',
    service: 'mcp_server',
    event,
    ...data,
    error: error instanceof Error ? { name: error.name, message: error.message, stack: error.stack } : error,
  });
}

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

function jsonRpcResult(id: JsonRpcRequest['id'], result: object): APIGatewayProxyResult {
  return response(200, {
    jsonrpc: '2.0',
    id: id ?? null,
    result,
  });
}

function jsonRpcError(
  id: JsonRpcRequest['id'],
  code: number,
  message: string,
  data?: Record<string, unknown>,
): APIGatewayProxyResult {
  return response(200, {
    jsonrpc: '2.0',
    id: id ?? null,
    error: {
      code,
      message,
      ...(data ? { data } : {}),
    },
  });
}

function buildReplyEvent(args: Required<PublishChatReplyArgs>): ChatMessageReplyRequestedEvent {
  return {
    id: `mcp-reply:${args.source}:${String(args.chat_id)}:${Date.now()}`,
    version: '1.0',
    source: args.source,
    type: 'chat.message.reply.requested',
    occurred_at: new Date().toISOString(),
    payload: {
      channel: args.source,
      chat_id: args.chat_id,
      text: args.text,
    },
  };
}

async function enqueueChatReplyRequested(args: PublishChatReplyArgs): Promise<ChatMessageReplyRequestedEvent> {
  if (!outboxTableName) {
    throw new Error('Missing required env var OUTBOX_TABLE_NAME');
  }

  const source = args.source;
  const chatId = args.chat_id;
  const text = args.text;

  if (!source || !chatId || !text) {
    throw new Error('Invalid arguments: source, chat_id and text are required');
  }

  const replyEvent = buildReplyEvent({
    source,
    chat_id: chatId,
    text,
  });

  const now = new Date();
  const nowUnix = Math.floor(now.getTime() / 1000);
  const ttl = nowUnix + Math.max(1, outboxTtlHours) * 3600;
  const pk = `${tenantId}#${replyEvent.source}#chat#${String(replyEvent.payload.chat_id)}`;

  await ddbClient.send(
    new PutCommand({
      TableName: outboxTableName,
      Item: {
        PK: pk,
        SK: replyEvent.id,
        tenantId,
        source: replyEvent.source,
        eventType: replyEvent.type,
        eventVersion: replyEvent.version,
        payload: replyEvent,
        createdAt: now.toISOString(),
        status: 'PENDING',
        ttl,
      },
      ConditionExpression: 'attribute_not_exists(PK) AND attribute_not_exists(SK)',
    }),
  );

  return replyEvent;
}

export const handler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  const requestId = event.requestContext.requestId;
  logInfo('request.received', { requestId, path: event.path, method: event.httpMethod });

  let request: JsonRpcRequest;
  try {
    request = JSON.parse(decodeBody(event)) as JsonRpcRequest;
  } catch (error) {
    logError('request.invalid_json', error, { requestId });
    return jsonRpcError(null, -32700, 'Parse error');
  }

  const method = request.method;
  const id = request.id ?? null;
  if (!method || request.jsonrpc !== '2.0') {
    return jsonRpcError(id, -32600, 'Invalid Request');
  }

  try {
    if (method === 'initialize') {
      return jsonRpcResult(id, {
        protocolVersion: '2024-11-05',
        serverInfo: {
          name: serverName,
          version: serverVersion,
        },
        capabilities: {
          tools: {},
        },
      });
    }

    if (method === 'tools/list') {
      return jsonRpcResult(id, {
        tools: [
          {
            name: 'publish_chat_reply_requested',
            description: 'Writes chat.message.reply.requested into outbox with the informed source/chat/text.',
            inputSchema: {
              type: 'object',
              properties: {
                source: { type: 'string', description: 'Message channel/source, e.g. telegram.' },
                chat_id: {
                  oneOf: [{ type: 'string' }, { type: 'number' }],
                  description: 'Chat identifier for the target channel.',
                },
                text: { type: 'string', description: 'Text to be sent by the channel responder.' },
              },
              required: ['source', 'chat_id', 'text'],
            },
          },
        ],
      });
    }

    if (method === 'tools/call') {
      const params = (request.params || {}) as { name?: string; arguments?: PublishChatReplyArgs };
      if (params.name !== 'publish_chat_reply_requested') {
        return jsonRpcError(id, -32601, 'Method not found', { tool: params.name || null });
      }

      const publishedEvent = await enqueueChatReplyRequested(params.arguments || {});
      logInfo('tool.publish_chat_reply_requested.completed', {
        requestId,
        source: publishedEvent.source,
        chatId: publishedEvent.payload.chat_id,
        eventId: publishedEvent.id,
      });

      return jsonRpcResult(id, {
        content: [
          {
            type: 'text',
            text: `Enqueued ${publishedEvent.type} in outbox for source=${publishedEvent.source} chat_id=${String(
              publishedEvent.payload.chat_id,
            )}`,
          },
        ],
      });
    }

    return jsonRpcError(id, -32601, 'Method not found', { method });
  } catch (error) {
    logError('request.failed', error, { requestId, method });
    return jsonRpcError(id, -32603, 'Internal error');
  }
};
