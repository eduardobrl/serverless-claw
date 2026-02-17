import { SQSBatchResponse, SQSHandler, SQSRecord } from 'aws-lambda';
import { GetParameterCommand, SSMClient } from '@aws-sdk/client-ssm';

type DomainEvent = {
  payload?: {
    message?: {
      chat?: {
        id?: number | string;
      };
    };
  };
};

const ssmClient = new SSMClient({});
const telegramBotTokenParameterName = process.env.TELEGRAM_BOT_TOKEN_PARAMETER_NAME;
let cachedTelegramBotToken: string | undefined;

function logInfo(event: string, data: Record<string, unknown> = {}): void {
  console.info({
    level: 'info',
    service: 'telegram_responder',
    event,
    ...data,
  });
}

function logError(event: string, error: unknown, data: Record<string, unknown> = {}): void {
  console.error({
    level: 'error',
    service: 'telegram_responder',
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

async function sendHelloWorld(chatId: string | number): Promise<void> {
  logInfo('telegram.send_message.started', { chatId });
  const telegramBotToken = await getTelegramBotToken();

  const response = await fetch(`https://api.telegram.org/bot${telegramBotToken}/sendMessage`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      chat_id: chatId,
      text: 'Ola Mundo',
    }),
  });

  if (!response.ok) {
    const body = await response.text();
    logError('telegram.send_message.failed', new Error('Telegram sendMessage non-200 response'), {
      chatId,
      statusCode: response.status,
      responseBody: body,
    });
    throw new Error(`Telegram sendMessage failed (${response.status}): ${body}`);
  }

  logInfo('telegram.send_message.completed', { chatId, statusCode: response.status });
}

async function getTelegramBotToken(): Promise<string> {
  if (cachedTelegramBotToken) {
    logInfo('ssm.telegram_token.cache_hit');
    return cachedTelegramBotToken;
  }

  if (!telegramBotTokenParameterName) {
    logError('ssm.telegram_token.env_missing', new Error('Missing TELEGRAM_BOT_TOKEN_PARAMETER_NAME env var'));
    throw new Error('Missing TELEGRAM_BOT_TOKEN_PARAMETER_NAME env var');
  }

  logInfo('ssm.telegram_token.fetch.started', { parameterName: telegramBotTokenParameterName });
  const response = await ssmClient.send(
    new GetParameterCommand({
      Name: telegramBotTokenParameterName,
      WithDecryption: true,
    }),
  );

  const token = response.Parameter?.Value;
  if (!token || token === 'CHANGE_ME_IN_GOVERNANCE') {
    logError('ssm.telegram_token.invalid', new Error('Telegram bot token is not configured in SSM parameter'), {
      parameterName: telegramBotTokenParameterName,
    });
    throw new Error('Telegram bot token is not configured in SSM parameter');
  }

  cachedTelegramBotToken = token;
  logInfo('ssm.telegram_token.fetch.completed', { parameterName: telegramBotTokenParameterName });
  return token;
}

export const handler: SQSHandler = async (event): Promise<SQSBatchResponse> => {
  logInfo('handler.started', { recordCount: event.Records.length });
  const failures: SQSBatchResponse['batchItemFailures'] = [];

  for (const record of event.Records) {
    try {
      logInfo('record.processing.started', { messageId: record.messageId });
      const domainEvent = parseDomainEvent(record);
      const chatId = domainEvent.payload?.message?.chat?.id;

      if (!chatId) {
        logInfo('record.processing.skipped_missing_chat_id', { messageId: record.messageId });
        continue;
      }

      logInfo('record.processing.chat_id_extracted', { messageId: record.messageId, chatId });
      await sendHelloWorld(chatId);
      logInfo('record.processing.completed', { messageId: record.messageId, chatId });
    } catch (error) {
      logError('record.processing.failed', error, {
        messageId: record.messageId,
      });
      failures.push({ itemIdentifier: record.messageId });
    }
  }

  logInfo('handler.completed', {
    failureCount: failures.length,
    successCount: event.Records.length - failures.length,
  });
  return { batchItemFailures: failures };
};
