import { DynamoDBStreamEvent, DynamoDBStreamHandler } from 'aws-lambda';
import { PublishCommand, SNSClient } from '@aws-sdk/client-sns';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { unmarshall } from '@aws-sdk/util-dynamodb';

type OutboxItem = {
  PK: string;
  SK: string;
  eventType: string;
  source: string;
  eventVersion: string;
  payload: unknown;
  status?: string;
};

const snsClient = new SNSClient({});
const ddbClient = DynamoDBDocumentClient.from(new DynamoDBClient({}));

const topicArn = process.env.SNS_TOPIC_ARN;
const outboxTableName = process.env.OUTBOX_TABLE_NAME;

async function processRecord(record: DynamoDBStreamEvent['Records'][number]): Promise<void> {
  if (record.eventName !== 'INSERT') {
    return;
  }

  const newImage = record.dynamodb?.NewImage;
  if (!newImage) {
    return;
  }

  const item = unmarshall(newImage) as OutboxItem;
  if (item.status !== 'PENDING') {
    return;
  }

  await snsClient.send(
    new PublishCommand({
      TopicArn: topicArn,
      Message: JSON.stringify(item.payload),
      MessageAttributes: {
        event_type: { DataType: 'String', StringValue: item.eventType },
        source: { DataType: 'String', StringValue: item.source },
        event_version: { DataType: 'String', StringValue: item.eventVersion },
      },
    }),
  );

  await ddbClient.send(
    new UpdateCommand({
      TableName: outboxTableName,
      Key: { PK: item.PK, SK: item.SK },
      UpdateExpression: 'SET #status = :status, publishedAt = :publishedAt',
      ExpressionAttributeNames: { '#status': 'status' },
      ConditionExpression: '#status = :pending',
      ExpressionAttributeValues: {
        ':status': 'PUBLISHED',
        ':publishedAt': new Date().toISOString(),
        ':pending': 'PENDING',
      },
    }),
  );
}

export const handler: DynamoDBStreamHandler = async (event) => {
  if (!topicArn || !outboxTableName) {
    throw new Error('Missing required env vars SNS_TOPIC_ARN or OUTBOX_TABLE_NAME');
  }

  for (const record of event.Records) {
    await processRecord(record);
  }
};
