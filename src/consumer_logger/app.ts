import { SQSHandler } from 'aws-lambda';

type DomainEvent = {
  id?: string;
  type?: string;
  source?: string;
};

export const handler: SQSHandler = async (event) => {
  for (const record of event.Records) {
    const parsedBody = JSON.parse(record.body) as { Message?: string } | DomainEvent;
    const rawMessage = 'Message' in parsedBody ? parsedBody.Message : record.body;
    const domainEvent = JSON.parse(rawMessage) as DomainEvent;

    console.log(
      JSON.stringify({
        message: 'domain event received',
        id: domainEvent.id,
        type: domainEvent.type,
        source: domainEvent.source,
      }),
    );
  }
};
