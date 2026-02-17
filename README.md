# ServerlessClaw

Projeto serverless para AWS com arquitetura coreografada por eventos, usando TypeScript nas Lambdas.

## Arquitetura

- API Gateway expõe `POST /telegram/webhook`.
- Lambda `telegram-ingress` recebe o webhook do Telegram.
- A ingress aplica idempotência com DynamoDB (`PK = tenantId#telegram#update_id`, TTL padrão de 48h).
- A ingress grava evento normalizado na tabela `Outbox` via transação DynamoDB.
- `OutboxPublisherFunction` consome DynamoDB Stream da outbox e publica no SNS `domain-events`.
- Cada serviço consumidor usa o padrão fan-out: **SQS inscrita no SNS** e Lambda consumindo a fila, sempre com DLQ.
- Exemplos neste projeto:
  - `domain-events (SNS) -> event-logger-queue (SQS) -> event-logger (Lambda)`.
  - `domain-events (SNS) -> telegram-responder-queue (SQS) -> telegram-responder (Lambda)` que responde `Ola Mundo`.

## Estrutura

- `template.yaml`: infraestrutura AWS SAM.
- `src/ingress/app.ts`: Lambda de ingressão e gravação transacional (`Idempotency + Outbox`).
- `src/outbox_publisher/app.ts`: publica eventos da outbox no SNS via DynamoDB Stream.
- `src/consumer_logger/app.ts`: consumidor exemplo lendo eventos via SQS.
- `src/telegram_responder/app.ts`: consumidor que responde `Ola Mundo` ao Telegram.
- `events/telegram_update.json`: payload de teste.

## Pré-requisitos

- AWS CLI configurada (`aws configure`)
- AWS SAM CLI instalado
- Node.js 20+
- npm

## Instalação

```bash
npm install
```

## Build e deploy

```bash
sam build
sam deploy --guided
```

No deploy guiado:
- Defina `Stack Name` (ex: `telegram-choreography`).
- Confirme permissões IAM.
- Informe `TelegramBotTokenParameterName` com o nome do `SecureString` já provisionado pela governança.

## Configurar webhook no Telegram

Após o deploy, pegue o output `TelegramWebhookUrl` e execute:

```bash
curl -X POST "https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/setWebhook" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "<TELEGRAM_WEBHOOK_URL>",
    "secret_token": "<MESMO_TOKEN_DO_DEPLOY>"
  }'
```

## Teste local

```bash
sam local start-api
curl -X POST http://127.0.0.1:3000/telegram/webhook \
  -H "Content-Type: application/json" \
  -d @events/telegram_update.json
```

## Próximos serviços na coreografia

- Criar consumidores por domínio (ex: `user-service`, `billing-service`, `notification-service`).
- Usar `SNS FilterPolicy` por `event_type` para rotear eventos.
- Adicionar DLQ e observabilidade (CloudWatch Alarms + X-Ray).

## Idempotência na ingress

- A ingress executa `TransactWrite` em duas tabelas:
  - `Idempotency`: `ConditionExpression: attribute_not_exists(PK)` para bloquear duplicados.
  - `Outbox`: persiste o evento normalizado para publicação assíncrona via stream.
- Se a transação falhar por condição, o update é duplicado e a Lambda retorna `200 OK` sem novo processamento.
- TTL padrão:
  - `IdempotencyTtlHours = 48`.
  - `OutboxTtlHours = 168`.

Trade-off do MVP:
- A publicação no SNS é eventual (via outbox stream), então existe pequena latência entre ingestão e fan-out.

## Segredos

- O token do bot para a `telegram-responder` não fica em variável de ambiente.
- A stack lê um `SSM Parameter Store (SecureString)` existente.
- A governança deve criar/gerenciar esse parâmetro e informar seu nome no deploy (`TelegramBotTokenParameterName`).
