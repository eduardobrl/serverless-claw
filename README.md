# ServerlessClaw

Projeto serverless para AWS com arquitetura coreografada por eventos, usando TypeScript nas Lambdas.

## Arquitetura

- API Gateway expõe `POST /telegram/webhook`.
- Lambda `telegram-ingress` recebe o webhook do Telegram.
- A ingress aplica idempotência com DynamoDB (`PK = tenantId#telegram#update_id`, TTL padrão de 48h).
- A Lambda normaliza o evento e publica no SNS `domain-events`.
- Cada serviço consumidor usa o padrão fan-out: **SQS inscrita no SNS** e Lambda consumindo a fila.
- Exemplo neste projeto: `domain-events (SNS) -> event-logger-queue (SQS) -> event-logger (Lambda)`.

## Estrutura

- `template.yaml`: infraestrutura AWS SAM.
- `src/ingress/app.ts`: Lambda de ingressão e normalização.
- `src/consumer_logger/app.ts`: consumidor exemplo lendo eventos via SQS.
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
- Informe `TelegramSecretToken` (opcional, mas recomendado).
- Confirme permissões IAM.

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

- A ingress tenta gravar um item na tabela DynamoDB com `ConditionExpression: attribute_not_exists(PK)`.
- Se o `Put` funcionar: segue fluxo e publica no SNS.
- Se der `ConditionalCheckFailedException`: update duplicado, retorna `200 OK` e não publica novamente.
- `status` inicia como `RECEIVED` e vira `ENQUEUED` após publish bem sucedido.
- TTL padrão é `48h` (parâmetro `IdempotencyTtlHours`).

Trade-off do MVP:
- Existe janela rara entre gravar `RECEIVED` e publicar no SNS. Se falhar nesse meio, o evento pode não ser enfileirado automaticamente.
- Mitigação futura: reconciliador para itens `RECEIVED` antigos.
