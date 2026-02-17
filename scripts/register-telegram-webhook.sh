#!/usr/bin/env bash

set -euo pipefail

prompt_required() {
  local label="$1"
  local value=""

  while [[ -z "${value}" ]]; do
    read -r -p "${label}: " value
  done

  printf "%s" "${value}"
}

echo "Telegram webhook registration"
echo

BOT_TOKEN="$(prompt_required "Enter Telegram bot token")"
WEBHOOK_URL="$(prompt_required "Enter webhook URL (https://...)")"

read -r -p "Enter secret token (optional, press Enter to skip): " SECRET_TOKEN

if [[ -n "${SECRET_TOKEN}" ]]; then
  PAYLOAD="$(cat <<JSON
{"url":"${WEBHOOK_URL}","secret_token":"${SECRET_TOKEN}"}
JSON
)"
else
  PAYLOAD="$(cat <<JSON
{"url":"${WEBHOOK_URL}"}
JSON
)"
fi

echo
echo "Registering webhook..."
curl -sS -X POST "https://api.telegram.org/bot${BOT_TOKEN}/setWebhook" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}"

echo
echo
echo "Webhook info:"
curl -sS "https://api.telegram.org/bot${BOT_TOKEN}/getWebhookInfo"
echo
