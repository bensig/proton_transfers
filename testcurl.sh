#!/bin/bash
curl -X 'POST' \
  'https://hyperion.libre.quantumblok.com/v1/history/get_actions' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "account_name": "willy",
  "sort": "desc",
  "pos": -1,
  "offset": -1,
  "json": true
}'