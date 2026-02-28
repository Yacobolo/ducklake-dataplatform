#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-before}"
FILE="internal/api/handler_catalogs.go"

if [[ "$MODE" == "before" ]]; then
  echo "[repro-before] Expecting buggy 403 response type in ValidationError branch"
  grep -q 'SetDefaultCatalog403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 400' "$FILE"
  echo "Bug reproduced: validation is mapped to 403 response type with code 400 body."
elif [[ "$MODE" == "after" ]]; then
  echo "[verify-after] Expecting 400 response type in ValidationError branch"
  grep -q 'SetDefaultCatalog400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400' "$FILE"
  if grep -q 'SetDefaultCatalog403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 400' "$FILE"; then
    echo "Unexpected old buggy mapping still present"
    exit 1
  fi
  echo "Fix verified: ValidationError now maps to 400 response type."
else
  echo "Usage: $0 [before|after]"
  exit 2
fi
