#!/usr/bin/env bash
# Prints a Keycloak access token.
#   scripts/get-token.sh alice        # user token (password grant, test convenience only)
#   scripts/get-token.sh bob
#   scripts/get-token.sh partner      # machine token (client credentials)
set -euo pipefail
KC=${KC:-http://localhost:8180/realms/orders-poc/protocol/openid-connect/token}
who=${1:-alice}

if [[ "$who" == "partner" ]]; then
  curl -sf "$KC" -d grant_type=client_credentials -d client_id=partner-client -d client_secret=partner-secret | jq -r .access_token
else
  # Direct password grant is enabled on gateway-ui only so scripts can get user tokens without a browser.
  curl -sf "$KC" -d grant_type=password -d client_id=gateway-ui -d client_secret=gateway-ui-secret \
    -d username="$who" -d password="$who" -d scope=openid | jq -r .access_token
fi
