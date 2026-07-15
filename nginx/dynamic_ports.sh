#!/bin/bash

echo "CAPIF_ENABLED: ${CAPIF_ENABLED}"

# Defaults match the docker-compose service name/port; Kubernetes overrides
# these via the release-prefixed backend Service name/port (see helm-chart).
export NGINX_BACKEND_HOST="${NGINX_BACKEND_HOST:-backend}"
export NGINX_BACKEND_PORT="${NGINX_BACKEND_PORT:-80}"

if [ "${CAPIF_ENABLED}" = "true" ]; then
    : "${CAPIF_USERNAME:?ERROR: CAPIF_USERNAME is not set}"
    
    export NGINX_SSL_CERT_PATH="/etc/nginx/provider-certs/${CAPIF_USERNAME}/aef-1.crt"
    export NGINX_SSL_CERT_KEY_PATH="/etc/nginx/provider-certs/${CAPIF_USERNAME}/AEF-1_private_key.key"
    export NGINX_SSL_CLIENT_CERT_PATH="/etc/nginx/provider-certs/${CAPIF_USERNAME}/ca.crt"
    export NGINX_SSL_VERIFY_CLIENT='optional'
    export NGINX_TLS_CONDITION='!= SUCCESS'
else
    # FOR DEVELOPMENT PURPOSES (TLS not enforced and certs irrelevant)
    export NGINX_SSL_CERT_PATH="/etc/nginx/certs/self_signed_nef.pem"
    export NGINX_SSL_CERT_KEY_PATH="/etc/nginx/certs/private_nef.pem"
    export NGINX_SSL_CLIENT_CERT_PATH="/etc/nginx/certs/self_signed_nef.pem"
    export NGINX_SSL_VERIFY_CLIENT='off'
    export NGINX_TLS_CONDITION='= NEVER'
fi

# After using envsubst, it will yield the build-in nginx variables. Exporting DOLLAR="$" solves the problem.
export DOLLAR="$"
envsubst < /etc/nginx/conf.d/app.conf > /etc/nginx/conf.d/default.conf  
unset DOLLAR 
rm -f /etc/nginx/conf.d/app.conf
nginx -g "daemon off;"
