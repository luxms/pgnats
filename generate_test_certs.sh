#!/bin/bash

set -e

CERT_DIR="tests/certs"
mkdir -p "$CERT_DIR"

CA_KEY="$CERT_DIR/ca.key"
CA_CERT="$CERT_DIR/ca.crt"
SERVER_KEY="$CERT_DIR/server.key"
SERVER_CSR="$CERT_DIR/server.csr"
SERVER_CERT="$CERT_DIR/server.crt"
SERVER_EXT="$CERT_DIR/server.ext"

echo "🔧 Generating Certificate Authority (CA)..."

# Generate CA key and self-signed cert
openssl genrsa -out "$CA_KEY" 2048
openssl req -x509 -new -nodes -key "$CA_KEY" -sha256 -days 3650 \
  -out "$CA_CERT" -subj "/C=US/ST=Test/L=Test/O=TestCA/CN=TestRootCA"

echo "🔧 Generating server key and CSR..."

# Generate server key and certificate signing request (CSR)
openssl genrsa -out "$SERVER_KEY" 2048
openssl req -new -key "$SERVER_KEY" -out "$SERVER_CSR" \
  -subj "/C=US/ST=Test/L=Test/O=TestServer/CN=localhost"

cat > "$SERVER_EXT" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
EOF

echo "🔧 Signing server cert with CA..."

# Sign the CSR with the CA key and cert
openssl x509 -req -in "$SERVER_CSR" -CA "$CA_CERT" -CAkey "$CA_KEY" \
  -CAcreateserial -out "$SERVER_CERT" -days 365 -sha256 -extfile "$SERVER_EXT"

echo "✅ TLS test certificates generated:"
echo "- CA:        $CA_CERT"
echo "- Server:    $SERVER_CERT"
echo "- Server Key:$SERVER_KEY"
