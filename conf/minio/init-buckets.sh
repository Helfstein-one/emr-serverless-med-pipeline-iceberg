#!/bin/bash
# ============================================================
# Inicialização dos Buckets no MinIO
# Executado pelo container minio-init no startup
# ============================================================
set -euo pipefail

echo "⏳ Aguardando MinIO ficar disponível..."
until mc alias set local http://minio:9000 minioadmin minioadmin 2>/dev/null; do
  echo "   MinIO ainda não está pronto, tentando novamente em 2s..."
  sleep 2
done

echo "✅ MinIO disponível!"

# Cria os buckets se não existirem
for BUCKET in bronze silver gold warehouse; do
  if mc ls local/${BUCKET} > /dev/null 2>&1; then
    echo "ℹ️  Bucket '${BUCKET}' já existe."
  else
    mc mb local/${BUCKET}
    echo "✅ Bucket '${BUCKET}' criado com sucesso."
  fi
done

echo ""
echo "📦 Buckets disponíveis:"
mc ls local/

echo ""
echo "🎉 Inicialização do MinIO concluída!"
