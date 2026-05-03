#!/usr/bin/env bash
# ============================================================
# pipeline.sh — Script de Automação CI/CD
# ============================================================
# Executa o pipeline completo do Lakehouse de Saúde
# em ambiente Podman (MinIO + Nessie + Spark + Airflow).
#
# Stages:
#   1. SETUP    — Sobe a infra e aguarda healthchecks
#   2. INGEST   — Envia dados de amostra para o MinIO
#   3. VALIDATE — Executa Silver + testes de contrato
#   4. BUILD    — Executa Gold + gera documentação
#   5. CLEANUP  — Derruba o ambiente e limpa volumes
#
# Uso:
#   chmod +x pipeline.sh
#   ./pipeline.sh [--skip-cleanup]
#
# Flags:
#   --skip-cleanup  Não derruba o ambiente ao final
# ============================================================

set -euo pipefail

# ---- Cores para output ----
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# ---- Configuração ----
COMPOSE_FILE="podman-compose.yml"
SKIP_CLEANUP=false

for arg in "$@"; do
    case $arg in
        --skip-cleanup) SKIP_CLEANUP=true ;;
    esac
done

# ---- Funções auxiliares ----
log_stage() {
    echo -e "\n${BOLD}${BLUE}════════════════════════════════════════════════════${NC}"
    echo -e "${BOLD}${BLUE}  STAGE: $1${NC}"
    echo -e "${BOLD}${BLUE}════════════════════════════════════════════════════${NC}\n"
}

log_step() {
    echo -e "${CYAN}  ➤ $1${NC}"
}

log_success() {
    echo -e "${GREEN}  ✅ $1${NC}"
}

log_error() {
    echo -e "${RED}  ❌ $1${NC}"
}

log_warn() {
    echo -e "${YELLOW}  ⚠️  $1${NC}"
}

# Aguarda um serviço ficar disponível
wait_for_service() {
    local service=$1
    local max_retries=${2:-30}
    local retry=0

    log_step "Aguardando ${service}..."
    while [ $retry -lt $max_retries ]; do
        local status
        status=$(podman ps --filter "name=^${service}$" --format "{{.Status}}" 2>/dev/null || true)
        if [ -n "$status" ] && echo "$status" | grep -qi "Up"; then
            log_success "${service} está pronto! (${status})"
            return 0
        fi
        retry=$((retry + 1))
        echo -n "."
        sleep 3
    done
    echo ""
    log_error "${service} não ficou pronto após $((max_retries * 3))s"
    local debug_status
    debug_status=$(podman ps -a --filter "name=^${service}$" --format "{{.Names}} {{.Status}}" 2>/dev/null || true)
    log_error "Status atual: ${debug_status:-container não encontrado}"
    return 1
}

# Função de cleanup (registrada como trap)
cleanup() {
    if [ "$SKIP_CLEANUP" = true ]; then
        log_warn "Cleanup ignorado (--skip-cleanup). Use 'podman-compose down -v' para limpar."
        return
    fi
    log_stage "5/5 — CLEANUP"
    log_step "Derrubando o stack e limpando volumes..."
    podman-compose -f ${COMPOSE_FILE} down -v --remove-orphans 2>/dev/null || true
    log_success "Ambiente limpo!"
}

# Registra cleanup em caso de erro
trap cleanup EXIT


# ============================================================
# STAGE 1: SETUP — Sobe a infraestrutura
# ============================================================
log_stage "1/5 — SETUP"

log_step "Construindo imagens e subindo os serviços..."
podman-compose -f ${COMPOSE_FILE} up -d --build

log_step "Aguardando serviços ficarem saudáveis..."
wait_for_service "minio"
wait_for_service "nessie"
wait_for_service "spark"

# Aguarda minio-init completar (container que sobe, executa e encerra)
log_step "Aguardando minio-init concluir upload dos dados..."
for i in $(seq 1 30); do
    status=$(podman ps -a --filter "name=^minio-init$" --format "{{.Status}}" 2>/dev/null || true)
    if echo "$status" | grep -qi "exited"; then
        log_success "minio-init concluído!"
        break
    fi
    if [ $i -eq 30 ]; then
        log_error "minio-init não concluiu em 90s"
        exit 1
    fi
    echo -n "."
    sleep 3
done

log_success "Infraestrutura pronta!"
echo ""
echo -e "  ${CYAN}📊 MinIO Console:${NC}  http://localhost:9001 (minioadmin/minioadmin)"
echo -e "  ${CYAN}🔀 Nessie API:${NC}    http://localhost:19120/api/v1"
echo -e "  ${CYAN}⚡ Spark UI:${NC}      http://localhost:4040"
echo -e "  ${CYAN}🌬️  Airflow UI:${NC}    http://localhost:8080 (admin/admin)"


# ============================================================
# STAGE 2: INGEST — Envia dados para o MinIO
# ============================================================
log_stage "2/5 — INGEST"

log_step "Executando job de ingestão Bronze (CSV → Parquet)..."
podman exec spark \
    /opt/spark/bin/spark-submit \
    --master 'local[2]' \
    --driver-memory 1g \
    --conf spark.executor.memory=512m \
    /app/spark/jobs/ingest_bronze.py

log_success "Ingestão Bronze concluída!"


# ============================================================
# STAGE 3: VALIDATE — Silver + Testes de Contrato
# ============================================================
log_stage "3/5 — VALIDATE"

log_step "Executando promoção Silver (Parquet → Iceberg)..."
podman exec spark \
    /opt/spark/bin/spark-submit \
    --master 'local[4]' \
    --driver-memory 4g \
    --conf spark.executor.memory=2g \
    /app/spark/jobs/promote_silver.py

log_success "Promoção Silver concluída!"

# Inicia Thrift Server para dbt se conectar via JDBC
log_step "Iniciando Spark Thrift Server para dbt..."
podman exec -d spark \
    /opt/spark/sbin/start-thriftserver.sh \
    --master local[2] \
    --driver-memory 512m \
    --conf spark.executor.memory=512m

# Aguarda Thrift Server ficar pronto
log_step "Aguardando Thrift Server ficar pronto..."
for i in $(seq 1 30); do
    if podman exec spark bash -c "echo 'SELECT 1;' | /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 -n spark 2>/dev/null" | grep -q "1"; then
        log_success "Thrift Server pronto!"
        break
    fi
    if [ $i -eq 30 ]; then
        log_error "Thrift Server não iniciou em 90s"
        exit 1
    fi
    echo -n "."
    sleep 3
done

log_step "Instalando dependências dbt..."
podman exec airflow-scheduler \
    dbt deps \
    --project-dir /app/dbt_project \
    --profiles-dir /app/dbt_project

log_success "dbt deps instalado!"

log_step "Executando dbt run (camada Silver)..."
podman exec airflow-scheduler \
    dbt run \
    --project-dir /app/dbt_project \
    --profiles-dir /app/dbt_project \
    --select silver

log_success "dbt Silver executado!"

log_step "Executando dbt test (validação de contratos)..."
if podman exec airflow-scheduler \
    dbt test \
    --project-dir /app/dbt_project \
    --profiles-dir /app/dbt_project; then
    log_success "Todos os contratos de dados foram validados!"
else
    log_error "CONTRATO DE DADOS VIOLADO! Pipeline interrompido."
    log_error "Verifique os logs acima para identificar os testes que falharam."
    exit 1
fi


# ============================================================
# STAGE 4: BUILD — Gold + Documentação
# ============================================================
log_stage "4/5 — BUILD"

log_step "Executando dbt run (camada Gold — Star Schema)..."
podman exec airflow-scheduler \
    dbt run \
    --project-dir /app/dbt_project \
    --profiles-dir /app/dbt_project \
    --select gold

log_success "Camada Gold construída!"

log_step "Gerando documentação dbt..."
podman exec airflow-scheduler \
    dbt docs generate \
    --project-dir /app/dbt_project \
    --profiles-dir /app/dbt_project

log_success "Documentação gerada!"


# ============================================================
# RESULTADO FINAL
# ============================================================
echo ""
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════${NC}"
echo -e "${BOLD}${GREEN}  🎉 PIPELINE EXECUTADO COM SUCESSO!${NC}"
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  ${CYAN}Camadas processadas:${NC}"
echo -e "    ├── 🥉 Bronze: CSV → Parquet (MinIO)"
echo -e "    ├── 🥈 Silver: Parquet → Iceberg (Nessie) + Contratos"
echo -e "    └── 🥇 Gold: Star Schema (dim_tempo, dim_geografia, dim_especialidade, fct_atendimentos)"
echo ""

if [ "$SKIP_CLEANUP" = true ]; then
    echo -e "  ${YELLOW}Ambiente mantido ativo (--skip-cleanup).${NC}"
    echo -e "  ${CYAN}Para limpar:${NC} podman-compose -f ${COMPOSE_FILE} down -v"
fi
