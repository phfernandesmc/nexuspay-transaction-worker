#!/usr/bin/env bash
# Regenera src/test/resources/schema.sql a partir do banco do gateway.
#
# O Alembic no repositorio nexuspay-api-gateway e dono unico do schema. Este
# worker nao roda migration nenhuma, mas seus testes precisam das tabelas.
# Rode este script sempre que o gateway ganhar uma migration que toque
# accounts, transactions ou ledger_entries.
#
# Pre-requisito: o container nexuspay-postgres do gateway no ar, com
# `alembic upgrade head` ja aplicado.
set -euo pipefail

destino="$(dirname "$0")/../src/test/resources/schema.sql"

# pg_dump recentes (16.10+/17.6+, correcao de seguranca CVE-2025-1094) emitem
# \restrict e \unrestrict no topo e no fim do dump. Sao meta-comandos do psql,
# nao SQL: o executor do Testcontainers (withInitScript) roda cada statement
# via JDBC puro e quebra nessas linhas. Elas nao alteram o schema, entao sao
# filtradas aqui.
docker exec nexuspay-postgres pg_dump \
  -U nexuspay -d nexuspay \
  --schema-only --no-owner --no-privileges --no-comments \
  | grep -Ev '^\\(un)?restrict ' \
  > "$destino"

echo "schema regravado em $destino ($(wc -l < "$destino") linhas)"
