"""
Migra dados das tabelas Delta (Databricks SQL Warehouse) para Lakebase (PostgreSQL).
Tabelas: motoristas, embarcadores, transportadoras, cargas

Uso:
    python scripts/migrate_delta_to_lakebase.py
"""
import json
import subprocess
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# ---------------------------------------------------------------------------
# Config Databricks (origem)
# ---------------------------------------------------------------------------
DELTA_WAREHOUSE_ID = "bb828695aaf0a968"
DELTA_SCHEMA       = "leticia_santos_classic_stable_catalog.motz_demo"

# ---------------------------------------------------------------------------
# Config Lakebase (destino)
# ---------------------------------------------------------------------------
LAKEBASE_PROJECT  = "motz-demo"
LAKEBASE_BRANCH   = "production"
LAKEBASE_ENDPOINT = "primary"
LAKEBASE_HOST     = "ep-twilight-art-d1liaemf.database.us-west-2.cloud.databricks.com"
LAKEBASE_DATABASE = "motz"
LAKEBASE_PORT     = 5432


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def delta_query(query: str) -> list[dict]:
    w = WorkspaceClient()
    result = w.statement_execution.execute_statement(
        warehouse_id=DELTA_WAREHOUSE_ID,
        statement=query,
        wait_timeout="50s",
    )
    if result.status.state != StatementState.SUCCEEDED:
        err = result.status.error
        raise RuntimeError(f"SQL error: {err.message if err else result.status.state}")
    schema = result.manifest.schema.columns if result.manifest and result.manifest.schema else []
    cols   = [c.name for c in schema]
    rows   = result.result.data_array or [] if result.result else []
    return [dict(zip(cols, row)) for row in rows]


def lakebase_conn():
    endpoint_path = (
        f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}"
        f"/endpoints/{LAKEBASE_ENDPOINT}"
    )
    token_out = subprocess.run(
        ["databricks", "postgres", "generate-database-credential",
         endpoint_path, "-p", "DEFAULT", "--output", "json"],
        capture_output=True, text=True, check=True,
    )
    token = json.loads(token_out.stdout)["token"]
    user_out = subprocess.run(
        ["databricks", "current-user", "me", "-p", "DEFAULT", "--output", "json"],
        capture_output=True, text=True, check=True,
    )
    email = json.loads(user_out.stdout)["userName"]
    return psycopg2.connect(
        host=LAKEBASE_HOST, port=LAKEBASE_PORT, database=LAKEBASE_DATABASE,
        user=email, password=token, sslmode="require",
    )


def insert_batch(cur, table: str, rows: list[dict], conflict_col: str = "id"):
    if not rows:
        return
    cols        = list(rows[0].keys())
    placeholders = ", ".join(f"%({c})s" for c in cols)
    updates      = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != conflict_col)
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_col}) DO UPDATE SET {updates}"
    )
    psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

def migrate():
    print("Conectando ao Lakebase...")
    conn = lakebase_conn()
    cur  = conn.cursor()

    # ── transportadoras ──────────────────────────────────────────────────────
    print("Migrando transportadoras...", end=" ", flush=True)
    rows = delta_query(f"""
        SELECT id, nome, cnpj, razao_social, endereco, cidade, estado, cep,
               telefone, email, CAST(data_cadastro AS STRING) AS data_cadastro, ativo
        FROM {DELTA_SCHEMA}.transportadoras
    """)
    insert_batch(cur, "transportadoras", rows)
    conn.commit()
    print(f"{len(rows)} registros")

    # ── embarcadores ─────────────────────────────────────────────────────────
    print("Migrando embarcadores...", end=" ", flush=True)
    rows = delta_query(f"""
        SELECT id, nome, cnpj, razao_social, endereco, cidade, estado, cep,
               telefone, email, CAST(data_cadastro AS STRING) AS data_cadastro, ativo
        FROM {DELTA_SCHEMA}.embarcadores
    """)
    insert_batch(cur, "embarcadores", rows)
    conn.commit()
    print(f"{len(rows)} registros")

    # ── motoristas ───────────────────────────────────────────────────────────
    print("Migrando motoristas...", end=" ", flush=True)
    rows = delta_query(f"""
        SELECT id, transportadora_id, nome, cpf, cnh, categoria_cnh,
               CAST(data_nascimento AS STRING) AS data_nascimento,
               telefone, email,
               CAST(data_cadastro AS STRING) AS data_cadastro,
               ativo,
               localizacao_atual, localizacao_estado,
               veiculo_placa, veiculo_modelo, veiculo_composicao,
               veiculo_caracteristica,
               CAST(veiculo_capacidade_kg AS INT) AS veiculo_capacidade_kg
        FROM {DELTA_SCHEMA}.motoristas
    """)
    insert_batch(cur, "motoristas", rows)
    conn.commit()
    print(f"{len(rows)} registros")

    # ── cargas ───────────────────────────────────────────────────────────────
    print("Migrando cargas...", end=" ", flush=True)
    rows = delta_query(f"""
        SELECT id, embarcador_id, transportadora_id, motorista_id,
               tipo_carga, composicao_veiculo, caracteristica_veiculo, embalagem,
               origem_cidade, origem_estado, destino_cidade, destino_estado,
               CAST(data_prevista_coleta AS STRING)  AS data_prevista_coleta,
               CAST(data_prevista_entrega AS STRING) AS data_prevista_entrega,
               CAST(data_entrega AS STRING)          AS data_entrega,
               status,
               CAST(peso_kg AS DOUBLE)       AS peso_kg,
               CAST(valor_frete AS DOUBLE)   AS valor_frete,
               CAST(data_criacao AS STRING)  AS data_criacao
        FROM {DELTA_SCHEMA}.cargas
    """)
    insert_batch(cur, "cargas", rows)
    conn.commit()
    print(f"{len(rows)} registros")

    cur.close()
    conn.close()
    print("Migração concluída.")


if __name__ == "__main__":
    migrate()
