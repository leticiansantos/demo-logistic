"""
Helper compartilhado para executar SQL no Lakebase (PostgreSQL gerenciado).
Substitui a conexão via Databricks SQL Warehouse por psycopg2 direto ao Lakebase.
"""
import json
import subprocess
import time
from typing import Optional

import psycopg2
import psycopg2.extras

LAKEBASE_PROJECT  = "motz-demo"
LAKEBASE_BRANCH   = "production"
LAKEBASE_ENDPOINT = "primary"
LAKEBASE_HOST     = "ep-twilight-art-d1liaemf.database.us-west-2.cloud.databricks.com"
LAKEBASE_DATABASE = "motz"
LAKEBASE_PORT     = 5432

# Schema público do Lakebase (tabelas acessadas como public.<tabela>)
SCHEMA = "public"

# Cache de credenciais OAuth — validade ~1h, renovado 60s antes do vencimento
_cred_cache: dict = {}


def _get_credentials() -> tuple[str, str]:
    """Retorna (email, token) para o Lakebase, com cache de ~55 minutos."""
    now = time.time()
    if _cred_cache.get("expires_at", 0) > now + 60:
        return _cred_cache["email"], _cred_cache["token"]

    endpoint_path = (
        f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}"
        f"/endpoints/{LAKEBASE_ENDPOINT}"
    )
    token_out = subprocess.run(
        ["databricks", "postgres", "generate-database-credential",
         endpoint_path, "-p", "DEFAULT", "--output", "json"],
        capture_output=True, text=True, check=True,
    )
    user_out = subprocess.run(
        ["databricks", "current-user", "me", "-p", "DEFAULT", "--output", "json"],
        capture_output=True, text=True, check=True,
    )
    token = json.loads(token_out.stdout)["token"]
    email = json.loads(user_out.stdout)["userName"]

    _cred_cache["token"]      = token
    _cred_cache["email"]      = email
    _cred_cache["expires_at"] = now + 3300  # 55 min
    return email, token


def get_connection():
    """Retorna uma conexão psycopg2 ao Lakebase."""
    email, token = _get_credentials()
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        database=LAKEBASE_DATABASE,
        user=email,
        password=token,
        sslmode="require",
    )


def run_sql(query: str, params: Optional[dict] = None) -> list[dict]:
    """Executa SQL no Lakebase e retorna lista de dicts.

    params: dicionário com parâmetros nomeados — use %(nome)s no SQL.
    Para valores arbitrários (JSON, texto de usuário) sempre passe via params
    para evitar problemas de escaping e injeção.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            conn.commit()
            if cur.description:
                return [dict(row) for row in cur.fetchall()]
            return []
    finally:
        conn.close()


def sql_escape(value: str) -> str:
    """Escapa string para uso em literal SQL com aspas simples.
    Prefira parâmetros nomeados via run_sql(..., params={...}) para conteúdo
    arbitrário; use sql_escape apenas para valores controlados (siglas, IDs).
    """
    return value.replace("'", "''")
