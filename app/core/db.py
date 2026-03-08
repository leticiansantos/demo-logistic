"""
Helper compartilhado para executar SQL no Lakebase (PostgreSQL gerenciado).
Substitui a conexão via Databricks SQL Warehouse por psycopg2 direto ao Lakebase.
"""
import json
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

    from databricks.sdk import WorkspaceClient
    from datetime import datetime, timezone
    w = WorkspaceClient()

    endpoint_path = (
        f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}"
        f"/endpoints/{LAKEBASE_ENDPOINT}"
    )
    result = w.api_client.do(
        "POST",
        "/api/2.0/postgres/credentials",
        body={"endpoint": endpoint_path},
    )
    token = result["token"]
    email = w.current_user.me().user_name

    # Usa expire_time da resposta; fallback de 55 min
    expire_time = result.get("expire_time")
    if expire_time:
        dt = datetime.fromisoformat(expire_time.replace("Z", "+00:00"))
        expires_at = dt.timestamp() - 60  # 1 min de margem
    else:
        expires_at = now + 3300

    _cred_cache["token"]      = token
    _cred_cache["email"]      = email
    _cred_cache["expires_at"] = expires_at
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
