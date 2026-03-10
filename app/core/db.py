"""
Helper compartilhado para executar SQL no Lakebase (PostgreSQL gerenciado).
Substitui a conexão via Databricks SQL Warehouse por psycopg2 direto ao Lakebase.
"""
import base64
import json
import os
import time
import urllib.request
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

    from datetime import datetime, timezone
    from databricks.sdk import WorkspaceClient

    endpoint_path = (
        f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}"
        f"/endpoints/{LAKEBASE_ENDPOINT}"
    )

    # Detect Databricks Apps environment: client_id + client_secret are injected as M2M OAuth.
    # In that case, use them to fetch the PAT from the secret scope at runtime, then use
    # the PAT to generate Lakebase credentials with human user identity (avoids SP role issues).
    # Locally, fall back to the SDK OAuth flow using the developer's own identity.
    is_apps_env = bool(
        os.environ.get("DATABRICKS_CLIENT_ID") and os.environ.get("DATABRICKS_CLIENT_SECRET")
    )

    if is_apps_env:
        w = WorkspaceClient()
        secret_resp = w.secrets.get_secret(scope="motz-demo", key="lakebase-pat")
        pat = base64.b64decode(secret_resp.value).decode()

        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        if not host.startswith("http"):
            host = f"https://{host}"
        req = urllib.request.Request(
            f"{host}/api/2.0/postgres/credentials",
            data=json.dumps({"endpoint": endpoint_path}).encode(),
            headers={"Authorization": f"Bearer {pat}", "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
        email = "leticia.santos@databricks.com"
    else:
        w = WorkspaceClient()
        result = w.api_client.do(
            "POST",
            "/api/2.0/postgres/credentials",
            body={"endpoint": endpoint_path},
        )
        email = w.current_user.me().user_name

    token = result["token"]

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
