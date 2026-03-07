"""
Helper compartilhado para executar SQL no Databricks SQL Warehouse.
"""
from typing import Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState, StatementParameterListItem

WAREHOUSE_ID = "bb828695aaf0a968"
SCHEMA = "leticia_santos_classic_stable_catalog.motz_demo"


def run_sql(query: str, params: Optional[list[tuple[str, str]]] = None) -> list[dict]:
    """Executa SQL no Databricks e retorna lista de dicts.

    params: lista de tuplas (nome, valor) para parâmetros nomeados (:nome).
    Use parâmetros para valores com conteúdo arbitrário (JSON, textos do usuário)
    para evitar problemas de escaping em string literals SQL.
    """
    w = WorkspaceClient()

    statement_params = None
    if params:
        statement_params = [
            StatementParameterListItem(name=name, value="" if value is None else str(value))
            for name, value in params
        ]

    result = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        parameters=statement_params,
        wait_timeout="30s",
    )
    if result.status.state != StatementState.SUCCEEDED:
        err = result.status.error
        raise RuntimeError(f"SQL error: {err.message if err else result.status.state}")

    schema = result.manifest.schema.columns if result.manifest and result.manifest.schema else []
    cols = [c.name for c in schema]
    rows = result.result.data_array or [] if result.result else []
    return [dict(zip(cols, row)) for row in rows]


def sql_escape(value: str) -> str:
    """Escapa string para uso seguro em literal SQL com aspas simples.
    Use apenas para valores controlados (siglas, IDs). Para conteúdo
    arbitrário prefira parâmetros nomeados via run_sql(..., params=[...]).
    """
    return value.replace("'", "''")
