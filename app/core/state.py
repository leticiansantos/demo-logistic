"""
Gerenciamento de estado das conversas via tabela conversas no Lakebase (PostgreSQL).
"""
import json
from datetime import datetime

from core.db import run_sql, SCHEMA


def _empty_conversation(driver_id: str) -> dict:
    return {
        "driver_id": driver_id,
        "status": "idle",
        "messages": [],
        "context": {
            "origem_estado": None,
            "destino_estado": None,
            "cargas_oferecidas": [],
            "carga_aceita": None,
            "viagem_iniciada_em": None,
        },
    }


def _row_to_conv(driver_id: str, row: dict) -> dict:
    conv = _empty_conversation(driver_id)
    conv["status"] = row.get("status") or "idle"
    try:
        conv["messages"] = json.loads(row.get("messages") or "[]")
    except (json.JSONDecodeError, TypeError):
        conv["messages"] = []
    try:
        conv["context"] = json.loads(row.get("context") or "{}")
    except (json.JSONDecodeError, TypeError):
        pass
    return conv


def load_state() -> dict:
    rows = run_sql(f"SELECT driver_id, status, messages, context FROM {SCHEMA}.conversas")
    conversations = {r["driver_id"]: _row_to_conv(r["driver_id"], r) for r in rows}
    return {"conversations": conversations, "last_updated": datetime.now().isoformat()}


def get_conversation(driver_id: str) -> dict:
    rows = run_sql(
        f"SELECT status, messages, context FROM {SCHEMA}.conversas WHERE driver_id = %(driver_id)s",
        {"driver_id": driver_id},
    )
    if not rows:
        return _empty_conversation(driver_id)
    return _row_to_conv(driver_id, rows[0])


def _upsert(conv: dict) -> None:
    driver_id     = conv["driver_id"]
    status        = conv.get("status", "idle")
    messages_json = json.dumps(conv.get("messages", []), ensure_ascii=False)
    context_json  = json.dumps(conv.get("context", {}), ensure_ascii=False)
    now           = datetime.now()

    # PostgreSQL UPSERT — parâmetros nomeados evitam problemas de escaping
    # com conteúdo arbitrário (newlines, aspas, barras) no JSON.
    run_sql(f"""
        INSERT INTO {SCHEMA}.conversas (driver_id, status, messages, context, updated_at)
        VALUES (%(driver_id)s, %(status)s, %(messages)s, %(context)s, %(updated_at)s)
        ON CONFLICT (driver_id) DO UPDATE SET
            status     = EXCLUDED.status,
            messages   = EXCLUDED.messages,
            context    = EXCLUDED.context,
            updated_at = EXCLUDED.updated_at
    """, {
        "driver_id":  driver_id,
        "status":     status,
        "messages":   messages_json,
        "context":    context_json,
        "updated_at": now,
    })


def save_conversation_full(driver_id: str, data: dict) -> None:
    """Salva estado completo da conversa em uma única operação UPSERT, sem ler do DB antes.
    Evita ciclos de leitura/escrita intercalados que causam dados obsoletos."""
    conv = _empty_conversation(driver_id)
    conv.update(data)
    conv["driver_id"] = driver_id
    _upsert(conv)


def reset_conversation(driver_id: str) -> None:
    _upsert(_empty_conversation(driver_id))
