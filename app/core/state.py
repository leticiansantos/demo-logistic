"""
Gerenciamento de estado das conversas via tabela Delta no Databricks.
Tabela: motz_demo.conversas
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
        f"SELECT status, messages, context FROM {SCHEMA}.conversas WHERE driver_id = :driver_id",
        [("driver_id", driver_id)],
    )
    if not rows:
        return _empty_conversation(driver_id)
    return _row_to_conv(driver_id, rows[0])


def _upsert(conv: dict) -> None:
    driver_id = conv["driver_id"]
    status = conv.get("status", "idle")
    messages_json = json.dumps(conv.get("messages", []), ensure_ascii=False)
    context_json = json.dumps(conv.get("context", {}), ensure_ascii=False)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Parâmetros nomeados evitam problemas de escaping com conteúdo arbitrário
    # (newlines, aspas, barras) no JSON de mensagens e contexto.
    run_sql(f"""
        MERGE INTO {SCHEMA}.conversas AS t
        USING (SELECT :driver_id AS driver_id, :status AS status,
                      :messages AS messages, :context AS context) AS s
        ON t.driver_id = s.driver_id
        WHEN MATCHED THEN UPDATE SET
          t.status     = s.status,
          t.messages   = s.messages,
          t.context    = s.context,
          t.updated_at = TIMESTAMP '{now}'
        WHEN NOT MATCHED THEN INSERT (driver_id, status, messages, context, updated_at)
          VALUES (s.driver_id, s.status, s.messages, s.context, TIMESTAMP '{now}')
    """, [
        ("driver_id", driver_id),
        ("status",    status),
        ("messages",  messages_json),
        ("context",   context_json),
    ])


def update_conversation(driver_id: str, updates: dict) -> None:
    conv = get_conversation(driver_id)
    conv.update(updates)
    conv["driver_id"] = driver_id
    _upsert(conv)


def add_message(driver_id: str, role: str, content: str, msg_type: str = "text") -> None:
    conv = get_conversation(driver_id)
    conv.setdefault("messages", []).append({
        "role": role,
        "content": content,
        "type": msg_type,
        "timestamp": datetime.now().strftime("%H:%M"),
    })
    conv["driver_id"] = driver_id
    _upsert(conv)


def reset_conversation(driver_id: str) -> None:
    _upsert(_empty_conversation(driver_id))
