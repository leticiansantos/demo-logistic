"""
Motz Demo – FastAPI Backend
Serve a API para o frontend React (WhatsApp simulator + Live dashboard + Dashboard).
"""
import sys
import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

AUDIO_DIR = Path("/tmp/motz_audio")
AUDIO_DIR.mkdir(parents=True, exist_ok=True)

# Garante que app/ está no path para importar core/
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.agent import process_message
from core.transcriber import transcribe
from core import state as state_mgr
from core.db import run_sql, SCHEMA

FRONTEND_BUILD = Path(__file__).parent.parent / "frontend" / "dist"

app = FastAPI(title="Motz Demo API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class MessageBody(BaseModel):
    driver_id: str
    message: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_drivers() -> list[dict]:
    rows = run_sql(f"""
        SELECT id, nome, telefone, categoria_cnh,
               localizacao_atual, localizacao_estado,
               veiculo_placa, veiculo_modelo,
               veiculo_composicao, veiculo_caracteristica, veiculo_capacidade_kg
        FROM {SCHEMA}.motoristas
        WHERE veiculo_composicao IS NOT NULL
        ORDER BY nome
    """)
    return [
        {
            "id": r["id"],
            "nome": r["nome"],
            "telefone": r["telefone"],
            "categoria_cnh": r["categoria_cnh"],
            "localizacao_atual": r["localizacao_atual"],
            "localizacao_estado": r["localizacao_estado"],
            "veiculo": {
                "placa": r["veiculo_placa"],
                "modelo": r["veiculo_modelo"],
                "composicao": r["veiculo_composicao"],
                "caracteristica": r["veiculo_caracteristica"],
                "capacidade_kg": int(r["veiculo_capacidade_kg"] or 0),
            },
        }
        for r in rows
    ]


def _get_driver(driver_id: str) -> dict:
    drivers = _load_drivers()
    driver = next((d for d in drivers if d["id"] == driver_id), None)
    if not driver:
        raise HTTPException(status_code=404, detail=f"Motorista '{driver_id}' não encontrado.")
    return driver


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/backend/status")
def backend_status():
    """Retorna estado do endpoint de modelo."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    try:
        ep = w.serving_endpoints.get(name="databricks-claude-sonnet-4-6")
        ep_state = ep.state.ready.value if ep.state and ep.state.ready else "NOT_READY"
    except Exception:
        ep_state = "UNKNOWN"

    return {
        "model_endpoint": {"name": "databricks-claude-sonnet-4-6", "state": ep_state},
        "ready": ep_state == "READY",
    }


@app.post("/api/backend/start")
def backend_start():
    return {"ok": True}


@app.get("/api/dashboard-embed")
def dashboard_embed():
    """Retorna a URL de embed do dashboard Databricks AI/BI (publicado com embed_credentials)."""
    dashboard_id = "01f11987b498173aba3688e58d455efd"
    org_id = "7474658265676932"
    host = "https://fevm-leticia-santos-classic-stable.cloud.databricks.com"
    embed_url = f"{host}/dashboardsv3/{dashboard_id}/published?o={org_id}"
    return {"url": embed_url, "dashboard_id": dashboard_id}


@app.get("/api/drivers")
def list_drivers():
    return _load_drivers()


@app.get("/api/loads/available")
def get_available_loads():
    """Cargas disponíveis agrupadas por período de coleta."""
    try:
        rows = _run_sql(f"""
            SELECT
                COUNT(*)                                                                                                   AS total,
                COUNT(CASE WHEN data_prevista_coleta = CURRENT_DATE                                          THEN 1 END) AS hoje,
                COUNT(CASE WHEN data_prevista_coleta BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '2 days' THEN 1 END) AS proximos_3_dias,
                COUNT(CASE WHEN data_prevista_coleta BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '6 days' THEN 1 END) AS proxima_semana,
                COUNT(CASE WHEN data_prevista_coleta BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '29 days' THEN 1 END) AS proximo_mes,
                COUNT(CASE WHEN data_prevista_coleta < CURRENT_DATE                                          THEN 1 END) AS atrasadas
            FROM {SCHEMA}.cargas
            WHERE status = 'disponivel'
        """)
        totais = {k: int(v or 0) for k, v in rows[0].items()} if rows else {}

        por_tipo = _run_sql(f"""
            SELECT tipo_carga, COUNT(*) AS quantidade,
                   MIN(data_prevista_coleta) AS proxima_coleta
            FROM {SCHEMA}.cargas
            WHERE status = 'disponivel'
              AND data_prevista_coleta >= CURRENT_DATE
            GROUP BY tipo_carga
            ORDER BY quantidade DESC
            LIMIT 8
        """)

        return {
            "totais": totais,
            "por_tipo": [
                {
                    "tipo_carga": r["tipo_carga"],
                    "quantidade": int(r["quantidade"]),
                    "proxima_coleta": r["proxima_coleta"],
                }
                for r in por_tipo
            ],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _run_sql(query: str) -> list[dict]:
    try:
        return run_sql(query)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/metrics")
def get_metrics():
    """Busca métricas ao vivo do Databricks SQL."""
    try:
        kpis_rows = _run_sql(f"""
            SELECT
                (SELECT COUNT(*) FROM {SCHEMA}.transportadoras)                                                AS total_transportadoras,
                (SELECT COUNT(*) FROM {SCHEMA}.motoristas)                                                     AS total_motoristas,
                (SELECT COUNT(*) FROM {SCHEMA}.embarcadores)                                                   AS total_embarcadores,
                (SELECT COUNT(*) FROM {SCHEMA}.cargas)                                                         AS total_cargas,
                (SELECT COUNT(*) FROM {SCHEMA}.cargas WHERE status = 'realizada')                              AS cargas_realizadas,
                (SELECT COUNT(*) FROM {SCHEMA}.cargas WHERE status = 'disponivel')                             AS cargas_disponiveis,
                (SELECT COUNT(*) FROM {SCHEMA}.cargas WHERE status NOT IN ('realizada','disponivel'))          AS cargas_futuras,
                (SELECT ROUND(COALESCE(SUM(valor_frete),0)::numeric,2) FROM {SCHEMA}.cargas WHERE status='realizada')  AS valor_total_fretes_realizados,
                (SELECT ROUND(COALESCE(AVG(valor_frete),0)::numeric,2) FROM {SCHEMA}.cargas WHERE status='realizada')  AS valor_medio_frete_realizado,
                (SELECT ROUND(COALESCE(SUM(peso_kg),0)::numeric,2)    FROM {SCHEMA}.cargas WHERE status='realizada')   AS peso_total_kg_realizadas
        """)
        resumo = {k: (float(v) if v is not None else 0) for k, v in kpis_rows[0].items()} if kpis_rows else {}

        por_tipo = _run_sql(f"""
            SELECT tipo_carga, COUNT(*) AS quantidade
            FROM {SCHEMA}.cargas GROUP BY tipo_carga ORDER BY quantidade DESC LIMIT 10
        """)

        por_composicao = _run_sql(f"""
            SELECT composicao_veiculo, COUNT(*) AS quantidade
            FROM {SCHEMA}.cargas GROUP BY composicao_veiculo ORDER BY quantidade DESC
        """)

        por_uf_origem = _run_sql(f"""
            SELECT COALESCE(origem_estado,'N/A') AS estado, COUNT(*) AS quantidade
            FROM {SCHEMA}.cargas GROUP BY origem_estado ORDER BY quantidade DESC
        """)

        por_uf_destino = _run_sql(f"""
            SELECT COALESCE(destino_estado,'N/A') AS estado, COUNT(*) AS quantidade
            FROM {SCHEMA}.cargas GROUP BY destino_estado ORDER BY quantidade DESC
        """)

        por_mes = _run_sql(f"""
            SELECT TO_CHAR(data_entrega, 'YYYY-MM') AS ano_mes, COUNT(*) AS realizadas,
                   ROUND(SUM(valor_frete)::numeric, 2) AS valor_total
            FROM {SCHEMA}.cargas WHERE status='realizada' AND data_entrega IS NOT NULL
            GROUP BY TO_CHAR(data_entrega, 'YYYY-MM') ORDER BY ano_mes
        """)

        ticket_medio = _run_sql(f"""
            SELECT tipo_carga, COUNT(*) AS quantidade,
                   ROUND(AVG(valor_frete)::numeric,2) AS ticket_medio,
                   ROUND(SUM(valor_frete)::numeric,2) AS valor_total
            FROM {SCHEMA}.cargas WHERE status='realizada'
            GROUP BY tipo_carga ORDER BY valor_total DESC LIMIT 10
        """)

        dq = _run_sql(f"""
            SELECT 'Transportadoras' AS tabela, COUNT(*) AS registros_com_problema
            FROM {SCHEMA}.transportadoras
            WHERE LENGTH(TRIM(cnpj))!=14 OR TRIM(COALESCE(nome,''))='' OR email NOT LIKE '%@%' OR email IS NULL
            UNION ALL
            SELECT 'Motoristas', COUNT(*) FROM {SCHEMA}.motoristas
            WHERE LENGTH(TRIM(cpf))!=11 OR categoria_cnh NOT IN ('C','D','E') OR categoria_cnh IS NULL OR email NOT LIKE '%@%'
            UNION ALL
            SELECT 'Embarcadores', COUNT(*) FROM {SCHEMA}.embarcadores
            WHERE LENGTH(TRIM(cnpj))!=14 OR TRIM(COALESCE(nome,''))='' OR email NOT LIKE '%@%' OR email IS NULL
            UNION ALL
            SELECT 'Cargas', COUNT(*) FROM {SCHEMA}.cargas
            WHERE (status='realizada' AND data_entrega IS NULL) OR peso_kg IS NULL OR peso_kg<=0 OR valor_frete IS NULL OR valor_frete<0
        """)

        return {
            "resumo": resumo,
            "cargas_por_tipo": [{"tipo_carga": r["tipo_carga"], "quantidade": int(r["quantidade"])} for r in por_tipo],
            "cargas_por_composicao": [{"composicao_veiculo": r["composicao_veiculo"], "quantidade": int(r["quantidade"])} for r in por_composicao],
            "cargas_por_uf_origem": [{"estado": r["estado"], "quantidade": int(r["quantidade"])} for r in por_uf_origem],
            "cargas_por_uf_destino": [{"estado": r["estado"], "quantidade": int(r["quantidade"])} for r in por_uf_destino],
            "cargas_por_mes": [{"ano_mes": r["ano_mes"], "realizadas": int(r["realizadas"]), "valor_total": float(r["valor_total"] or 0)} for r in por_mes],
            "ticket_medio": [{"tipo_carga": r["tipo_carga"], "quantidade": int(r["quantidade"]), "ticket_medio": float(r["ticket_medio"] or 0), "valor_total": float(r["valor_total"] or 0)} for r in ticket_medio],
            "data_quality": [{"tabela": r["tabela"], "registros_com_problema": int(r["registros_com_problema"])} for r in dq],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/state")
def get_all_state():
    return state_mgr.load_state()


@app.get("/api/state/{driver_id}")
def get_driver_state(driver_id: str):
    return state_mgr.get_conversation(driver_id)


@app.post("/api/message")
def send_text_message(body: MessageBody):
    driver = _get_driver(body.driver_id)
    try:
        response = process_message(body.message, driver, save_driver_msg=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {
        "response": response,
        "state": state_mgr.get_conversation(body.driver_id),
    }


@app.get("/api/audio/{filename}")
def serve_audio(filename: str):
    """Serve a persisted audio file by filename."""
    path = AUDIO_DIR / filename
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Arquivo de áudio não encontrado.")
    return FileResponse(path, media_type="audio/wav")


@app.post("/api/audio/{driver_id}")
async def send_audio_message(driver_id: str, file: UploadFile = File(...)):
    driver = _get_driver(driver_id)
    audio_bytes = await file.read()

    # Persist audio so the player works across page reloads
    audio_filename = f"{uuid.uuid4()}.wav"
    (AUDIO_DIR / audio_filename).write_bytes(audio_bytes)
    audio_url = f"/api/audio/{audio_filename}"

    try:
        transcript = transcribe(audio_bytes, file.filename or "audio.wav")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    try:
        response = process_message(
            transcript, driver,
            save_driver_msg=True,
            stored_content=transcript,
            msg_type="audio",
            audio_url=audio_url,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "transcript": transcript,
        "audio_url": audio_url,
        "response": response,
        "state": state_mgr.get_conversation(driver_id),
    }


@app.delete("/api/state/{driver_id}")
def reset_driver_conversation(driver_id: str):
    state_mgr.reset_conversation(driver_id)
    return {"ok": True}


@app.delete("/api/state")
def reset_all_conversations():
    run_sql(f"DELETE FROM {SCHEMA}.conversas")
    return {"ok": True}


# ---------------------------------------------------------------------------
# Serve React build (production mode)
# ---------------------------------------------------------------------------
if FRONTEND_BUILD.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_BUILD), html=True), name="static")
