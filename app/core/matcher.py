"""
Lógica de matching: encontra cargas disponíveis na tabela cargas do Databricks
compatíveis com o caminhão e rota do motorista.
"""
from datetime import datetime

from core.db import run_sql, sql_escape, SCHEMA

# Mapeamento de siglas e nomes de estados
ESTADOS = {
    "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas",
    "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo",
    "GO": "Goiás", "MA": "Maranhão", "MT": "Mato Grosso", "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais", "PA": "Pará", "PB": "Paraíba", "PR": "Paraná",
    "PE": "Pernambuco", "PI": "Piauí", "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte", "RS": "Rio Grande do Sul", "RO": "Rondônia",
    "RR": "Roraima", "SC": "Santa Catarina", "SP": "São Paulo",
    "SE": "Sergipe", "TO": "Tocantins",
}
ESTADOS_INVERSO = {v.lower(): k for k, v in ESTADOS.items()}
ESTADOS_INVERSO.update({k.lower(): k for k in ESTADOS})


def normalizar_estado(texto: str) -> str | None:
    """Converte nome ou sigla de estado para sigla (2 letras maiúsculas)."""
    if not texto:
        return None
    texto = texto.strip().lower()
    resultado = ESTADOS_INVERSO.get(texto)
    if resultado:
        return resultado
    for nome, sigla in ESTADOS_INVERSO.items():
        if texto in nome or nome in texto:
            return sigla
    return None


def _query_loads(composicao: str, caracteristica: str, extra_filters: list[str]) -> list[dict]:
    """Executa a query de cargas com os filtros fornecidos."""
    base = [
        "c.status = 'disponivel'",
        f"c.composicao_veiculo = '{sql_escape(composicao)}'",
        f"c.caracteristica_veiculo = '{sql_escape(caracteristica)}'",
    ] + extra_filters

    rows = run_sql(f"""
        SELECT
            c.id, c.tipo_carga, c.composicao_veiculo, c.caracteristica_veiculo,
            c.origem_cidade, c.origem_estado, c.destino_cidade, c.destino_estado,
            CAST(c.data_prevista_coleta AS STRING) AS data_prevista_coleta,
            c.peso_kg, c.valor_frete,
            e.nome AS embarcador
        FROM {SCHEMA}.cargas c
        LEFT JOIN {SCHEMA}.embarcadores e ON e.id = c.embarcador_id
        WHERE {" AND ".join(base)}
        ORDER BY c.valor_frete DESC
        LIMIT 5
    """)
    return [
        {
            "id": r["id"],
            "tipo_carga": r["tipo_carga"],
            "composicao_veiculo": r["composicao_veiculo"],
            "caracteristica_veiculo": r["caracteristica_veiculo"],
            "origem_cidade": r["origem_cidade"],
            "origem_estado": r["origem_estado"],
            "destino_cidade": r["destino_cidade"],
            "destino_estado": r["destino_estado"],
            "data_prevista_coleta": r["data_prevista_coleta"],
            "peso_kg": float(r["peso_kg"] or 0),
            "valor_frete": float(r["valor_frete"] or 0),
            "embarcador": r["embarcador"] or "",
        }
        for r in rows
    ]


def find_loads(driver: dict, dest_estado: str, orig_estado: str = None) -> list[dict]:
    """
    Encontra cargas disponíveis compatíveis com o caminhão do motorista e rota desejada.
    Aplica fallback progressivo: origem+destino → só destino → sem filtro de rota.
    """
    composicao = driver["veiculo"]["composicao"]
    caracteristica = driver["veiculo"]["caracteristica"]

    dest_sigla = normalizar_estado(dest_estado) if dest_estado else None
    orig_sigla = normalizar_estado(orig_estado) if orig_estado else None

    # Tentativa 1: origem + destino
    if orig_sigla and dest_sigla:
        results = _query_loads(composicao, caracteristica, [
            f"c.origem_estado = '{orig_sigla}'",
            f"c.destino_estado = '{dest_sigla}'",
        ])
        if results:
            return results

    # Tentativa 2: só destino
    if dest_sigla:
        results = _query_loads(composicao, caracteristica, [
            f"c.destino_estado = '{dest_sigla}'",
        ])
        if results:
            return results

    # Tentativa 3: só origem
    if orig_sigla:
        results = _query_loads(composicao, caracteristica, [
            f"c.origem_estado = '{orig_sigla}'",
        ])
        if results:
            return results

    # Fallback: qualquer carga disponível para o tipo de veículo
    return _query_loads(composicao, caracteristica, [])


def format_loads_list(loads: list[dict]) -> str:
    """Formata lista de cargas para exibição na mensagem."""
    if not loads:
        return "Nenhuma carga disponível para essa rota."

    lines = []
    for i, c in enumerate(loads, 1):
        valor = f"R$ {c['valor_frete']:,.0f}".replace(",", ".")
        peso = f"{c['peso_kg'] / 1000:.1f}t"
        lines.append(
            f"*{i}.* {c['tipo_carga']} | {c['origem_cidade']}/{c['origem_estado']} → "
            f"{c['destino_cidade']}/{c['destino_estado']}\n"
            f"   {peso} | {valor} | Coleta: {c['data_prevista_coleta']}\n"
            f"   Embarcador: {c['embarcador']}"
        )
    return "\n\n".join(lines)


def mark_load_accepted(carga_id: str, motorista_id: str) -> None:
    """Marca a carga como aceita no Databricks para evitar dupla aceitação."""
    now = datetime.now().strftime("%Y-%m-%d")
    run_sql(f"""
        UPDATE {SCHEMA}.cargas
        SET status = 'aceita',
            motorista_id = '{sql_escape(motorista_id)}'
        WHERE id = '{sql_escape(carga_id)}'
    """)


def get_carga_by_id(carga_id: str) -> dict | None:
    rows = run_sql(
        f"SELECT * FROM {SCHEMA}.cargas WHERE id = '{sql_escape(carga_id)}'"
    )
    return rows[0] if rows else None
