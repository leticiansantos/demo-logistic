"""
Lógica de matching: encontra cargas disponíveis na tabela cargas do Lakebase
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


def _query_loads(
    composicao: str,
    caracteristica: str,
    capacidade_kg: float,
    orig_sigla: str,
    dest_sigla: str | None,
    dest_cidade: str | None,
    date_filter: str,
    exclude_ids: list[str] | None = None,
    orig_cidade: str | None = None,
) -> list[dict]:
    """
    Busca cargas respeitando:
    - Origem estrita (motorista só pode embarcar de onde está)
    - Destino flexível: cidade exata > mesmo estado > outros destinos
    - Capacidade: só cargas que cabem no caminhão (peso_kg <= capacidade)
    - Data: filtro passado pelo chamador
    Ordenação: cidade exata (3) + mesmo estado (2) > utilização de peso > valor do frete
    """
    state_score = (
        f"CASE WHEN c.destino_estado = '{dest_sigla}' THEN 2 ELSE 0 END"
        if dest_sigla else "0::int"
    )
    city_score = (
        f"CASE WHEN LOWER(c.destino_cidade) = LOWER('{sql_escape(dest_cidade)}') THEN 3 ELSE 0 END"
        if dest_cidade else "0::int"
    )
    dest_score = f"({state_score} + {city_score})"
    # Utilização de capacidade: cargas que aproveitam mais o caminhão aparecem primeiro
    weight_util = f"c.peso_kg::float / {int(capacidade_kg)}"

    rows = run_sql(f"""
        SELECT
            c.id, c.tipo_carga, c.composicao_veiculo, c.caracteristica_veiculo,
            c.origem_cidade, c.origem_estado, c.destino_cidade, c.destino_estado,
            c.data_prevista_coleta::text AS data_prevista_coleta,
            c.peso_kg, c.valor_frete,
            e.nome AS embarcador
        FROM {SCHEMA}.cargas c
        LEFT JOIN {SCHEMA}.embarcadores e ON e.id = c.embarcador_id
        WHERE c.status = 'disponivel'
          AND c.composicao_veiculo      = '{sql_escape(composicao)}'
          AND c.caracteristica_veiculo  = '{sql_escape(caracteristica)}'
          AND c.peso_kg                 <= {int(capacidade_kg)}
          AND c.origem_estado           = '{orig_sigla}'
          AND c.data_prevista_coleta    {date_filter}
          {f"AND LOWER(c.origem_cidade) = LOWER('{sql_escape(orig_cidade)}')" if orig_cidade else ""}
          {("AND c.id NOT IN (" + ",".join(f"'{sql_escape(i)}'" for i in exclude_ids) + ")") if exclude_ids else ""}
        ORDER BY
          {dest_score} DESC,
          ({weight_util}) DESC,
          c.valor_frete DESC
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


def find_loads(
    driver: dict,
    dest_estado: str,
    orig_estado: str = None,
    data_referencia: str = None,
    dest_cidade: str = None,
    exclude_ids: list[str] | None = None,
    data_coleta: str = None,
    orig_cidade: str = None,
) -> tuple[list[dict], str]:
    """
    Encontra cargas disponíveis compatíveis com o caminhão e rota do motorista.

    Retorna (loads, origem_match) onde origem_match é:
      "cidade_exata" — encontrou cargas na cidade específica pedida
      "estado"       — não havia na cidade; trouxe cargas de outras cidades do mesmo estado
      "nenhuma"      — sem cargas para o estado/período

    Regras:
    - Origem: tenta cidade exata primeiro; se vazio, abre para o estado inteiro
    - Destino flexível: cidade exata > outras cidades do mesmo estado > outros destinos
    - Peso: nunca excede a capacidade do veículo
    - Data: se data_coleta informada, usa exata; se data_referencia, a partir dela; senão hoje → +3d
    - Ordenação: cidade exata (3) + mesmo estado (2) > utilização de peso > valor do frete
    """
    composicao     = driver["veiculo"]["composicao"]
    caracteristica = driver["veiculo"]["caracteristica"]
    capacidade_kg  = driver["veiculo"]["capacidade_kg"]

    orig_sigla = normalizar_estado(orig_estado) if orig_estado else None
    dest_sigla = normalizar_estado(dest_estado) if dest_estado else None

    if not orig_sigla:
        return [], "nenhuma"

    if data_coleta:
        date_filters = [f"= DATE '{data_coleta}'"]
    elif data_referencia:
        date_filters = [
            f"BETWEEN DATE '{data_referencia}' AND DATE '{data_referencia}' + INTERVAL '3 days'",
            f"BETWEEN DATE '{data_referencia}' AND DATE '{data_referencia}' + INTERVAL '7 days'",
        ]
    else:
        date_filters = [
            "= CURRENT_DATE",
            "BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '3 days'",
        ]

    def _buscar(cidade: str | None) -> list[dict]:
        for date_filter in date_filters:
            results = _query_loads(
                composicao, caracteristica, capacidade_kg,
                orig_sigla, dest_sigla, dest_cidade, date_filter, exclude_ids,
                orig_cidade=cidade,
            )
            if results:
                return results
        return []

    # 1ª tentativa: cidade exata (se informada)
    if orig_cidade:
        results = _buscar(orig_cidade)
        if results:
            return results, "cidade_exata"
        # 2ª tentativa: outras cidades do mesmo estado
        results = _buscar(None)
        return results, "estado" if results else "nenhuma"

    # Sem cidade pedida: busca em todo o estado
    results = _buscar(None)
    return results, "estado" if results else "nenhuma"


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
    """Marca a carga como aceita no Lakebase para evitar dupla aceitação."""
    run_sql(
        f"UPDATE {SCHEMA}.cargas SET status = 'aceita', motorista_id = %(motorista_id)s WHERE id = %(carga_id)s",
        {"motorista_id": motorista_id, "carga_id": carga_id},
    )


def get_carga_by_id(carga_id: str) -> dict | None:
    rows = run_sql(
        f"SELECT * FROM {SCHEMA}.cargas WHERE id = %(carga_id)s",
        {"carga_id": carga_id},
    )
    return rows[0] if rows else None
