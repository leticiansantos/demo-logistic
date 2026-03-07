"""
Gera cargas sintéticas no Lakebase cobrindo:
- Todos os dias de hoje até 30/04/2026
- Pelo menos 1 carga para cada par (origem_estado, destino_estado) por dia
- Cidades variadas dentro de cada estado como origem
- Combinações de veículo cicladas para garantir cobertura de todos os tipos

Uso:
    python scripts/seed_cargas_lakebase.py

Prereqs:
    pip install psycopg2-binary
    databricks CLI v0.285.0+ autenticado com perfil DEFAULT
"""

import json
import random
import subprocess
import uuid
from datetime import date, timedelta
from itertools import cycle

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Config Lakebase
# ---------------------------------------------------------------------------
LAKEBASE_PROJECT  = "motz-demo"
LAKEBASE_BRANCH   = "production"
LAKEBASE_ENDPOINT = "primary"
LAKEBASE_HOST     = "ep-twilight-art-d1liaemf.database.us-west-2.cloud.databricks.com"
LAKEBASE_DATABASE = "motz"
LAKEBASE_PORT     = 5432
SCHEMA            = "public"

# ---------------------------------------------------------------------------
# Domínios
# ---------------------------------------------------------------------------
UFS = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
    "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
    "RS","RO","RR","SC","SP","SE","TO",
]

CIDADES_POR_UF = {
    "AC": ["Rio Branco","Cruzeiro do Sul","Sena Madureira","Tarauacá","Feijó","Brasileia","Epitaciolândia"],
    "AL": ["Maceió","Arapiraca","Rio Largo","Palmeira dos Índios","União dos Palmares","Penedo","Delmiro Gouveia"],
    "AP": ["Macapá","Santana","Laranjal do Jari","Oiapoque","Mazagão","Porto Grande","Pedra Branca do Amapari"],
    "AM": ["Manaus","Parintins","Itacoatiara","Manacapuru","Coari","Tefé","Tabatinga","Maués"],
    "BA": ["Salvador","Feira de Santana","Vitória da Conquista","Camaçari","Ilhéus","Jequié","Lauro de Freitas","Juazeiro","Barreiras"],
    "CE": ["Fortaleza","Caucaia","Juazeiro do Norte","Maracanaú","Crato","Sobral","Iguatu","Quixadá"],
    "DF": ["Brasília","Ceilândia","Taguatinga","Gama","Planaltina","Samambaia","Sobradinho"],
    "ES": ["Vitória","Vila Velha","Serra","Cariacica","Cachoeiro de Itapemirim","Linhares","Colatina"],
    "GO": ["Goiânia","Aparecida de Goiânia","Anápolis","Rio Verde","Luziânia","Itumbiara","Catalão","Jataí"],
    "MA": ["São Luís","Imperatriz","São José de Ribamar","Timon","Caxias","Codó","Açailândia","Bacabal"],
    "MT": ["Cuiabá","Várzea Grande","Rondonópolis","Sinop","Tangará da Serra","Sorriso","Lucas do Rio Verde"],
    "MS": ["Campo Grande","Dourados","Três Lagoas","Corumbá","Ponta Porã","Naviraí","Nova Andradina"],
    "MG": ["Belo Horizonte","Uberlândia","Contagem","Juiz de Fora","Montes Claros","Uberaba","Governador Valadares","Ipatinga","Divinópolis"],
    "PA": ["Belém","Ananindeua","Santarém","Marabá","Castanhal","Parauapebas","Itaituba","Altamira"],
    "PB": ["João Pessoa","Campina Grande","Santa Rita","Patos","Bayeux","Sousa","Cajazeiras"],
    "PR": ["Curitiba","Londrina","Maringá","Ponta Grossa","Cascavel","São José dos Pinhais","Foz do Iguaçu","Colombo"],
    "PE": ["Recife","Caruaru","Olinda","Jaboatão dos Guararapes","Petrolina","Paulista","Vitória de Santo Antão"],
    "PI": ["Teresina","Parnaíba","Picos","Floriano","Barras","Oeiras","Campo Maior"],
    "RJ": ["Rio de Janeiro","Niterói","Duque de Caxias","Nova Iguaçu","Campos dos Goytacazes","Belford Roxo","São Gonçalo"],
    "RN": ["Natal","Mossoró","Parnamirim","Caicó","Assú","Currais Novos","Santa Cruz"],
    "RS": ["Porto Alegre","Caxias do Sul","Pelotas","Santa Maria","Canoas","Novo Hamburgo","São Leopoldo","Gravataí"],
    "RO": ["Porto Velho","Ji-Paraná","Ariquemes","Vilhena","Cacoal","Rolim de Moura","Guajará-Mirim"],
    "RR": ["Boa Vista","Caracaraí","Rorainópolis","Alto Alegre","Mucajaí","Pacaraima","Bonfim"],
    "SC": ["Florianópolis","Joinville","Blumenau","São José","Chapecó","Criciúma","Itajaí","Lages"],
    "SP": ["São Paulo","Campinas","Santos","Sorocaba","Ribeirão Preto","São José dos Campos","Osasco","Guarulhos","Jundiaí","Bauru","Piracicaba","São Bernardo do Campo"],
    "SE": ["Aracaju","Nossa Senhora do Socorro","Lagarto","Itabaiana","São Cristóvão","Estância","Tobias Barreto"],
    "TO": ["Palmas","Araguaína","Gurupi","Porto Nacional","Paraíso do Tocantins","Araguatins","Colinas do Tocantins"],
}

TIPOS_CARGA = [
    "Adubo","Argamassa","Arroz","Brita","Calcário","Cimento","Cinza",
    "Clínquer","Concreto","Coque","Escória","Farelo de Algodão",
    "Fertilizantes","Fubá","Gesso","Granilha","Milho","Minério",
    "Óleos Vegetais","Pedrisco","Pó Cerâmico","Rejunte","Sacaria","Soja",
]

COMPOSICOES = [
    "Bitrem","Caminhão 3/4","Caminhão Bitruck","Caminhão Toco",
    "Caminhão Truck","Carreta","Rodotrem",
]

CARACTERISTICAS = [
    "Baú","Caçamba","Grade Baixa","Graneleiro","Sider","Silo","Tanque",
]

EMBALAGENS = [
    "Balde","Big Bag","Bombona","Caixa","Container","Default","Fardo",
    "Granel","Pallet","Sacas","Saco","Tanque","Unidade","Volumes",
]

# Capacidade típica por composição (kg)
CAPACIDADE_POR_COMPOSICAO = {
    "Bitrem":          45_000,
    "Caminhão 3/4":     4_000,
    "Caminhão Bitruck": 14_000,
    "Caminhão Toco":    7_000,
    "Caminhão Truck":  14_000,
    "Carreta":         28_000,
    "Rodotrem":        57_000,
}

# ---------------------------------------------------------------------------
# Lakebase helpers
# ---------------------------------------------------------------------------

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
    user_out = subprocess.run(
        ["databricks", "current-user", "me", "-p", "DEFAULT", "--output", "json"],
        capture_output=True, text=True, check=True,
    )
    token = json.loads(token_out.stdout)["token"]
    email = json.loads(user_out.stdout)["userName"]
    return psycopg2.connect(
        host=LAKEBASE_HOST, port=LAKEBASE_PORT, database=LAKEBASE_DATABASE,
        user=email, password=token, sslmode="require",
    )


def fetch_embarcador_ids(cur) -> list[str]:
    cur.execute(f"SELECT id FROM {SCHEMA}.embarcadores")
    return [r[0] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Geração de cargas
# ---------------------------------------------------------------------------

def gerar_carga(
    dia: date,
    uf_orig: str,
    uf_dest: str,
    embarcador_ids: list[str],
    composicao: str,
    caracteristica: str,
    rng: random.Random,
) -> dict:
    capacidade = CAPACIDADE_POR_COMPOSICAO[composicao]
    peso = round(rng.uniform(capacidade * 0.3, capacidade * 0.95), 2)
    valor = round(peso * rng.uniform(0.08, 0.25), 2)
    entrega_prev = dia + timedelta(days=rng.randint(1, 8))
    data_criacao = dia - timedelta(days=rng.randint(1, 15))

    cidade_orig = rng.choice(CIDADES_POR_UF[uf_orig])
    cidade_dest = rng.choice(CIDADES_POR_UF[uf_dest])

    return {
        "id": str(uuid.uuid4()),
        "embarcador_id": rng.choice(embarcador_ids),
        "transportadora_id": None,
        "motorista_id": None,
        "tipo_carga": rng.choice(TIPOS_CARGA),
        "composicao_veiculo": composicao,
        "caracteristica_veiculo": caracteristica,
        "embalagem": rng.choice(EMBALAGENS),
        "origem_cidade": cidade_orig,
        "origem_estado": uf_orig,
        "destino_cidade": cidade_dest,
        "destino_estado": uf_dest,
        "data_prevista_coleta": dia,
        "data_prevista_entrega": entrega_prev,
        "data_entrega": None,
        "status": "disponivel",
        "peso_kg": peso,
        "valor_frete": valor,
        "data_criacao": data_criacao,
    }


def gerar_lote(
    data_inicio: date,
    data_fim: date,
    embarcador_ids: list[str],
    seed: int = 42,
) -> list[dict]:
    rng = random.Random(seed)
    registros = []

    # Cicla pelos combos de veículo para distribuir uniformemente
    combos = [(c, k) for c in COMPOSICOES for k in CARACTERISTICAS]
    combo_cycle = cycle(combos)

    dia = data_inicio
    while dia <= data_fim:
        for uf_orig in UFS:
            for uf_dest in UFS:
                if uf_orig == uf_dest:
                    continue
                composicao, caracteristica = next(combo_cycle)
                registros.append(
                    gerar_carga(dia, uf_orig, uf_dest, embarcador_ids,
                                composicao, caracteristica, rng)
                )
        dia += timedelta(days=1)

    return registros


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

INSERT_SQL = f"""
    INSERT INTO {SCHEMA}.cargas (
        id, embarcador_id, transportadora_id, motorista_id,
        tipo_carga, composicao_veiculo, caracteristica_veiculo, embalagem,
        origem_cidade, origem_estado, destino_cidade, destino_estado,
        data_prevista_coleta, data_prevista_entrega, data_entrega,
        status, peso_kg, valor_frete, data_criacao
    ) VALUES (
        %(id)s, %(embarcador_id)s, %(transportadora_id)s, %(motorista_id)s,
        %(tipo_carga)s, %(composicao_veiculo)s, %(caracteristica_veiculo)s, %(embalagem)s,
        %(origem_cidade)s, %(origem_estado)s, %(destino_cidade)s, %(destino_estado)s,
        %(data_prevista_coleta)s, %(data_prevista_entrega)s, %(data_entrega)s,
        %(status)s, %(peso_kg)s, %(valor_frete)s, %(data_criacao)s
    )
    ON CONFLICT (id) DO NOTHING
"""


def inserir_em_lotes(cur, conn, registros: list[dict], batch_size: int = 500):
    total = len(registros)
    for i in range(0, total, batch_size):
        lote = registros[i:i + batch_size]
        psycopg2.extras.execute_batch(cur, INSERT_SQL, lote, page_size=batch_size)
        conn.commit()
        print(f"  inseridos {min(i + batch_size, total)}/{total}", end="\r")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    hoje = date.today()
    data_fim = date(2026, 4, 30)

    print(f"Período: {hoje} → {data_fim}")
    dias = (data_fim - hoje).days + 1
    pares_por_dia = len(UFS) * (len(UFS) - 1)
    total_estimado = dias * pares_por_dia
    print(f"Dias: {dias} | Pares por dia: {pares_por_dia} | Total estimado: {total_estimado:,}")

    print("Conectando ao Lakebase...")
    conn = lakebase_conn()
    cur  = conn.cursor()

    print("Buscando embarcadores...")
    embarcador_ids = fetch_embarcador_ids(cur)
    if not embarcador_ids:
        raise RuntimeError("Nenhum embarcador encontrado no Lakebase. Execute a migração primeiro.")
    print(f"  {len(embarcador_ids)} embarcadores disponíveis")

    print("Gerando registros em memória...")
    registros = gerar_lote(hoje, data_fim, embarcador_ids)
    print(f"  {len(registros):,} cargas geradas")

    print("Inserindo no Lakebase...")
    inserir_em_lotes(cur, conn, registros)

    cur.close()
    conn.close()
    print(f"Concluído: {len(registros):,} cargas inseridas de {hoje} até {data_fim}.")


if __name__ == "__main__":
    main()
