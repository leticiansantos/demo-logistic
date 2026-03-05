"""
Geração de dados sintéticos para logística de caminhões.
Usa Faker (pt_BR) para dados realistas. Produz listas de dicts
compatíveis com criação de DataFrames PySpark.
"""
import uuid
import random
from datetime import date, timedelta
from typing import List, Dict, Any, Optional

# Faker pode não estar instalado no driver Databricks por padrão;
# no notebook, instalar com %pip install faker
try:
    from faker import Faker
except ImportError:
    Faker = None  # type: ignore

from src.tipos_carga import TIPOS_CARGA
from src.caracteristicas_veiculo import COMPOSICOES_VEICULO, CARACTERISTICAS_VEICULO
from src.embalagens_carga import EMBALAGENS_CARGA

# Categorias de CNH para caminhões (C, D, E)
CATEGORIAS_CNH = ["C", "D", "E"]

# UFs brasileiras
UFS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]


def _fake_cnpj() -> str:
    """Gera um CNPJ sintético (14 dígitos, apenas numéricos)."""
    base = [random.randint(0, 9) for _ in range(12)]
    # dígitos verificadores simplificados (não valida algoritmo real)
    d1 = sum(base[i] * (5 - i % 4) for i in range(12)) % 11
    d1 = 0 if d1 < 2 else 11 - d1
    d2 = sum(base[i] * (6 - i % 5) for i in range(12)) % 11
    d2 = 0 if d2 < 2 else 11 - d2
    return "".join(str(x) for x in base + [d1, d2])


def _fake_cpf() -> str:
    """Gera um CPF sintético (11 dígitos)."""
    base = [random.randint(0, 9) for _ in range(9)]
    d1 = (sum(base[i] * (10 - i) for i in range(9)) * 10) % 11 % 10
    d2 = (sum(base[i] * (11 - i) for i in range(9)) + d1 * 2) * 10 % 11 % 10
    return "".join(str(x) for x in base + [d1, d2])


def _fake_cnh() -> str:
    """Gera número de CNH sintético (11 dígitos)."""
    return "".join(str(random.randint(0, 9)) for _ in range(11))


def generate_transportadoras(n: int = 50, seed: int = 42) -> List[Dict[str, Any]]:
    """
    Gera n registros sintéticos de transportadoras.
    """
    if Faker is None:
        raise ImportError("Instale o pacote 'faker': pip install faker")
    random.seed(seed)
    Faker.seed(seed)
    fake = Faker("pt_BR")

    hoje = date.today()
    registros = []
    for _ in range(n):
        nome = fake.company()
        cnpj = _fake_cnpj()
        registros.append({
            "id": str(uuid.uuid4()),
            "nome": nome,
            "cnpj": cnpj,
            "razao_social": nome + " Ltda",
            "endereco": fake.street_address(),
            "cidade": fake.city(),
            "estado": random.choice(UFS),
            "cep": fake.postcode().replace("-", ""),
            "telefone": fake.phone_number()[:20],
            "email": fake.company_email(),
            "data_cadastro": fake.date_between(start_date=hoje - timedelta(days=365*3), end_date=hoje),
            "ativo": random.choice([True, True, False]),  # maioria ativo
        })
    return registros


def generate_motoristas(
    n: int = 200,
    transportadora_ids: List[str] | None = None,
    seed: int = 42,
) -> List[Dict[str, Any]]:
    """
    Gera n registros sintéticos de motoristas.
    Se transportadora_ids for None, gera IDs fictícios (use após criar transportadoras).
    """
    if Faker is None:
        raise ImportError("Instale o pacote 'faker': pip install faker")
    random.seed(seed)
    Faker.seed(seed)
    fake = Faker("pt_BR")

    if not transportadora_ids:
        transportadora_ids = [str(uuid.uuid4()) for _ in range(20)]

    hoje = date.today()
    registros = []
    for _ in range(n):
        registros.append({
            "id": str(uuid.uuid4()),
            "transportadora_id": random.choice(transportadora_ids),
            "nome": fake.name(),
            "cpf": _fake_cpf(),
            "cnh": _fake_cnh(),
            "categoria_cnh": random.choice(CATEGORIAS_CNH),
            "data_nascimento": fake.date_of_birth(minimum_age=23, maximum_age=60),
            "telefone": fake.phone_number()[:20],
            "email": fake.email(),
            "data_cadastro": fake.date_between(start_date=hoje - timedelta(days=365*5), end_date=hoje),
            "ativo": random.choice([True, True, False]),
        })
    return registros


def generate_embarcadores(n: int = 40, seed: int = 42) -> List[Dict[str, Any]]:
    """Gera n registros sintéticos de embarcadores (contratantes de frete)."""
    if Faker is None:
        raise ImportError("Instale o pacote 'faker': pip install faker")
    random.seed(seed)
    Faker.seed(seed)
    fake = Faker("pt_BR")
    hoje = date.today()
    registros = []
    for _ in range(n):
        nome = fake.company()
        registros.append({
            "id": str(uuid.uuid4()),
            "nome": nome,
            "cnpj": _fake_cnpj(),
            "razao_social": nome + " Ltda",
            "endereco": fake.street_address(),
            "cidade": fake.city(),
            "estado": random.choice(UFS),
            "cep": fake.postcode().replace("-", ""),
            "telefone": fake.phone_number()[:20],
            "email": fake.company_email(),
            "data_cadastro": fake.date_between(start_date=hoje - timedelta(days=365 * 4), end_date=hoje),
            "ativo": random.choice([True, True, False]),
        })
    return registros


def generate_cargas(
    n_realizadas: int = 150,
    n_disponiveis: int = 30,
    n_futuras: int = 50,
    embarcador_ids: Optional[List[str]] = None,
    transportadora_ids: Optional[List[str]] = None,
    motoristas_por_transportadora: Optional[Dict[str, List[str]]] = None,
    seed: int = 42,
) -> List[Dict[str, Any]]:
    """
    Gera cargas sintéticas: realizadas (passado), disponíveis (presente), futuras.
    Conecta embarcadores, transportadoras e motoristas. Usa tipos de carga,
    composição/característica de veículo e embalagem do projeto.
    """
    if Faker is None:
        raise ImportError("Instale o pacote 'faker': pip install faker")
    random.seed(seed)
    Faker.seed(seed)
    fake = Faker("pt_BR")
    hoje = date.today()

    if not embarcador_ids:
        embarcador_ids = [str(uuid.uuid4()) for _ in range(20)]
    if not transportadora_ids:
        transportadora_ids = [str(uuid.uuid4()) for _ in range(20)]
    if not motoristas_por_transportadora:
        motoristas_por_transportadora = {tid: [] for tid in transportadora_ids}

    transportadoras_com_motorista = [tid for tid in transportadora_ids if motoristas_por_transportadora.get(tid)]
    def _uma_carga(status: str, data_coleta: date, data_entrega_prevista: date, data_entrega_real: Optional[date]) -> Dict[str, Any]:
        if status == "realizada":
            transp_id = random.choice(transportadoras_com_motorista) if transportadoras_com_motorista else random.choice(transportadora_ids)
            motoristas_da_transp = motoristas_por_transportadora.get(transp_id, [])
            motorista_id = random.choice(motoristas_da_transp) if motoristas_da_transp else None
        else:
            transp_id = random.choice(transportadora_ids) if random.random() < 0.35 else None
            motoristas_da_transp = motoristas_por_transportadora.get(transp_id, []) if transp_id else []
            motorista_id = random.choice(motoristas_da_transp) if motoristas_da_transp else None

        cid_orig = fake.city()
        uf_orig = random.choice(UFS)
        cid_dest = fake.city()
        uf_dest = random.choice(UFS)
        peso = round(random.uniform(1_000, 45_000), 2)
        valor = round(peso * random.uniform(0.08, 0.25), 2)

        return {
            "id": str(uuid.uuid4()),
            "embarcador_id": random.choice(embarcador_ids),
            "transportadora_id": transp_id,
            "motorista_id": motorista_id,
            "tipo_carga": random.choice(TIPOS_CARGA),
            "composicao_veiculo": random.choice(COMPOSICOES_VEICULO),
            "caracteristica_veiculo": random.choice(CARACTERISTICAS_VEICULO),
            "embalagem": random.choice(EMBALAGENS_CARGA),
            "origem_cidade": cid_orig,
            "origem_estado": uf_orig,
            "destino_cidade": cid_dest,
            "destino_estado": uf_dest,
            "data_prevista_coleta": data_coleta,
            "data_prevista_entrega": data_entrega_prevista,
            "data_entrega": data_entrega_real,
            "status": status,
            "peso_kg": peso,
            "valor_frete": valor,
            "data_criacao": data_coleta - timedelta(days=random.randint(1, 30)),
        }

    registros = []

    # Realizadas: coleta e entrega no passado
    for _ in range(n_realizadas):
        dias_atras = random.randint(10, 400)
        coleta = hoje - timedelta(days=dias_atras)
        entrega_prev = coleta + timedelta(days=random.randint(1, 7))
        entrega_real = entrega_prev + timedelta(days=random.randint(-1, 2))
        registros.append(_uma_carga("realizada", coleta, entrega_prev, entrega_real))

    # Disponíveis: coleta até hoje, entrega no futuro próximo
    for _ in range(n_disponiveis):
        coleta = hoje - timedelta(days=random.randint(0, 5)) if random.random() < 0.6 else hoje
        if coleta > hoje:
            coleta = hoje
        entrega_prev = coleta + timedelta(days=random.randint(1, 10))
        registros.append(_uma_carga("disponivel", coleta, entrega_prev, None))

    # Futuras: coleta no futuro
    for _ in range(n_futuras):
        coleta = hoje + timedelta(days=random.randint(1, 90))
        entrega_prev = coleta + timedelta(days=random.randint(1, 10))
        registros.append(_uma_carga("futura", coleta, entrega_prev, None))

    return registros
