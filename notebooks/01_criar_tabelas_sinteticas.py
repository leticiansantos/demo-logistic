# Databricks notebook source
# MAGIC %md
# MAGIC # Criar bases sintéticas – Motz Demo
# MAGIC
# MAGIC Gera e grava nas tabelas Delta:
# MAGIC - **transportadoras** – empresas de transporte
# MAGIC - **motoristas** – motoristas vinculados a transportadoras
# MAGIC - **embarcadores** – contratantes de frete
# MAGIC - **cargas** – realizadas (passado), disponíveis (presente) e futuras; conectam embarcadores, transportadoras e motoristas; usam tipos de carga, composição/característica de veículo e embalagem

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# Configuração: workspace 7474658265676932 | Catalog e schema: leticia_santos_classic_stable_catalog.motz_demo
CATALOG = "leticia_santos_classic_stable_catalog"
SCHEMA = "motz_demo"
NUM_TRANSPORTADORAS = 50
NUM_MOTORISTAS = 200
NUM_EMBARCADORES = 40
NUM_CARGAS_REALIZADAS = 150
NUM_CARGAS_DISPONIVEIS = 30
NUM_CARGAS_FUTURAS = 50
SEED = 42

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, DoubleType
from pyspark.sql.functions import col
import uuid
import random
from datetime import date, timedelta
from collections import defaultdict

try:
    from faker import Faker
except ImportError:
    raise ImportError("Execute a célula %pip install faker antes de continuar.")

spark = SparkSession.builder.getOrCreate()
Faker.seed(SEED)
random.seed(SEED)
fake = Faker("pt_BR")

# COMMAND ----------

# Referências do projeto (tipos de carga, veículo e embalagem – Motz)
UFS = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]
CATEGORIAS_CNH = ["C", "D", "E"]

TIPOS_CARGA = ["Adubo","Argamassa","Arroz","Brita","Calcário","Cimento","Cinza","Clínquer","Concreto","Coque","Escória","Farelo de Algodão","Fertilizantes","Fubá","Gesso","Granilha","Milho","Minério","Óleos Vegetais","Pedrisco","Pó Cerâmico","Rejunte","Sacaria","Soja"]

COMPOSICOES_VEICULO = ["Bitrem","Caminhão 3/4","Caminhão Bitruck","Caminhão Toco","Caminhão Truck","Carreta","Rodotrem"]
CARACTERISTICAS_VEICULO = ["Baú","Caçamba","Grade Baixa","Graneleiro","Sider","Silo","Tanque"]

EMBALAGENS_CARGA = ["Balde","Big Bag","Bombona","Caixa","Container","Default","Fardo","Granel","Moinha de Carvão","Pallet","Pallet / Granel","Rolo","Sacas","Saco","Tanque","Unidade","Viga VPL","Volumes"]

# COMMAND ----------

def _fake_cnpj():
    base = [random.randint(0, 9) for _ in range(12)]
    d1 = sum(base[i] * (5 - i % 4) for i in range(12)) % 11
    d1 = 0 if d1 < 2 else 11 - d1
    d2 = sum(base[i] * (6 - i % 5) for i in range(12)) % 11
    d2 = 0 if d2 < 2 else 11 - d2
    return "".join(str(x) for x in base + [d1, d2])

def _fake_cpf():
    base = [random.randint(0, 9) for _ in range(9)]
    d1 = (sum(base[i] * (10 - i) for i in range(9)) * 10) % 11 % 10
    d2 = (sum(base[i] * (11 - i) for i in range(9)) + d1 * 2) * 10 % 11 % 10
    return "".join(str(x) for x in base + [d1, d2])

def _fake_cnh():
    return "".join(str(random.randint(0, 9)) for _ in range(11))

# COMMAND ----------

def generate_transportadoras(n, seed=42):
    random.seed(seed)
    Faker.seed(seed)
    f = Faker("pt_BR")
    hoje = date.today()
    return [{
        "id": str(uuid.uuid4()),
        "nome": f.company(),
        "cnpj": _fake_cnpj(),
        "razao_social": f.company() + " Ltda",
        "endereco": f.street_address(),
        "cidade": f.city(),
        "estado": random.choice(UFS),
        "cep": f.postcode().replace("-", ""),
        "telefone": f.phone_number()[:20],
        "email": f.company_email(),
        "data_cadastro": f.date_between(start_date=hoje - timedelta(days=365*3), end_date=hoje),
        "ativo": random.choice([True, True, False]),
    } for _ in range(n)]

def generate_motoristas(n, transportadora_ids, seed=42):
    random.seed(seed)
    Faker.seed(seed)
    f = Faker("pt_BR")
    hoje = date.today()
    return [{
        "id": str(uuid.uuid4()),
        "transportadora_id": random.choice(transportadora_ids),
        "nome": f.name(),
        "cpf": _fake_cpf(),
        "cnh": _fake_cnh(),
        "categoria_cnh": random.choice(CATEGORIAS_CNH),
        "data_nascimento": f.date_of_birth(minimum_age=23, maximum_age=60),
        "telefone": f.phone_number()[:20],
        "email": f.email(),
        "data_cadastro": f.date_between(start_date=hoje - timedelta(days=365*5), end_date=hoje),
        "ativo": random.choice([True, True, False]),
    } for _ in range(n)]

def generate_embarcadores(n, seed=42):
    random.seed(seed)
    Faker.seed(seed)
    f = Faker("pt_BR")
    hoje = date.today()
    return [{
        "id": str(uuid.uuid4()),
        "nome": f.company(),
        "cnpj": _fake_cnpj(),
        "razao_social": f.company() + " Ltda",
        "endereco": f.street_address(),
        "cidade": f.city(),
        "estado": random.choice(UFS),
        "cep": f.postcode().replace("-", ""),
        "telefone": f.phone_number()[:20],
        "email": f.company_email(),
        "data_cadastro": f.date_between(start_date=hoje - timedelta(days=365*4), end_date=hoje),
        "ativo": random.choice([True, True, False]),
    } for _ in range(n)]

# COMMAND ----------

def generate_cargas(n_realizadas, n_disponiveis, n_futuras, embarcador_ids, transportadora_ids, motoristas_por_transportadora, seed=42):
    random.seed(seed)
    Faker.seed(seed)
    f = Faker("pt_BR")
    hoje = date.today()
    registros = []

    transportadoras_com_motoristas = [tid for tid in transportadora_ids if motoristas_por_transportadora.get(tid)]
    def uma_carga(status, data_coleta, data_entrega_prevista, data_entrega_real):
        if status == "realizada":
            transp_id = random.choice(transportadoras_com_motoristas) if transportadoras_com_motoristas else random.choice(transportadora_ids)
            motoristas_da_transp = motoristas_por_transportadora.get(transp_id, [])
            motorista_id = random.choice(motoristas_da_transp) if motoristas_da_transp else None
        else:
            transp_id = random.choice(transportadora_ids) if random.random() < 0.35 else None
            motoristas_da_transp = motoristas_por_transportadora.get(transp_id, []) if transp_id else []
            motorista_id = random.choice(motoristas_da_transp) if motoristas_da_transp else None

        cid_orig, uf_orig = f.city(), random.choice(UFS)
        cid_dest, uf_dest = f.city(), random.choice(UFS)
        peso = round(random.uniform(1_000, 45_000), 2)
        valor = round(peso * random.uniform(0.08, 0.25), 2)
        data_criacao = data_coleta - timedelta(days=random.randint(1, 30))

        return {
            "id": str(uuid.uuid4()),
            "embarcador_id": random.choice(embarcador_ids),
            "transportadora_id": transp_id,
            "motorista_id": motorista_id,
            "tipo_carga": random.choice(TIPOS_CARGA),
            "composicao_veiculo": random.choice(COMPOSICOES_VEICULO),
            "caracteristica_veiculo": random.choice(CARACTERISTICAS_VEICULO),
            "embalagem": random.choice(EMBALAGENS_CARGA),
            "origem_cidade": cid_orig, "origem_estado": uf_orig,
            "destino_cidade": cid_dest, "destino_estado": uf_dest,
            "data_prevista_coleta": data_coleta,
            "data_prevista_entrega": data_entrega_prevista,
            "data_entrega": data_entrega_real,
            "status": status,
            "peso_kg": peso, "valor_frete": valor,
            "data_criacao": data_criacao,
        }

    for _ in range(n_realizadas):
        dias_atras = random.randint(10, 400)
        coleta = hoje - timedelta(days=dias_atras)
        entrega_prev = coleta + timedelta(days=random.randint(1, 7))
        entrega_real = entrega_prev + timedelta(days=random.randint(-1, 2))
        registros.append(uma_carga("realizada", coleta, entrega_prev, entrega_real))

    for _ in range(n_disponiveis):
        coleta = hoje - timedelta(days=random.randint(0, 5)) if random.random() < 0.6 else hoje
        if coleta > hoje:
            coleta = hoje
        entrega_prev = coleta + timedelta(days=random.randint(1, 10))
        registros.append(uma_carga("disponivel", coleta, entrega_prev, None))

    for _ in range(n_futuras):
        coleta = hoje + timedelta(days=random.randint(1, 90))
        entrega_prev = coleta + timedelta(days=random.randint(1, 10))
        registros.append(uma_carga("futura", coleta, entrega_prev, None))

    return registros

# COMMAND ----------

# Gera transportadoras e grava
dados_transportadoras = generate_transportadoras(NUM_TRANSPORTADORAS, SEED)
ids_transportadoras = [r["id"] for r in dados_transportadoras]

schema_transportadoras = StructType([
    StructField("id", StringType(), False),
    StructField("nome", StringType(), False),
    StructField("cnpj", StringType(), False),
    StructField("razao_social", StringType(), True),
    StructField("endereco", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("cep", StringType(), True),
    StructField("telefone", StringType(), True),
    StructField("email", StringType(), True),
    StructField("data_cadastro", DateType(), True),
    StructField("ativo", BooleanType(), True),
])
df_transportadoras = spark.createDataFrame(dados_transportadoras, schema_transportadoras)
table_transportadoras = f"{CATALOG}.{SCHEMA}.transportadoras"
df_transportadoras.write.format("delta").mode("overwrite").saveAsTable(table_transportadoras)
print(f"Tabela criada: {table_transportadoras} ({df_transportadoras.count()} registros)")

# COMMAND ----------

# Gera motoristas e grava
dados_motoristas = generate_motoristas(NUM_MOTORISTAS, ids_transportadoras, SEED)
ids_motoristas = [r["id"] for r in dados_motoristas]

schema_motoristas = StructType([
    StructField("id", StringType(), False),
    StructField("transportadora_id", StringType(), False),
    StructField("nome", StringType(), False),
    StructField("cpf", StringType(), False),
    StructField("cnh", StringType(), True),
    StructField("categoria_cnh", StringType(), True),
    StructField("data_nascimento", DateType(), True),
    StructField("telefone", StringType(), True),
    StructField("email", StringType(), True),
    StructField("data_cadastro", DateType(), True),
    StructField("ativo", BooleanType(), True),
])
df_motoristas = spark.createDataFrame(dados_motoristas, schema_motoristas)
table_motoristas = f"{CATALOG}.{SCHEMA}.motoristas"
df_motoristas.write.format("delta").mode("overwrite").saveAsTable(table_motoristas)
print(f"Tabela criada: {table_motoristas} ({df_motoristas.count()} registros)")

# COMMAND ----------

# Gera embarcadores e grava
dados_embarcadores = generate_embarcadores(NUM_EMBARCADORES, SEED)
ids_embarcadores = [r["id"] for r in dados_embarcadores]

schema_embarcadores = StructType([
    StructField("id", StringType(), False),
    StructField("nome", StringType(), False),
    StructField("cnpj", StringType(), False),
    StructField("razao_social", StringType(), True),
    StructField("endereco", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("cep", StringType(), True),
    StructField("telefone", StringType(), True),
    StructField("email", StringType(), True),
    StructField("data_cadastro", DateType(), True),
    StructField("ativo", BooleanType(), True),
])
df_embarcadores = spark.createDataFrame(dados_embarcadores, schema_embarcadores)
table_embarcadores = f"{CATALOG}.{SCHEMA}.embarcadores"
df_embarcadores.write.format("delta").mode("overwrite").saveAsTable(table_embarcadores)
print(f"Tabela criada: {table_embarcadores} ({df_embarcadores.count()} registros)")

# COMMAND ----------

# Monta mapa transportadora_id -> [motorista_id, ...] para gerar cargas
motoristas_por_transportadora = defaultdict(list)
for m in dados_motoristas:
    motoristas_por_transportadora[m["transportadora_id"]].append(m["id"])

# Gera cargas (realizadas, disponíveis, futuras) e grava
dados_cargas = generate_cargas(
    NUM_CARGAS_REALIZADAS,
    NUM_CARGAS_DISPONIVEIS,
    NUM_CARGAS_FUTURAS,
    ids_embarcadores,
    ids_transportadoras,
    motoristas_por_transportadora,
    SEED,
)

schema_cargas = StructType([
    StructField("id", StringType(), False),
    StructField("embarcador_id", StringType(), False),
    StructField("transportadora_id", StringType(), True),
    StructField("motorista_id", StringType(), True),
    StructField("tipo_carga", StringType(), False),
    StructField("composicao_veiculo", StringType(), False),
    StructField("caracteristica_veiculo", StringType(), False),
    StructField("embalagem", StringType(), False),
    StructField("origem_cidade", StringType(), True),
    StructField("origem_estado", StringType(), True),
    StructField("destino_cidade", StringType(), True),
    StructField("destino_estado", StringType(), True),
    StructField("data_prevista_coleta", DateType(), True),
    StructField("data_prevista_entrega", DateType(), True),
    StructField("data_entrega", DateType(), True),
    StructField("status", StringType(), False),
    StructField("peso_kg", DoubleType(), True),
    StructField("valor_frete", DoubleType(), True),
    StructField("data_criacao", DateType(), True),
])
df_cargas = spark.createDataFrame(dados_cargas, schema_cargas)
table_cargas = f"{CATALOG}.{SCHEMA}.cargas"
df_cargas.write.format("delta").mode("overwrite").saveAsTable(table_cargas)
print(f"Tabela criada: {table_cargas} ({df_cargas.count()} registros)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo por status (cargas)

# COMMAND ----------

df_cargas.groupBy("status").count().orderBy("status").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amostras das tabelas

# COMMAND ----------

display(spark.table(table_transportadoras).limit(5))

# COMMAND ----------

display(spark.table(table_motoristas).limit(5))

# COMMAND ----------

display(spark.table(table_embarcadores).limit(5))

# COMMAND ----------

display(spark.table(table_cargas).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integridade referencial

# COMMAND ----------

# Motoristas referenciam transportadoras existentes
ids_transp = [r.id for r in spark.table(table_transportadoras).select("id").collect()]
inc_mot = spark.table(table_motoristas).filter(~col("transportadora_id").isin(ids_transp)).count()
print(f"Motoristas com transportadora_id inexistente: {inc_mot} (deve ser 0)")

# Cargas referenciam embarcadores existentes
ids_emb = [r.id for r in spark.table(table_embarcadores).select("id").collect()]
inc_emb = spark.table(table_cargas).filter(~col("embarcador_id").isin(ids_emb)).count()
print(f"Cargas com embarcador_id inexistente: {inc_emb} (deve ser 0)")

# Cargas realizadas com transportadora e motorista preenchidos
realizadas = spark.table(table_cargas).filter(col("status") == "realizada")
sem_transp = realizadas.filter(col("transportadora_id").isNull()).count()
sem_mot = realizadas.filter(col("motorista_id").isNull()).count()
print(f"Cargas realizadas sem transportadora: {sem_transp} (deve ser 0)")
print(f"Cargas realizadas sem motorista: {sem_mot} (deve ser 0)")
