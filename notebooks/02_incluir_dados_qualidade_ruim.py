# Databricks notebook source
# MAGIC %md
# MAGIC # Incluir dados com problemas de qualidade (Data Quality demo)
# MAGIC
# MAGIC Este notebook **adiciona** registros intencionalmente incorretos em cada tabela, para demonstrar:
# MAGIC - Regras de validação (formatos, domínios, integridade referencial)
# MAGIC - Métricas e monitoramento de data quality
# MAGIC - Limpeza e correção de dados
# MAGIC
# MAGIC **Pré-requisito:** executar antes o notebook `01_criar_tabelas_sinteticas.py` para ter as tabelas base.

# COMMAND ----------

# Configuração (mesmo catalog/schema do notebook 01)
CATALOG = "leticia_santos_classic_stable_catalog"
SCHEMA = "motz_demo"
# Quantidade de registros "ruins" a inserir por tipo (ajuste para o demo)
NUM_TRANSPORTADORAS_RUIM = 5
NUM_MOTORISTAS_RUIM = 5
NUM_EMBARCADORES_RUIM = 5
NUM_CARGAS_RUIM = 8

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, DoubleType
from datetime import date, timedelta
import uuid

spark = SparkSession.builder.getOrCreate()
hoje = date.today()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Transportadoras – dados ruins
# MAGIC
# MAGIC | Problema | Exemplo |
# MAGIC |----------|---------|
# MAGIC | CNPJ tamanho errado | 13 ou 15 dígitos |
# MAGIC | Nome vazio | "" ou só espaços |
# MAGIC | Email inválido | sem @, domínio errado |
# MAGIC | UF inexistente | "XX" |
# MAGIC | data_cadastro no futuro | amanhã |

# COMMAND ----------

def bad_transportadoras(n):
    rows = []
    for i in range(n):
        base = {
            "id": str(uuid.uuid4()),
            "razao_social": None,
            "endereco": "Rua Qualquer, 100",
            "cidade": "São Paulo",
            "cep": "01310100",
            "telefone": "11999999999",
            "data_cadastro": hoje - timedelta(days=100),
            "ativo": True,
        }
        if i == 0:
            base["nome"] = "Transporte OK Ltda"
            base["cnpj"] = "12345678000199"  # 14 dígitos mas dígitos verificadores inválidos
            base["estado"] = "SP"
            base["email"] = "contato@transporte.com"
        elif i == 1:
            base["nome"] = ""  # Nome vazio
            base["cnpj"] = "11222333000181"
            base["estado"] = "MG"
            base["email"] = "email@valido.com"
        elif i == 2:
            base["nome"] = "   "  # Só espaços
            base["cnpj"] = "11222333000181"
            base["estado"] = "MG"
            base["email"] = "email@valido.com"
        elif i == 3:
            base["nome"] = "Sem Arroba Ltda"
            base["cnpj"] = "11222333000181"
            base["estado"] = "SP"
            base["email"] = "emailinvalido.sem.arroba"  # Email sem @
        elif i == 4:
            base["nome"] = "UF Fantasma Ltda"
            base["cnpj"] = "12345"  # CNPJ curto demais
            base["estado"] = "XX"  # UF inexistente
            base["email"] = "contato@uf.com"
        else:
            base["nome"] = "Transporte Extra Ltda"
            base["cnpj"] = "11222333000181"
            base["estado"] = "RJ"
            base["email"] = "contato@extra.com"
        base["data_cadastro"] = hoje + timedelta(days=10) if i == 4 else base["data_cadastro"]  # futuro
        rows.append(base)
    return rows[:n]

# COMMAND ----------

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
df_bad_transp = spark.createDataFrame(bad_transportadoras(NUM_TRANSPORTADORAS_RUIM), schema_transportadoras)
table_transportadoras = f"{CATALOG}.{SCHEMA}.transportadoras"
df_bad_transp.write.format("delta").mode("append").saveAsTable(table_transportadoras)
print(f"Append: {NUM_TRANSPORTADORAS_RUIM} registros com problemas em {table_transportadoras}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Motoristas – dados ruins
# MAGIC
# MAGIC | Problema | Exemplo |
# MAGIC |----------|---------|
# MAGIC | CPF tamanho errado | 10 dígitos |
# MAGIC | transportadora_id inexistente | UUID que não existe na tabela transportadoras |
# MAGIC | categoria_cnh inválida | "A" ou "B" (não permitido para caminhão) |
# MAGIC | Email inválido | sem @ |

# COMMAND ----------

# UUID que não existe em transportadoras (FK quebrada)
FAKE_TRANSPORTADORA_ID = "00000000-0000-0000-0000-000000000001"

def bad_motoristas(n):
    rows = []
    for i in range(n):
        base = {
            "id": str(uuid.uuid4()),
            "nome": "Motorista Teste",
            "cnh": "12345678901",
            "categoria_cnh": "E",
            "data_nascimento": date(1985, 5, 15),
            "telefone": "11988887777",
            "data_cadastro": hoje - timedelta(days=200),
            "ativo": True,
        }
        if i == 0:
            base["transportadora_id"] = FAKE_TRANSPORTADORA_ID  # FK inexistente
            base["cpf"] = "1234567890"  # CPF com 10 dígitos
            base["email"] = "motorista@email.com"
        elif i == 1:
            base["transportadora_id"] = FAKE_TRANSPORTADORA_ID
            base["cpf"] = "12345678901"
            base["email"] = "motorista@email.com"
        elif i == 2:
            base["transportadora_id"] = FAKE_TRANSPORTADORA_ID
            base["cpf"] = "98765432100"
            base["categoria_cnh"] = "A"  # Categoria inválida para caminhão
            base["email"] = "motorista@email.com"
        elif i == 3:
            base["transportadora_id"] = FAKE_TRANSPORTADORA_ID
            base["cpf"] = "11122233344"
            base["categoria_cnh"] = "B"
            base["email"] = "motorista@email.com"
        elif i == 4:
            base["transportadora_id"] = FAKE_TRANSPORTADORA_ID
            base["cpf"] = "55566677788"
            base["email"] = "emailsemarroba"  # Email inválido
        else:
            base["transportadora_id"] = FAKE_TRANSPORTADORA_ID
            base["cpf"] = "11122233344"
            base["email"] = "motorista@email.com"
        rows.append(base)
    return rows[:n]

# COMMAND ----------

# Parte dos motoristas com FK inválida (órfãos), parte com FK válida mas outros erros (CPF, categoria, email)
transportadoras_existentes = spark.table(table_transportadoras).limit(1)
ids_transp = [r.id for r in transportadoras_existentes.collect()]
id_transp_valido = ids_transp[0] if ids_transp else FAKE_TRANSPORTADORA_ID

def bad_motoristas_final(n):
    rows = bad_motoristas(n)
    for i, r in enumerate(rows):
        if i >= 3:
            r["transportadora_id"] = id_transp_valido  # FK válida, mas ex.: CPF/categoria/email ruins
    return rows

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
df_bad_mot = spark.createDataFrame(bad_motoristas_final(NUM_MOTORISTAS_RUIM), schema_motoristas)
table_motoristas = f"{CATALOG}.{SCHEMA}.motoristas"
df_bad_mot.write.format("delta").mode("append").saveAsTable(table_motoristas)
print(f"Append: {NUM_MOTORISTAS_RUIM} registros com problemas em {table_motoristas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Embarcadores – dados ruins
# MAGIC
# MAGIC | Problema | Exemplo |
# MAGIC |----------|---------|
# MAGIC | CNPJ inválido | tamanho errado |
# MAGIC | Nome vazio | "" |
# MAGIC | UF inexistente | "YY" |
# MAGIC | Email inválido | formato errado |

# COMMAND ----------

def bad_embarcadores(n):
    rows = []
    for i in range(n):
        base = {
            "id": str(uuid.uuid4()),
            "razao_social": None,
            "endereco": "Av. Brasil, 1000",
            "cidade": "Curitiba",
            "cep": "80000000",
            "telefone": "4199999999",
            "data_cadastro": hoje - timedelta(days=150),
            "ativo": True,
        }
        if i == 0:
            base["nome"] = ""
            base["cnpj"] = "11222333000181"
            base["estado"] = "PR"
            base["email"] = "embarcador@email.com"
        elif i == 1:
            base["nome"] = "Embarcador CNPJ Curto"
            base["cnpj"] = "12345678"  # CNPJ curto
            base["estado"] = "PR"
            base["email"] = "embarcador@email.com"
        elif i == 2:
            base["nome"] = "Embarcador UF Invalida"
            base["cnpj"] = "11222333000181"
            base["estado"] = "YY"
            base["email"] = "embarcador@email.com"
        elif i == 3:
            base["nome"] = "Embarcador Email Ruim"
            base["cnpj"] = "11222333000181"
            base["estado"] = "SC"
            base["email"] = "sem-arroba"
        elif i == 4:
            base["nome"] = "Embarcador Duplicado Nome"
            base["cnpj"] = "11222333000181"
            base["estado"] = "RS"
            base["email"] = "embarcador@email.com"
        else:
            base["nome"] = "Embarcador Extra"
            base["cnpj"] = "11222333000181"
            base["estado"] = "RS"
            base["email"] = "extra@email.com"
        rows.append(base)
    return rows[:n]

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
table_embarcadores = f"{CATALOG}.{SCHEMA}.embarcadores"
df_bad_emb = spark.createDataFrame(bad_embarcadores(NUM_EMBARCADORES_RUIM), schema_embarcadores)
df_bad_emb.write.format("delta").mode("append").saveAsTable(table_embarcadores)
print(f"Append: {NUM_EMBARCADORES_RUIM} registros com problemas em {table_embarcadores}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cargas – dados ruins
# MAGIC
# MAGIC | Problema | Exemplo |
# MAGIC |----------|---------|
# MAGIC | embarcador_id inexistente | UUID fake |
# MAGIC | status x datas inconsistentes | realizada sem data_entrega |
# MAGIC | data_entrega &lt; data_coleta | datas invertidas |
# MAGIC | tipo_carga / composição / embalagem fora do domínio | typo ou valor não cadastrado |
# MAGIC | peso_kg ou valor_frete negativos | -100 |
# MAGIC | Origem = destino | mesma cidade/estado |

# COMMAND ----------

FAKE_EMBARCADOR_ID = "00000000-0000-0000-0000-000000000002"
FAKE_TRANSP_ID = "00000000-0000-0000-0000-000000000003"
FAKE_MOTORISTA_ID = "00000000-0000-0000-0000-000000000004"

ids_emb = [r.id for r in spark.table(table_embarcadores).select("id").limit(3).collect()]
id_emb_valido = ids_emb[0] if ids_emb else FAKE_EMBARCADOR_ID
ids_transp_list = [r.id for r in spark.table(table_transportadoras).select("id").limit(3).collect()]
id_transp_valido_carga = ids_transp_list[0] if ids_transp_list else FAKE_TRANSP_ID

def bad_cargas(n):
    rows = []
    for i in range(n):
        base = {
            "origem_cidade": "São Paulo",
            "origem_estado": "SP",
            "destino_cidade": "Curitiba",
            "destino_estado": "PR",
            "data_prevista_coleta": hoje - timedelta(days=5),
            "data_prevista_entrega": hoje + timedelta(days=2),
            "data_entrega": None,
            "data_criacao": hoje - timedelta(days=10),
        }
        if i == 0:
            base["embarcador_id"] = FAKE_EMBARCADOR_ID
            base["transportadora_id"] = id_transp_valido_carga
            base["motorista_id"] = None
            base["tipo_carga"] = "Cimento"
            base["composicao_veiculo"] = "Carreta"
            base["caracteristica_veiculo"] = "Baú"
            base["embalagem"] = "Granel"
            base["status"] = "disponivel"
            base["peso_kg"] = 15000.0
            base["valor_frete"] = 3500.0
        elif i == 1:
            base["embarcador_id"] = id_emb_valido
            base["transportadora_id"] = id_transp_valido_carga
            base["motorista_id"] = None
            base["tipo_carga"] = "Cimento"
            base["composicao_veiculo"] = "Carreta"
            base["caracteristica_veiculo"] = "Baú"
            base["embalagem"] = "Granel"
            base["status"] = "realizada"
            base["data_entrega"] = None  # realizada sem data_entrega
            base["peso_kg"] = 20000.0
            base["valor_frete"] = 5000.0
        elif i == 2:
            base["embarcador_id"] = id_emb_valido
            base["transportadora_id"] = id_transp_valido_carga
            base["motorista_id"] = None
            base["tipo_carga"] = "Soja"
            base["composicao_veiculo"] = "Carreta"
            base["caracteristica_veiculo"] = "Graneleiro"
            base["embalagem"] = "Granel"
            base["status"] = "realizada"
            base["data_prevista_coleta"] = hoje - timedelta(days=10)
            base["data_prevista_entrega"] = hoje - timedelta(days=15)  # entrega antes da coleta
            base["data_entrega"] = hoje - timedelta(days=8)
            base["peso_kg"] = 25000.0
            base["valor_frete"] = 6000.0
        elif i == 3:
            base["embarcador_id"] = id_emb_valido
            base["transportadora_id"] = id_transp_valido_carga
            base["motorista_id"] = None
            base["tipo_carga"] = "Cimentoo"  # typo – fora do domínio
            base["composicao_veiculo"] = "Carreta"
            base["caracteristica_veiculo"] = "Baú"
            base["embalagem"] = "Granel"
            base["status"] = "disponivel"
            base["peso_kg"] = 18000.0
            base["valor_frete"] = 4000.0
        elif i == 4:
            base["embarcador_id"] = id_emb_valido
            base["transportadora_id"] = id_transp_valido_carga
            base["motorista_id"] = None
            base["tipo_carga"] = "Cimento"
            base["composicao_veiculo"] = "Carreta Falsa"  # valor não cadastrado
            base["caracteristica_veiculo"] = "Baú"
            base["embalagem"] = "Granel"
            base["status"] = "disponivel"
            base["peso_kg"] = 12000.0
            base["valor_frete"] = 3000.0
        elif i == 5:
            base["embarcador_id"] = id_emb_valido
            base["transportadora_id"] = id_transp_valido_carga
            base["motorista_id"] = None
            base["tipo_carga"] = "Cimento"
            base["composicao_veiculo"] = "Carreta"
            base["caracteristica_veiculo"] = "Baú"
            base["embalagem"] = "Granell"  # typo
            base["status"] = "futura"
            base["peso_kg"] = -500.0   # peso negativo
            base["valor_frete"] = 2000.0
        elif i == 6:
            base["embarcador_id"] = id_emb_valido
            base["transportadora_id"] = id_transp_valido_carga
            base["motorista_id"] = None
            base["tipo_carga"] = "Soja"
            base["composicao_veiculo"] = "Carreta"
            base["caracteristica_veiculo"] = "Graneleiro"
            base["embalagem"] = "Granel"
            base["status"] = "disponivel"
            base["peso_kg"] = 20000.0
            base["valor_frete"] = -1000.0  # valor negativo
        elif i == 7:
            base["embarcador_id"] = id_emb_valido
            base["transportadora_id"] = id_transp_valido_carga
            base["motorista_id"] = None
            base["tipo_carga"] = "Milho"
            base["composicao_veiculo"] = "Carreta"
            base["caracteristica_veiculo"] = "Graneleiro"
            base["embalagem"] = "Granel"
            base["origem_cidade"] = "Porto Alegre"
            base["origem_estado"] = "RS"
            base["destino_cidade"] = "Porto Alegre"
            base["destino_estado"] = "RS"
            base["status"] = "futura"
            base["peso_kg"] = 15000.0
            base["valor_frete"] = 3500.0
        else:
            base["embarcador_id"] = id_emb_valido
            base["transportadora_id"] = id_transp_valido_carga
            base["motorista_id"] = None
            base["tipo_carga"] = "Cimento"
            base["composicao_veiculo"] = "Carreta"
            base["caracteristica_veiculo"] = "Baú"
            base["embalagem"] = "Granel"
            base["status"] = "disponivel"
            base["peso_kg"] = 10000.0
            base["valor_frete"] = 2500.0
        base["id"] = str(uuid.uuid4())
        rows.append(base)
    return rows[:n]

# COMMAND ----------

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
df_bad_cargas = spark.createDataFrame(bad_cargas(NUM_CARGAS_RUIM), schema_cargas)
table_cargas = f"{CATALOG}.{SCHEMA}.cargas"
df_bad_cargas.write.format("delta").mode("append").saveAsTable(table_cargas)
print(f"Append: {NUM_CARGAS_RUIM} registros com problemas em {table_cargas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo – problemas inseridos por tabela
# MAGIC
# MAGIC Use essas regras em pipelines de Data Quality (ex.: Delta Live Tables expectations, Great Expectations, ou queries de monitoramento):

# COMMAND ----------

# MAGIC %md
# MAGIC **Transportadoras**
# MAGIC - `LENGTH(cnpj) != 14` OU CNPJ com dígitos verificadores inválidos
# MAGIC - `TRIM(nome) = ''`
# MAGIC - `email NOT LIKE '%@%'`
# MAGIC - `estado NOT IN ('AC','AL',...,'TO')`
# MAGIC - `data_cadastro > current_date()`
# MAGIC
# MAGIC **Motoristas**
# MAGIC - `LENGTH(cpf) != 11`
# MAGIC - `transportadora_id NOT IN (SELECT id FROM transportadoras)`
# MAGIC - `categoria_cnh NOT IN ('C','D','E')`
# MAGIC - `email NOT LIKE '%@%'`
# MAGIC
# MAGIC **Embarcadores**
# MAGIC - `LENGTH(cnpj) != 14`
# MAGIC - `TRIM(nome) = ''`
# MAGIC - `estado NOT IN (lista UFs)`
# MAGIC - `email NOT LIKE '%@%'`
# MAGIC
# MAGIC **Cargas**
# MAGIC - `embarcador_id NOT IN (SELECT id FROM embarcadores)`
# MAGIC - `status = 'realizada' AND data_entrega IS NULL`
# MAGIC - `data_prevista_entrega < data_prevista_coleta`
# MAGIC - `tipo_carga NOT IN (lista TIPOS_CARGA)` (idem composicao_veiculo, caracteristica_veiculo, embalagem)
# MAGIC - `peso_kg <= 0 OR valor_frete < 0`
# MAGIC - `origem_cidade = destino_cidade AND origem_estado = destino_estado`

# COMMAND ----------

print("Concluído. Execute regras de Data Quality nas tabelas para detectar os registros inseridos.")
