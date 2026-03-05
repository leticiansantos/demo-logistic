# Databricks notebook source
# MAGIC %md
# MAGIC # Exportar métricas para o dashboard
# MAGIC
# MAGIC Agrega os dados das tabelas Delta e gera o JSON no formato esperado pelo app (`app/data/dashboard_metrics.json`).
# MAGIC Copie a saída da célula final para o arquivo no repositório ou use um job para escrever em volume/DBFS.

# COMMAND ----------

CATALOG = "leticia_santos_classic_stable_catalog"
SCHEMA = "motz_demo"

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

def table(name):
    return spark.table(f"{CATALOG}.{SCHEMA}.{name}")

# COMMAND ----------

# Resumo: totais
transportadoras = table("transportadoras")
motoristas = table("motoristas")
embarcadores = table("embarcadores")
cargas = table("cargas")

total_transportadoras = transportadoras.count()
total_motoristas = motoristas.count()
total_embarcadores = embarcadores.count()
total_cargas = cargas.count()

cargas_realizadas = cargas.filter(F.col("status") == "realizada").count()
cargas_disponiveis = cargas.filter(F.col("status") == "disponivel").count()
cargas_futuras = cargas.filter(F.col("status") == "futura").count()

agg_realizadas = cargas.filter(F.col("status") == "realizada").agg(
    F.sum("valor_frete").alias("valor_total"),
    F.sum("peso_kg").alias("peso_total"),
    F.count("*").alias("n"),
).collect()[0]

valor_total_fretes_realizados = float(agg_realizadas["valor_total"] or 0)
peso_total_kg_realizadas = float(agg_realizadas["peso_total"] or 0)
n_realizadas = int(agg_realizadas["n"] or 0)
valor_medio_frete_realizado = valor_total_fretes_realizados / n_realizadas if n_realizadas else 0

agg_disp = cargas.filter(F.col("status") == "disponivel").agg(F.sum("valor_frete").alias("v")).collect()[0]
valor_total_fretes_disponiveis = float(agg_disp["v"] or 0)

peso_medio_geral = cargas.agg(F.avg("peso_kg")).collect()[0][0]
peso_medio_kg_por_carga = float(peso_medio_geral) if peso_medio_geral else 0

# COMMAND ----------

# Cargas por tipo (top 10)
cargas_por_tipo = (
    cargas.groupBy("tipo_carga")
    .agg(
        F.count("*").alias("quantidade"),
        F.sum("valor_frete").alias("valor_total"),
    )
    .orderBy(F.desc("quantidade"))
    .limit(10)
)
cargas_por_tipo_list = [
    {"tipo_carga": r["tipo_carga"], "quantidade": int(r["quantidade"]), "valor_total": float(r["valor_total"] or 0)}
    for r in cargas_por_tipo.collect()
]

# COMMAND ----------

# Cargas por UF origem e destino
cargas_por_uf_origem = (
    cargas.groupBy("origem_estado")
    .agg(F.count("*").alias("quantidade"))
    .orderBy(F.desc("quantidade"))
    .limit(10)
)
cargas_por_uf_origem_list = [{"estado": r["origem_estado"] or "N/A", "quantidade": int(r["quantidade"])} for r in cargas_por_uf_origem.collect()]

cargas_por_uf_destino = (
    cargas.groupBy("destino_estado")
    .agg(F.count("*").alias("quantidade"))
    .orderBy(F.desc("quantidade"))
    .limit(10)
)
cargas_por_uf_destino_list = [{"estado": r["destino_estado"] or "N/A", "quantidade": int(r["quantidade"])} for r in cargas_por_uf_destino.collect()]

# COMMAND ----------

# Cargas por composição de veículo
cargas_por_composicao = (
    cargas.groupBy("composicao_veiculo")
    .agg(F.count("*").alias("quantidade"))
    .orderBy(F.desc("quantidade"))
)
cargas_por_composicao_list = [{"composicao_veiculo": r["composicao_veiculo"], "quantidade": int(r["quantidade"])} for r in cargas_por_composicao.collect()]

# COMMAND ----------

# Cargas realizadas por mês (tendência)
cargas_realizadas_df = cargas.filter(F.col("status") == "realizada").withColumn(
    "ano_mes", F.date_format(F.col("data_entrega"), "yyyy-MM")
)
cargas_por_mes = (
    cargas_realizadas_df.groupBy("ano_mes")
    .agg(
        F.count("*").alias("realizadas"),
        F.sum("valor_frete").alias("valor_total"),
    )
    .orderBy("ano_mes")
)
cargas_por_mes_list = [
    {"ano_mes": r["ano_mes"], "realizadas": int(r["realizadas"]), "valor_total": float(r["valor_total"] or 0)}
    for r in cargas_por_mes.collect()
]

# COMMAND ----------

# Data quality: contagem de registros com problemas (heurísticas simples)
# Ajuste conforme suas regras reais (ex.: DLT expectations)
UFS_VALIDAS = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]

transp_ruim = transportadoras.filter(
    (F.length(F.col("cnpj")) != 14) | (F.trim(F.col("nome")) == "") | (~F.col("estado").isin(UFS_VALIDAS))
).count()
mot_ruim = motoristas.filter(F.length(F.col("cpf")) != 11).count()
emb_ruim = embarcadores.filter(
    (F.length(F.col("cnpj")) != 14) | (F.trim(F.col("nome")) == "") | (~F.col("estado").isin(UFS_VALIDAS))
).count()
cargas_ruim = cargas.filter(
    (F.col("peso_kg").isNull()) | (F.col("peso_kg") <= 0) | (F.col("valor_frete").isNull()) | (F.col("valor_frete") < 0)
).count()

# COMMAND ----------

payload = {
    "resumo": {
        "total_transportadoras": total_transportadoras,
        "total_motoristas": total_motoristas,
        "total_embarcadores": total_embarcadores,
        "total_cargas": total_cargas,
        "cargas_realizadas": cargas_realizadas,
        "cargas_disponiveis": cargas_disponiveis,
        "cargas_futuras": cargas_futuras,
        "valor_total_fretes_realizados": round(valor_total_fretes_realizados, 2),
        "valor_total_fretes_disponiveis": round(valor_total_fretes_disponiveis, 2),
        "peso_total_kg_realizadas": round(peso_total_kg_realizadas, 0),
        "peso_medio_kg_por_carga": round(peso_medio_kg_por_carga, 0),
        "valor_medio_frete_realizado": round(valor_medio_frete_realizado, 2),
    },
    "cargas_por_tipo": cargas_por_tipo_list,
    "cargas_por_uf_origem": cargas_por_uf_origem_list,
    "cargas_por_uf_destino": cargas_por_uf_destino_list,
    "cargas_por_composicao": cargas_por_composicao_list,
    "cargas_por_mes": cargas_por_mes_list,
    "data_quality": {
        "transportadoras_com_problema": transp_ruim,
        "motoristas_com_problema": mot_ruim,
        "embarcadores_com_problema": emb_ruim,
        "cargas_com_problema": cargas_ruim,
        "total_registros_com_problema": transp_ruim + mot_ruim + emb_ruim + cargas_ruim,
    },
}

# COMMAND ----------

# Saída JSON: copie e cole em app/data/dashboard_metrics.json
print(json.dumps(payload, indent=2, ensure_ascii=False))

# COMMAND ----------

# Opcional: salvar em arquivo no driver (Databricks) para depois baixar
# dbutils.fs.put("file:/tmp/dashboard_metrics.json", json.dumps(payload, indent=2, ensure_ascii=False))
# displayHTML("<a href='/files/tmp/dashboard_metrics.json' download>Baixar dashboard_metrics.json</a>")
