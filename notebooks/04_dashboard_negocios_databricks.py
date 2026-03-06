# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard de Negócios – Motz Demo
# MAGIC
# MAGIC Este notebook consulta as tabelas Delta e exibe visões para **gerenciamento de negócio**.
# MAGIC
# MAGIC ### Resumo: melhor visualização por painel
# MAGIC | # | Painel | Visualização | Eixos / Uso |
# MAGIC |---|--------|---------------|-------------|
# MAGIC | 1 | KPIs (visão geral) | **Table** | Uma linha com todos os números |
# MAGIC | 2 | Cargas por status | **Pie chart** | Keys: status_carga, Values: quantidade |
# MAGIC | 3 | Top tipos de carga | **Bar chart** (horizontal) | X: quantidade, Y: tipo_carga |
# MAGIC | 4 | Cargas por composição de veículo | **Bar chart** (horizontal) | X: quantidade, Y: composicao_veiculo |
# MAGIC | 5 | Cargas por UF origem | **Bar chart** (vertical) | X: estado, Y: quantidade |
# MAGIC | 6 | Cargas por UF destino | **Bar chart** (vertical) | X: estado, Y: quantidade |
# MAGIC | 7 | Cargas realizadas por mês | **Line chart** | X: ano_mes, Y: realizadas |
# MAGIC | 8 | Ticket médio por tipo de carga | **Table** ou **Bar chart** | Tabela completa ou Y: valor_total_R |
# MAGIC | 9 | Data Quality | **Table** ou **Bar chart** | Tabela × registros_com_problema |
# MAGIC
# MAGIC **Para criar o Dashboard no Databricks:**
# MAGIC 1. Execute todas as células (Run all).
# MAGIC 2. Em cada resultado, clique no ícone **📊** e escolha o tipo conforme a tabela acima.
# MAGIC 3. **View → Add to dashboard** e arraste os painéis no layout.
# MAGIC 4. Ajuste refresh em Dashboard settings.

# COMMAND ----------

# Configuração (mesmo catalog/schema do projeto)
CATALOG = "leticia_santos_classic_stable_catalog"
SCHEMA = "motz_demo"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Visão geral – KPIs
# MAGIC **Visualização:** **Table** (tabela)
# MAGIC - Mostra uma única linha com todos os indicadores numéricos.
# MAGIC - Alternativa: use **Counter** para cada coluna se o Databricks permitir múltiplos counters; senão, Table é o mais claro.

# COMMAND ----------

spark.sql(f"""
  SELECT
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.transportadoras) AS transportadoras,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.motoristas) AS motoristas,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.embarcadores) AS embarcadores,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.cargas) AS total_cargas,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.cargas WHERE status = 'realizada') AS cargas_realizadas,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.cargas WHERE status = 'disponivel') AS cargas_disponiveis,
    (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.cargas WHERE status = 'futura') AS cargas_futuras,
    (SELECT ROUND(COALESCE(SUM(valor_frete), 0), 2) FROM {CATALOG}.{SCHEMA}.cargas WHERE status = 'realizada') AS valor_total_fretes_realizados_R,
    (SELECT ROUND(COALESCE(SUM(peso_kg), 0), 0) FROM {CATALOG}.{SCHEMA}.cargas WHERE status = 'realizada') AS peso_total_kg_realizadas
""").createOrReplaceTempView("kpis_resumo")
display(spark.table("kpis_resumo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cargas por status
# MAGIC **Visualização:** **Pie chart** (gráfico de pizza)
# MAGIC - **Keys:** `status_carga` | **Values:** `quantidade`
# MAGIC - Proporção entre realizadas / disponíveis / futuras fica evidente de forma imediata.

# COMMAND ----------

display(
  spark.sql(f"""
    SELECT status AS status_carga, COUNT(*) AS quantidade
    FROM {CATALOG}.{SCHEMA}.cargas
    GROUP BY status
    ORDER BY quantidade DESC
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Top 10 tipos de carga (quantidade)
# MAGIC **Visualização:** **Bar chart** (barras horizontais)
# MAGIC - **X:** `quantidade` | **Y:** `tipo_carga` (barras horizontais para ler nomes longos)
# MAGIC - Ou **X:** `tipo_carga`, **Y:** `quantidade` se preferir barras verticais.

# COMMAND ----------

display(
  spark.sql(f"""
    SELECT tipo_carga, COUNT(*) AS quantidade, ROUND(SUM(valor_frete), 2) AS valor_total
    FROM {CATALOG}.{SCHEMA}.cargas
    GROUP BY tipo_carga
    ORDER BY quantidade DESC
    LIMIT 10
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cargas por composição de veículo
# MAGIC **Visualização:** **Bar chart** (barras horizontais)
# MAGIC - **X:** `quantidade` | **Y:** `composicao_veiculo`
# MAGIC - Facilita comparar qual composição (Carreta, Truck, Bitrem etc.) é mais usada.

# COMMAND ----------

display(
  spark.sql(f"""
    SELECT composicao_veiculo, COUNT(*) AS quantidade
    FROM {CATALOG}.{SCHEMA}.cargas
    GROUP BY composicao_veiculo
    ORDER BY quantidade DESC
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cargas por UF de origem
# MAGIC **Visualização:** **Bar chart** (barras verticais)
# MAGIC - **X:** `estado` | **Y:** `quantidade`
# MAGIC - UFs são curtas; barras verticais mostram bem os estados com mais cargas.

# COMMAND ----------

display(
  spark.sql(f"""
    SELECT COALESCE(origem_estado, 'N/A') AS estado, COUNT(*) AS quantidade
    FROM {CATALOG}.{SCHEMA}.cargas
    GROUP BY origem_estado
    ORDER BY quantidade DESC
    LIMIT 10
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cargas por UF de destino
# MAGIC **Visualização:** **Bar chart** (barras verticais)
# MAGIC - **X:** `estado` | **Y:** `quantidade`
# MAGIC - Compare lado a lado com o painel de origem para ver fluxos.

# COMMAND ----------

display(
  spark.sql(f"""
    SELECT COALESCE(destino_estado, 'N/A') AS estado, COUNT(*) AS quantidade
    FROM {CATALOG}.{SCHEMA}.cargas
    GROUP BY destino_estado
    ORDER BY quantidade DESC
    LIMIT 10
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cargas realizadas por mês (tendência)
# MAGIC **Visualização:** **Line chart** (gráfico de linhas)
# MAGIC - **X:** `ano_mes` | **Y:** `realizadas` (e opcionalmente `valor_total` em eixo secundário)
# MAGIC - Ideal para evolução no tempo; linha mostra tendência de crescimento ou queda.

# COMMAND ----------

display(
  spark.sql(f"""
    SELECT
      date_format(data_entrega, 'yyyy-MM') AS ano_mes,
      COUNT(*) AS realizadas,
      ROUND(SUM(valor_frete), 2) AS valor_total
    FROM {CATALOG}.{SCHEMA}.cargas
    WHERE status = 'realizada' AND data_entrega IS NOT NULL
    GROUP BY date_format(data_entrega, 'yyyy-MM')
    ORDER BY ano_mes
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Ticket médio e valor total por tipo de carga (realizadas)
# MAGIC **Visualização:** **Table** (tabela) ou **Bar chart** (barras)
# MAGIC - **Table:** ver todos os números (quantidade, ticket_medio_R, valor_total_R) por tipo.
# MAGIC - **Bar chart:** **X:** `tipo_carga` | **Y:** `valor_total_R` para destacar os tipos que mais faturam.

# COMMAND ----------

display(
  spark.sql(f"""
    SELECT
      tipo_carga,
      COUNT(*) AS quantidade,
      ROUND(AVG(valor_frete), 2) AS ticket_medio_R,
      ROUND(SUM(valor_frete), 2) AS valor_total_R
    FROM {CATALOG}.{SCHEMA}.cargas
    WHERE status = 'realizada'
    GROUP BY tipo_carga
    ORDER BY valor_total_R DESC
    LIMIT 10
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Data Quality – registros com possível problema
# MAGIC **Regras:** documentadas em `docs/data_quality_rules.md` (alinhadas ao notebook `02_incluir_dados_qualidade_ruim.py`).
# MAGIC **Visualização:** **Table** (resumo por tabela) e **Table** (detalhe por regra).
# MAGIC - Resumo: tabela × quantidade de registros com **qualquer** problema.
# MAGIC - Por regra: tabela × regra × quantidade (para ver qual regra falha mais).

# COMMAND ----------

display(
  spark.sql(f"""
    SELECT 'Transportadoras' AS tabela, COUNT(*) AS registros_com_problema FROM {CATALOG}.{SCHEMA}.transportadoras
    WHERE LENGTH(TRIM(cnpj)) != 14 OR TRIM(COALESCE(nome, '')) = '' OR email NOT LIKE '%@%%' OR email IS NULL OR estado NOT IN ('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO') OR data_cadastro > current_date()
    UNION ALL SELECT 'Motoristas', COUNT(*) FROM {CATALOG}.{SCHEMA}.motoristas m
    WHERE LENGTH(TRIM(m.cpf)) != 11 OR NOT EXISTS (SELECT 1 FROM {CATALOG}.{SCHEMA}.transportadoras t WHERE t.id = m.transportadora_id) OR m.categoria_cnh NOT IN ('C','D','E') OR m.categoria_cnh IS NULL OR m.email NOT LIKE '%@%%' OR m.email IS NULL
    UNION ALL SELECT 'Embarcadores', COUNT(*) FROM {CATALOG}.{SCHEMA}.embarcadores
    WHERE LENGTH(TRIM(cnpj)) != 14 OR TRIM(COALESCE(nome, '')) = '' OR email NOT LIKE '%@%%' OR email IS NULL OR estado NOT IN ('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')
    UNION ALL SELECT 'Cargas', COUNT(*) FROM {CATALOG}.{SCHEMA}.cargas c
    WHERE NOT EXISTS (SELECT 1 FROM {CATALOG}.{SCHEMA}.embarcadores e WHERE e.id = c.embarcador_id) OR (status = 'realizada' AND data_entrega IS NULL) OR data_prevista_entrega < data_prevista_coleta OR peso_kg IS NULL OR peso_kg <= 0 OR valor_frete IS NULL OR valor_frete < 0 OR (origem_estado = destino_estado AND origem_cidade = destino_cidade)
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 Data Quality por regra (detalhe)
# MAGIC Uma linha por regra; mesma lógica do dataset `data_quality_regras` do dashboard.

# COMMAND ----------

display(
  spark.sql(f"""
    SELECT 'Transportadoras' AS tabela, 'CNPJ inválido' AS regra, COUNT(*) AS registros_com_problema FROM {CATALOG}.{SCHEMA}.transportadoras WHERE LENGTH(TRIM(cnpj)) != 14
    UNION ALL SELECT 'Transportadoras', 'Nome vazio', COUNT(*) FROM {CATALOG}.{SCHEMA}.transportadoras WHERE TRIM(COALESCE(nome, '')) = ''
    UNION ALL SELECT 'Transportadoras', 'Email inválido', COUNT(*) FROM {CATALOG}.{SCHEMA}.transportadoras WHERE email NOT LIKE '%@%%' OR email IS NULL
    UNION ALL SELECT 'Transportadoras', 'UF inexistente', COUNT(*) FROM {CATALOG}.{SCHEMA}.transportadoras WHERE estado NOT IN ('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')
    UNION ALL SELECT 'Transportadoras', 'Data cadastro no futuro', COUNT(*) FROM {CATALOG}.{SCHEMA}.transportadoras WHERE data_cadastro > current_date()
    UNION ALL SELECT 'Motoristas', 'CPF inválido', COUNT(*) FROM {CATALOG}.{SCHEMA}.motoristas WHERE LENGTH(TRIM(cpf)) != 11
    UNION ALL SELECT 'Motoristas', 'FK transportadora órfã', COUNT(*) FROM {CATALOG}.{SCHEMA}.motoristas m WHERE NOT EXISTS (SELECT 1 FROM {CATALOG}.{SCHEMA}.transportadoras t WHERE t.id = m.transportadora_id)
    UNION ALL SELECT 'Motoristas', 'Categoria CNH inválida', COUNT(*) FROM {CATALOG}.{SCHEMA}.motoristas WHERE categoria_cnh NOT IN ('C','D','E') OR categoria_cnh IS NULL
    UNION ALL SELECT 'Motoristas', 'Email inválido', COUNT(*) FROM {CATALOG}.{SCHEMA}.motoristas WHERE email NOT LIKE '%@%%' OR email IS NULL
    UNION ALL SELECT 'Embarcadores', 'CNPJ inválido', COUNT(*) FROM {CATALOG}.{SCHEMA}.embarcadores WHERE LENGTH(TRIM(cnpj)) != 14
    UNION ALL SELECT 'Embarcadores', 'Nome vazio', COUNT(*) FROM {CATALOG}.{SCHEMA}.embarcadores WHERE TRIM(COALESCE(nome, '')) = ''
    UNION ALL SELECT 'Embarcadores', 'UF inexistente', COUNT(*) FROM {CATALOG}.{SCHEMA}.embarcadores WHERE estado NOT IN ('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')
    UNION ALL SELECT 'Embarcadores', 'Email inválido', COUNT(*) FROM {CATALOG}.{SCHEMA}.embarcadores WHERE email NOT LIKE '%@%%' OR email IS NULL
    UNION ALL SELECT 'Cargas', 'FK embarcador órfã', COUNT(*) FROM {CATALOG}.{SCHEMA}.cargas c WHERE NOT EXISTS (SELECT 1 FROM {CATALOG}.{SCHEMA}.embarcadores e WHERE e.id = c.embarcador_id)
    UNION ALL SELECT 'Cargas', 'Realizada sem data entrega', COUNT(*) FROM {CATALOG}.{SCHEMA}.cargas WHERE status = 'realizada' AND data_entrega IS NULL
    UNION ALL SELECT 'Cargas', 'Data entrega antes da coleta', COUNT(*) FROM {CATALOG}.{SCHEMA}.cargas WHERE data_prevista_entrega < data_prevista_coleta
    UNION ALL SELECT 'Cargas', 'Peso inválido', COUNT(*) FROM {CATALOG}.{SCHEMA}.cargas WHERE peso_kg IS NULL OR peso_kg <= 0
    UNION ALL SELECT 'Cargas', 'Valor frete inválido', COUNT(*) FROM {CATALOG}.{SCHEMA}.cargas WHERE valor_frete IS NULL OR valor_frete < 0
    UNION ALL SELECT 'Cargas', 'Origem = destino', COUNT(*) FROM {CATALOG}.{SCHEMA}.cargas WHERE origem_estado = destino_estado AND origem_cidade = destino_cidade
  """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Próximo passo:** adicione as visualizações acima a um **Dashboard** (View → Add to dashboard) e configure o refresh desejado.
