# Regras de Data Quality – Motz Demo

Regras alinhadas aos **problemas inseridos** pelo notebook `02_incluir_dados_qualidade_ruim.py`. Use em pipelines (Delta Live Tables expectations, Great Expectations) ou em queries de monitoramento.

---

## Transportadoras

| # | Regra | Condição SQL | Problema inserido (02) |
|---|------|--------------|-------------------------|
| 1 | CNPJ tamanho inválido | `LENGTH(TRIM(cnpj)) != 14` | CNPJ com 5, 13 ou 15 dígitos |
| 2 | Nome vazio | `TRIM(COALESCE(nome, '')) = ''` | Nome "" ou só espaços |
| 3 | Email inválido | `email NOT LIKE '%@%' OR email IS NULL` | Email sem @ |
| 4 | UF inexistente | `estado NOT IN ('AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO')` | UF "XX" |
| 5 | Data cadastro no futuro | `data_cadastro > current_date()` | data_cadastro = amanhã |

---

## Motoristas

| # | Regra | Condição SQL | Problema inserido (02) |
|---|------|--------------|-------------------------|
| 1 | CPF tamanho inválido | `LENGTH(TRIM(cpf)) != 11` | CPF com 10 dígitos |
| 2 | FK transportadora órfã | `transportadora_id NOT IN (SELECT id FROM transportadoras)` | UUID fake 00000000-... |
| 3 | Categoria CNH inválida | `categoria_cnh NOT IN ('C','D','E')` | Categoria "A" ou "B" |
| 4 | Email inválido | `email NOT LIKE '%@%' OR email IS NULL` | Email sem @ |

---

## Embarcadores

| # | Regra | Condição SQL | Problema inserido (02) |
|---|------|--------------|-------------------------|
| 1 | CNPJ tamanho inválido | `LENGTH(TRIM(cnpj)) != 14` | CNPJ curto |
| 2 | Nome vazio | `TRIM(COALESCE(nome, '')) = ''` | Nome "" |
| 3 | UF inexistente | `estado NOT IN (lista UFs)` | UF "YY" |
| 4 | Email inválido | `email NOT LIKE '%@%' OR email IS NULL` | "sem-arroba" |

---

## Cargas

| # | Regra | Condição SQL | Problema inserido (02) |
|---|------|--------------|-------------------------|
| 1 | FK embarcador órfã | `embarcador_id NOT IN (SELECT id FROM embarcadores)` | UUID fake |
| 2 | Realizada sem data de entrega | `status = 'realizada' AND data_entrega IS NULL` | Carga realizada sem data_entrega |
| 3 | Data entrega antes da coleta | `data_prevista_entrega < data_prevista_coleta` ou `data_entrega < data_prevista_coleta` | Datas invertidas |
| 4 | Tipo de carga fora do domínio | `tipo_carga NOT IN (lista TIPOS_CARGA)` | "Cimentoo" (typo) |
| 5 | Composição veículo fora do domínio | `composicao_veiculo NOT IN (lista COMPOSICOES)` | "Carreta Falsa" |
| 6 | Embalagem fora do domínio | `embalagem NOT IN (lista EMBALAGENS)` | "Granell" (typo) |
| 7 | Peso inválido | `peso_kg IS NULL OR peso_kg <= 0` | peso_kg negativo |
| 8 | Valor frete inválido | `valor_frete IS NULL OR valor_frete < 0` | valor_frete negativo |
| 9 | Origem = destino | `origem_estado = destino_estado AND origem_cidade = destino_cidade` | Mesma cidade/UF |

Listas de domínio (motivo do notebook 01):  
- **TIPOS_CARGA:** Adubo, Argamassa, Arroz, Brita, Calcário, Cimento, Cinza, Clínquer, Concreto, Coque, Escória, Farelo de Algodão, Fertilizantes, Fubá, Gesso, Granilha, Milho, Minério, Óleos Vegetais, Pedrisco, Pó Cerâmico, Rejunte, Sacaria, Soja  
- **COMPOSICOES_VEICULO:** Bitrem, Caminhão 3/4, Caminhão Bitruck, Caminhão Toco, Caminhão Truck, Carreta, Rodotrem  
- **EMBALAGENS_CARGA:** Balde, Big Bag, Bombona, Caixa, Container, Default, Fardo, Granel, Moinha de Carvão, Pallet, Pallet / Granel, Rolo, Sacas, Saco, Tanque, Unidade, Viga VPL, Volumes  

---

## Uso

- **Dashboard:** o dataset `data_quality` (e opcionalmente `data_quality_regras`) aplica essas condições e exibe totais por tabela e por regra.
- **Notebook 04:** a seção Data Quality usa as mesmas regras em SQL.
- **Pipeline:** use as condições em `EXPECT` (DLT) ou em checks do Great Expectations.
