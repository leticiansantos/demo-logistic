# Storytelling: Genie com os dados do Motz Demo

Uso do **Databricks Genie** para perguntas em linguagem natural sobre os dados do demo logístico — perguntas que **não** são respondidas diretamente pelo dashboard de negócios.

---

## Contexto

Você já consultou o **Dashboard de Negócios**, que mostra totais, cargas por status, tipo, composição, UF origem/destino, realizadas por mês, ticket médio e Data Quality. Agora quer ir além: fazer perguntas ad hoc, cruzar dimensões e explorar sem criar novos painéis.

O **Genie** acessa as mesmas tabelas do catalog `leticia_santos_classic_stable_catalog`, schema `motz_demo` (transportadoras, motoristas, embarcadores, cargas) e traduz perguntas em linguagem natural em SQL.

---

## Roteiro sugerido (perguntas que não estão no dashboard)

### 1. Quem mais embarca?

- **Pergunta:** *“Quais os 5 embarcadores com mais cargas? Mostra a quantidade e o valor total de frete.”*  
- **Por quê:** O dashboard não lista por embarcador; o Genie cruza `cargas` × `embarcadores` e agrega.

### 2. Quem mais transporta?

- **Pergunta:** *“Quantas cargas cada transportadora tem? Ordena pelas que mais têm.”*  
- **Por quê:** O dashboard mostra totais gerais, não a distribuição por transportadora; o Genie usa `cargas` × `transportadoras`.

### 3. Valor por transportadora

- **Pergunta:** *“Qual o valor total de frete realizado por transportadora?”*  
- **Por quê:** Não há no dashboard visão de faturamento por transportadora; o Genie agrega `valor_frete` por transportadora nas cargas realizadas.

### 4. Relação peso × valor

- **Pergunta:** *“Qual a média de valor do frete por tonelada (peso_kg/1000) nas cargas realizadas?”*  
- **Por quê:** O dashboard não mostra esse indicador; o Genie calcula a partir de `valor_frete` e `peso_kg`.

### 5. Principais rotas (par origem–destino)

- **Pergunta:** *“Quais os 10 pares de UF origem e destino com mais cargas?”*  
- **Por quê:** O dashboard mostra origem e destino separados; o Genie agrupa por (origem_estado, destino_estado) para ver rotas.

### 6. Cargas futuras e disponíveis por tipo

- **Pergunta:** *“Quantas cargas estão disponíveis e quantas futuras por tipo de carga?”*  
- **Por quê:** O dashboard mostra status no geral; o Genie detalha por `tipo_carga` para disponível e futura.

### 7. Motoristas por transportadora

- **Pergunta:** *“Quantos motoristas cada transportadora tem?”*  
- **Por quê:** O dashboard não mostra essa relação; o Genie agrupa `motoristas` por `transportadora_id`.

### 8. Ticket médio por embarcador

- **Pergunta:** *“Qual o ticket médio de frete (cargas realizadas) por embarcador?”*  
- **Por quê:** O dashboard tem ticket médio por tipo de carga; o Genie faz por embarcador.

### 9. Concentração em um mês

- **Pergunta:** *“Em qual mês tivemos o maior valor total de fretes realizados?”*  
- **Por quê:** O dashboard mostra realizadas por mês; o Genie pode responder “qual mês foi o pico” em uma frase.

### 10. Tipos de carga com valor acima da média

- **Pergunta:** *“Quais tipos de carga têm ticket médio acima da média geral das realizadas?”*  
- **Por quê:** O dashboard não faz essa comparação; o Genie usa média global e filtra por tipo.

---

## Fluxo sugerido na demonstração

1. **Abrir o Genie** no workspace (Genie Space apontando para o catalog/schema do demo).  
2. **Mostrar o dashboard** rapidamente: “Aqui vemos os KPIs e visões padrão.”  
3. **Dizer:** “Agora vou fazer perguntas que esse painel não responde, em linguagem natural.”  
4. **Fazer 2–3 perguntas** do roteiro (ex.: top embarcadores, valor por transportadora, principais rotas).  
5. **Mostrar a SQL gerada** (quando o Genie exibir) para reforçar que é sobre as mesmas tabelas.  
6. **Fechar:** “Com Genie, qualquer pessoa pode explorar os dados sem precisar de novo dashboard ou SQL na mão.”

---

## Dica para o ambiente

Configure o **Genie Space** com as tabelas do schema `leticia_santos_classic_stable_catalog.motz_demo` (ou o schema equivalente do seu workspace) para que o Genie use o mesmo contexto do dashboard e dos notebooks do demo.
