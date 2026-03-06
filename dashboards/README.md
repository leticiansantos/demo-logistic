# Dashboard Motz Demo (AI/BI)

Definição do **Dashboard de Negócios – Motz Demo** para Databricks AI/BI (Lakeview).

## Conteúdo

- **`motz_demo_dashboard.json`** – definição do dashboard (datasets + páginas e widgets).

## Dados

O dashboard usa as tabelas do catalog `leticia_santos_classic_stable_catalog`, schema `motz_demo`:

- `transportadoras`, `motoristas`, `embarcadores`, `cargas`

Certifique-se de ter executado os notebooks `01_criar_tabelas_sinteticas.py` e, se quiser dados com problemas de qualidade, o `02_incluir_dados_qualidade_ruim.py`.

## Deploy

### Opção 1: Script Python (recomendado)

Use um **ambiente virtual** para não conflitar com o Python gerenciado pelo sistema (Homebrew):

```bash
# Uma vez: criar o venv e instalar o SDK
python3 -m venv .venv
source .venv/bin/activate   # no Windows: .venv\Scripts\activate
pip install databricks-sdk

# Rodar o deploy (com autenticação Databricks configurada)
python scripts/deploy_dashboard.py
```

Autenticação: `databricks configure` ou variáveis `DATABRICKS_HOST` e `DATABRICKS_TOKEN`.  
Workspace do projeto: ver `docs/databricks-workspace.md`.

Parâmetros opcionais:

- `--parent-path "/Workspace/Users/<seu_usuario>/Dashboards"` – pasta de destino.
- `--warehouse-id <id>` – ID do SQL warehouse (senão o script tenta escolher um).
- `--no-publish` – só cria o rascunho, não publica.

### Opção 2: Cursor + MCP Databricks

Com o MCP do projeto conectado e autorizado ao workspace:

1. Obter um **warehouse_id** (por exemplo com a ferramenta `get_best_warehouse`).
2. Chamar **create_or_update_dashboard** com:
   - `display_name`: `"Dashboard de Negócios – Motz Demo"`
   - `parent_path`: ex. `"/Workspace/Dashboards"`
   - `serialized_dashboard`: conteúdo de `dashboards/motz_demo_dashboard.json` (string JSON).
   - `warehouse_id`: o ID obtido acima.

## Widgets do dashboard

| Área        | Widgets |
|------------|---------|
| **Filtros** (1ª página) | **Período (data de entrega)** – filtro global por intervalo de datas; afeta todos os gráficos e tabelas que usam data de entrega. |
| KPIs       | Transportadoras, Motoristas, Embarcadores, Total Cargas, Cargas Realizadas, Valor Total Fretes |
| Status     | Pizza: cargas por status (realizada/disponível/futura) |
| Tipo       | Barras: tipos de carga por quantidade |
| UF         | Barras: cargas por UF origem e por UF destino |
| Tendência  | Linha: cargas realizadas por mês |
| Detalhes   | Tabelas: ticket médio por tipo de carga e Data Quality (registros com problema) |
