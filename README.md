# Motz Demo – Logística de Caminhões

Projeto tipo "Uber de caminhões": conecta **embarcadores** (quem precisa enviar carga) e **transportadores** (motoristas/transportadoras).

## Stack

- **Databricks** – processamento e armazenamento
- **Delta Lake** – tabelas
- **Dados sintéticos** – Faker (por enquanto)

## Tabelas

| Tabela            | Descrição                                                                 |
|-------------------|----------------------------------------------------------------------------|
| `transportadoras` | Cadastro de transportadoras (empresas)                                    |
| `motoristas`      | Cadastro de motoristas (vinculados a transportadora)                     |
| `embarcadores`    | Cadastro de embarcadores (contratantes de frete)                          |
| `cargas`          | Cargas realizadas (passado), disponíveis (presente) e futuras; ligam embarcador, transportadora e motorista; usam tipo de carga, composição/característica de veículo e embalagem |

## Estrutura do repositório

```
motz-demo/
├── README.md
├── requirements.txt
├── src/
│   ├── schemas.py                 # Schemas das tabelas
│   ├── synthetic_data.py          # Geração de dados sintéticos
│   ├── tipos_carga.py             # Tipos de carga (Motz)
│   ├── caracteristicas_veiculo.py # Composições e carrocerias (App Motz)
│   └── embalagens_carga.py        # Tipos de embalagem (App Motz)
├── app/
│   ├── dashboard.py          # Dashboard de negócios (Streamlit)
│   ├── data/
│   │   └── dashboard_metrics.json  # Métricas para o dashboard (atualizar via notebook 03)
│   ├── requirements.txt
│   └── run.sh
├── notebooks/
│   ├── 01_criar_tabelas_sinteticas.py
│   ├── 02_incluir_dados_qualidade_ruim.py
│   ├── 03_exportar_metricas_dashboard.py
│   └── 04_dashboard_negocios_databricks.py  # Dashboard de negócios no Databricks
└── databricks.yml
```

## Workspace Databricks

- **Workspace ID:** `7474658265676932`
- **URL:** [https://fevm-leticia-santos-classic-stable.cloud.databricks.com/](https://fevm-leticia-santos-classic-stable.cloud.databricks.com/)
- **Catalog / Schema:** `leticia_santos_classic_stable_catalog.motz_demo` (tabelas: `transportadoras`, `motoristas`, `embarcadores`, `cargas`)

## Uso no Databricks

1. Crie um cluster com runtime **14.3 LTS** ou superior.
2. Instale no cluster (ou use init script): `faker` (PySpark e Delta já vêm no runtime).
3. Importe o notebook `notebooks/01_criar_tabelas_sinteticas.py` ou copie o conteúdo para um novo notebook.
4. Ajuste o **catalog** e **schema** (ex.: `main.default`) nas variáveis no topo do notebook.
5. Rode todas as células; as tabelas Delta serão criadas/atualizadas com dados sintéticos.

## Uso local (opcional)

```bash
pip install -r requirements.txt
# Requer Spark instalado; para testes de geração use apenas src/synthetic_data.py
python -c "from src.synthetic_data import *; print('OK')"
```

## Dashboard de negócios

O app em `app/` é um dashboard gerencial que consome o arquivo `app/data/dashboard_metrics.json`.

**Rodar o dashboard (local):**
```bash
cd app && pip install -r requirements.txt && streamlit run dashboard.py
```
Ou: `./app/run.sh` (a partir da raiz do repo).

**Atualizar dados a partir do Databricks:** execute o notebook `notebooks/03_exportar_metricas_dashboard.py`, copie o JSON impresso na última célula e substitua o conteúdo de `app/data/dashboard_metrics.json`.

**Dashboard direto no Databricks:** use o notebook `notebooks/04_dashboard_negocios_databricks.py`. Execute todas as células e, em cada resultado, escolha o tipo de gráfico (Pizza, Barras, Linhas). Depois: **View → Add to dashboard** para criar um Dashboard nativo do Databricks com os painéis (KPIs, cargas por status, por tipo, por UF, por composição, tendência mensal, data quality).

## Próximos passos (sugestão)

- Tabela de **veículos** (placa, modelo, composição, característica)
- Tabelas de **ofertas** / **lances** (matches embarcador × transportador)
