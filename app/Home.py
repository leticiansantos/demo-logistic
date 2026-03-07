"""
Dashboard de negócios – Motz Demo
Visão gerencial sobre transportadoras, motoristas, embarcadores e cargas.
Dados carregados de app/data/dashboard_metrics.json (gerado pelo notebook 03 no Databricks).
"""
import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# -----------------------------------------------------------------------------
# Configuração e carregamento de dados
# -----------------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent / "data"
METRICS_FILE = DATA_DIR / "dashboard_metrics.json"


def load_metrics():
    if not METRICS_FILE.exists():
        st.error(f"Arquivo de métricas não encontrado: {METRICS_FILE}. Execute o notebook 03 no Databricks para exportar os dados.")
        return None
    with open(METRICS_FILE, encoding="utf-8") as f:
        return json.load(f)


# -----------------------------------------------------------------------------
# Layout e estilo
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Motz Demo – Dashboard de Negócios",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Cores alinhadas a um tema logística (laranja/âmbar como Motz)
PRIMARY = "#E65100"
SECONDARY = "#FF8F00"
BG_CARD = "#FFF8E1"
FONT_HEADER = "DM Sans, sans-serif"

st.markdown("""
<style>
    .stApp { background: linear-gradient(180deg, #fafafa 0%, #fff 100%); }
    h1 { color: #1a1a2e; font-family: 'DM Sans', sans-serif; margin-bottom: 0.2em; }
    h2 { color: #16213e; font-size: 1.1rem; font-weight: 600; margin-top: 1.5rem; }
    .metric-card {
        background: white;
        border-radius: 12px;
        padding: 1rem 1.25rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        border-left: 4px solid #E65100;
        margin-bottom: 0.5rem;
    }
    .metric-card h3 { color: #666; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; margin: 0 0 0.25rem 0; }
    .metric-card .value { font-size: 1.5rem; font-weight: 700; color: #1a1a2e; }
    .metric-card .sub { font-size: 0.8rem; color: #888; margin-top: 0.15rem; }
    div[data-testid="stMetricValue"] { font-size: 1.4rem !important; font-weight: 700 !important; }
    .stPlotlyChart { border-radius: 12px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
</style>
""", unsafe_allow_html=True)


def main():
    data = load_metrics()
    if not data:
        return

    resumo = data["resumo"]
    dq = data.get("data_quality", {})

    # -------------------------------------------------------------------------
    # Cabeçalho
    # -------------------------------------------------------------------------
    st.title("🚛 Motz Demo – Visão de Negócio")
    st.caption("Dashboard gerencial com base nos dados de transportadoras, motoristas, embarcadores e cargas.")

    # -------------------------------------------------------------------------
    # KPIs – Visão geral
    # -------------------------------------------------------------------------
    st.markdown("### Visão geral")
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Transportadoras", resumo["total_transportadoras"], help="Total de empresas de transporte cadastradas")
    with col2:
        st.metric("Motoristas", resumo["total_motoristas"], help="Motoristas vinculados às transportadoras")
    with col3:
        st.metric("Embarcadores", resumo["total_embarcadores"], help="Contratantes de frete cadastrados")
    with col4:
        st.metric("Cargas (total)", resumo["total_cargas"], help="Realizadas + disponíveis + futuras")
    with col5:
        valor_fmt = f"R$ {resumo['valor_total_fretes_realizados']:,.0f}".replace(",", ".")
        st.metric("Fretes realizados (R$)", valor_fmt, help="Soma do valor dos fretes já realizados")

    # Segunda linha de KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Cargas realizadas", resumo["cargas_realizadas"], help="Já entregues")
    with col2:
        st.metric("Cargas disponíveis", resumo["cargas_disponiveis"], help="Em aberto / no presente")
    with col3:
        st.metric("Cargas futuras", resumo["cargas_futuras"], help="Agendadas para o futuro")
    with col4:
        peso_kt = resumo["peso_total_kg_realizadas"] / 1000
        st.metric("Peso transportado (t)", f"{peso_kt:,.0f}".replace(",", "."), help="Toneladas nas cargas realizadas")
    with col5:
        st.metric("Ticket médio (R$)", f"R$ {resumo['valor_medio_frete_realizado']:,.0f}".replace(",", "."), help="Valor médio por frete realizado")

    # -------------------------------------------------------------------------
    # Gráficos – duas colunas
    # -------------------------------------------------------------------------
    st.markdown("---")
    st.markdown("### Distribuição e volume")

    c1, c2 = st.columns(2)

    with c1:
        # Cargas por status (pizza)
        df_status = pd.DataFrame([
            {"status": "Realizadas", "quantidade": resumo["cargas_realizadas"]},
            {"status": "Disponíveis", "quantidade": resumo["cargas_disponiveis"]},
            {"status": "Futuras", "quantidade": resumo["cargas_futuras"]},
        ])
        fig_status = px.pie(
            df_status, values="quantidade", names="status",
            color_discrete_sequence=[PRIMARY, SECONDARY, "#BDBDBD"],
            title="Cargas por status",
        )
        fig_status.update_layout(showlegend=True, margin=dict(t=40, b=20), height=320)
        st.plotly_chart(fig_status, use_container_width=True)

    with c2:
        # Cargas por tipo (top 8)
        df_tipo = pd.DataFrame(data["cargas_por_tipo"]).head(8)
        fig_tipo = px.bar(
            df_tipo, x="tipo_carga", y="quantidade",
            title="Top 8 tipos de carga (quantidade)",
            color="quantidade", color_continuous_scale=["#FFE0B2", PRIMARY],
        )
        fig_tipo.update_layout(showlegend=False, margin=dict(t=40, b=80), height=320, xaxis_tickangle=-45)
        fig_tipo.update_coloraxes(showscale=False)
        st.plotly_chart(fig_tipo, use_container_width=True)

    # Segunda linha de gráficos
    c1, c2 = st.columns(2)

    with c1:
        # Composição de veículo
        df_comp = pd.DataFrame(data["cargas_por_composicao"])
        fig_comp = px.bar(
            df_comp, x="composicao_veiculo", y="quantidade",
            title="Cargas por composição de veículo",
            color="quantidade", color_continuous_scale=["#B3E5FC", "#0277BD"],
        )
        fig_comp.update_layout(showlegend=False, margin=dict(t=40, b=100), height=320, xaxis_tickangle=-35)
        fig_comp.update_coloraxes(showscale=False)
        st.plotly_chart(fig_comp, use_container_width=True)

    with c2:
        # Tendência mensal (cargas realizadas)
        df_mes = pd.DataFrame(data["cargas_por_mes"])
        fig_mes = px.line(
            df_mes, x="ano_mes", y="realizadas",
            title="Cargas realizadas por mês",
            markers=True,
        )
        fig_mes.update_traces(line_color=PRIMARY, line_width=2)
        fig_mes.update_layout(margin=dict(t=40, b=60), height=320)
        st.plotly_chart(fig_mes, use_container_width=True)

    # Origem e destino por UF
    st.markdown("### Origem e destino por UF")
    df_orig = pd.DataFrame(data["cargas_por_uf_origem"])
    df_dest = pd.DataFrame(data["cargas_por_uf_destino"])
    col1, col2 = st.columns(2)
    with col1:
        fig_orig = px.bar(df_orig, x="estado", y="quantidade", title="Cargas por UF de origem", color_discrete_sequence=[PRIMARY])
        fig_orig.update_layout(height=340, margin=dict(b=60))
        st.plotly_chart(fig_orig, use_container_width=True)
    with col2:
        fig_dest = px.bar(df_dest, x="estado", y="quantidade", title="Cargas por UF de destino", color_discrete_sequence=[SECONDARY])
        fig_dest.update_layout(height=340, margin=dict(b=60))
        st.plotly_chart(fig_dest, use_container_width=True)

    # -------------------------------------------------------------------------
    # Data Quality (se existir)
    # -------------------------------------------------------------------------
    if dq and dq.get("total_registros_com_problema", 0) > 0:
        st.markdown("---")
        st.markdown("### Qualidade dos dados (demo)")
        total_problemas = dq["total_registros_com_problema"]
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Transportadoras c/ problema", dq.get("transportadoras_com_problema", 0))
        with col2:
            st.metric("Motoristas c/ problema", dq.get("motoristas_com_problema", 0))
        with col3:
            st.metric("Embarcadores c/ problema", dq.get("embarcadores_com_problema", 0))
        with col4:
            st.metric("Cargas c/ problema", dq.get("cargas_com_problema", 0))
        with col5:
            st.metric("Total registros com problema", total_problemas)
        st.caption("Registros inseridos para demonstração de regras de Data Quality (notebook 02).")

    st.markdown("---")
    st.caption("Dados carregados de dashboard_metrics.json. Para atualizar com dados do Databricks, execute o notebook 03_exportar_metricas_dashboard e copie o JSON para app/data/.")


if __name__ == "__main__":
    main()
