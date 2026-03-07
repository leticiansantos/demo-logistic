"""
Dashboard "Ao Vivo" – Motz Demo
Monitoramento em tempo real das conversas e viagens ativas.
"""
import json
import sys
from pathlib import Path
from datetime import datetime

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from core import state as state_mgr

MOTORISTAS_FILE = Path(__file__).parent.parent / "data" / "motoristas_demo.json"

st.set_page_config(
    page_title="Motz – Ao Vivo",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# -----------------------------------------------------------------------------
# CSS
# -----------------------------------------------------------------------------
st.markdown("""
<style>
  .stApp { background: #0f1117; color: #fafafa; }
  h1, h2, h3 { color: #fafafa !important; }

  .driver-card {
    background: #1e2130;
    border-radius: 12px;
    padding: 16px 18px;
    border-left: 4px solid #333;
    margin-bottom: 12px;
    position: relative;
  }
  .driver-card.idle { border-left-color: #555; }
  .driver-card.searching { border-left-color: #FFB300; }
  .driver-card.matched { border-left-color: #43A047; }
  .driver-card.in_trip { border-left-color: #1E88E5; }
  .driver-card.delivered { border-left-color: #8D6E63; }

  .driver-name { font-size: 1rem; font-weight: 700; color: #fff; margin-bottom: 4px; }
  .driver-truck { font-size: 0.78rem; color: #aaa; margin-bottom: 8px; }
  .driver-status-badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.03em;
  }
  .badge-idle { background: #333; color: #aaa; }
  .badge-searching { background: #4A3800; color: #FFB300; }
  .badge-matched { background: #1B5E20; color: #A5D6A7; }
  .badge-in_trip { background: #0D47A1; color: #90CAF9; }
  .badge-delivered { background: #3E2723; color: #BCAAA4; }

  .route-info {
    font-size: 0.82rem;
    color: #ccc;
    margin-top: 8px;
    padding: 6px 10px;
    background: rgba(255,255,255,0.05);
    border-radius: 6px;
  }

  .kpi-box {
    background: #1e2130;
    border-radius: 12px;
    padding: 16px 20px;
    text-align: center;
    margin-bottom: 8px;
  }
  .kpi-value { font-size: 2rem; font-weight: 800; color: #E65100; }
  .kpi-label { font-size: 0.78rem; color: #aaa; text-transform: uppercase; letter-spacing: 0.05em; }

  .activity-item {
    display: flex;
    align-items: flex-start;
    gap: 12px;
    padding: 8px 0;
    border-bottom: 1px solid #2a2d3a;
    font-size: 0.83rem;
  }
  .activity-time { color: #E65100; font-weight: 700; flex-shrink: 0; min-width: 50px; }
  .activity-text { color: #ccc; }
  .activity-driver { color: #fff; font-weight: 600; }

  .last-msg-box {
    background: rgba(255,255,255,0.04);
    border-radius: 6px;
    padding: 6px 10px;
    font-size: 0.78rem;
    color: #bbb;
    margin-top: 6px;
    font-style: italic;
  }

  .pulse-dot {
    display: inline-block;
    width: 8px; height: 8px;
    border-radius: 50%;
    background: #43A047;
    margin-right: 6px;
    animation: pulse 1.5s infinite;
  }
  @keyframes pulse {
    0% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.5; transform: scale(1.3); }
    100% { opacity: 1; transform: scale(1); }
  }
  .section-title {
    font-size: 0.75rem;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #666;
    margin: 16px 0 8px;
  }
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def load_motoristas() -> list[dict]:
    with open(MOTORISTAS_FILE, encoding="utf-8") as f:
        return json.load(f)


STATUS_CONFIG = {
    "idle":      ("💤 Aguardando",   "badge-idle"),
    "searching": ("🔍 Buscando",     "badge-searching"),
    "matched":   ("✅ Carga aceita", "badge-matched"),
    "in_trip":   ("🚛 Em viagem",    "badge-in_trip"),
    "delivered": ("🏁 Entregue",     "badge-delivered"),
}


def collect_activity_feed(motoristas: list[dict]) -> list[dict]:
    """Coleta todos os eventos de todas as conversas, ordenados por timestamp."""
    events = []
    for m in motoristas:
        conv = state_mgr.get_conversation(m["id"])
        for msg in conv.get("messages", []):
            role = msg["role"]
            if role == "driver":
                icon = "💬"
                text = f"<span class='activity-driver'>{m['nome']}</span> enviou: {msg['content'][:80]}..."
            else:
                icon = "🤖"
                text = f"Sistema respondeu para <span class='activity-driver'>{m['nome']}</span>: {msg['content'][:80]}..."
            events.append({
                "time": msg.get("timestamp", "—"),
                "icon": icon,
                "text": text,
                "driver": m["nome"],
            })
    # Mantém os 20 mais recentes (última mensagem = mais recente)
    return events[-20:][::-1]


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    motoristas = load_motoristas()

    # Cabeçalho
    st.markdown("""
    <div style="display:flex; align-items:center; gap:12px; margin-bottom:4px;">
      <span style="font-size:1.8rem;">📡</span>
      <div>
        <span style="font-size:1.4rem;font-weight:800;color:#fff;">Motz – Central de Operações</span>
        <br/>
        <span class="pulse-dot"></span>
        <span style="font-size:0.82rem;color:#aaa;">Monitoramento em tempo real das conversas e viagens</span>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # Botão de refresh
    col_h1, col_h2 = st.columns([4, 1])
    with col_h2:
        if st.button("🔄 Atualizar", use_container_width=True):
            st.rerun()

    state = state_mgr.load_state()
    conversations = state.get("conversations", {})
    last_updated = state.get("last_updated")

    if last_updated:
        try:
            dt = datetime.fromisoformat(last_updated)
            st.caption(f"Última atualização: {dt.strftime('%H:%M:%S')}")
        except Exception:
            pass

    st.markdown("---")

    # -------------------------------------------------------------------------
    # KPIs
    # -------------------------------------------------------------------------
    total_ativas = sum(1 for c in conversations.values() if c.get("status") != "idle")
    total_buscando = sum(1 for c in conversations.values() if c.get("status") == "searching")
    total_matched = sum(1 for c in conversations.values() if c.get("status") == "matched")
    total_em_viagem = sum(1 for c in conversations.values() if c.get("status") == "in_trip")
    total_entregues = sum(1 for c in conversations.values() if c.get("status") == "delivered")

    # Valor total de cargas aceitas
    valor_total = 0.0
    for c in conversations.values():
        ctx = c.get("context", {})
        carga = ctx.get("carga_aceita")
        if carga and c.get("status") in ("matched", "in_trip", "delivered"):
            valor_total += carga.get("valor_frete", 0)

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.markdown(f"""<div class="kpi-box">
          <div class="kpi-value">{total_ativas}</div>
          <div class="kpi-label">Conversas ativas</div>
        </div>""", unsafe_allow_html=True)
    with k2:
        st.markdown(f"""<div class="kpi-box">
          <div class="kpi-value" style="color:#FFB300;">{total_buscando}</div>
          <div class="kpi-label">Buscando carga</div>
        </div>""", unsafe_allow_html=True)
    with k3:
        st.markdown(f"""<div class="kpi-box">
          <div class="kpi-value" style="color:#43A047;">{total_matched}</div>
          <div class="kpi-label">Carga aceita</div>
        </div>""", unsafe_allow_html=True)
    with k4:
        st.markdown(f"""<div class="kpi-box">
          <div class="kpi-value" style="color:#1E88E5;">{total_em_viagem}</div>
          <div class="kpi-label">Em viagem</div>
        </div>""", unsafe_allow_html=True)
    with k5:
        valor_fmt = f"R$ {valor_total:,.0f}".replace(",", ".")
        st.markdown(f"""<div class="kpi-box">
          <div class="kpi-value" style="color:#E65100;">{valor_fmt}</div>
          <div class="kpi-label">Fretes negociados</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Cards dos motoristas
    # -------------------------------------------------------------------------
    col_drivers, col_feed = st.columns([3, 2])

    with col_drivers:
        st.markdown('<div class="section-title">Status dos motoristas</div>', unsafe_allow_html=True)

        for m in motoristas:
            conv = conversations.get(m["id"], {})
            status = conv.get("status", "idle")
            label, badge_css = STATUS_CONFIG.get(status, ("💤 Aguardando", "badge-idle"))
            ctx = conv.get("context", {})
            carga = ctx.get("carga_aceita")
            messages = conv.get("messages", [])
            last_msg = messages[-1]["content"][:90] if messages else None

            # Rota info
            route_html = ""
            if carga:
                valor = f"R$ {carga['valor_frete']:,.0f}".replace(",", ".")
                route_html = f"""
                <div class="route-info">
                  📦 {carga['tipo_carga']} &nbsp;|&nbsp;
                  {carga['origem_cidade']}/{carga['origem_estado']} → {carga['destino_cidade']}/{carga['destino_estado']}
                  &nbsp;|&nbsp; {valor} &nbsp;|&nbsp; Coleta: {carga['data_prevista_coleta']}
                </div>"""
            elif ctx.get("destino_estado"):
                route_html = f"""
                <div class="route-info">
                  🔍 Buscando cargas para {ctx.get('destino_estado', '?')}
                  &nbsp;·&nbsp; {len(ctx.get('cargas_oferecidas', []))} opções encontradas
                </div>"""

            last_msg_html = f'<div class="last-msg-box">"{last_msg}"</div>' if last_msg else ""

            initials = "".join(p[0] for p in m["nome"].split()[:2]).upper()
            v = m["veiculo"]

            st.markdown(f"""
            <div class="driver-card {status}">
              <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;">
                <div style="width:40px;height:40px;border-radius:50%;background:#2a2d3a;
                            display:flex;align-items:center;justify-content:center;
                            font-size:1rem;font-weight:700;color:#E65100;flex-shrink:0;">
                  {initials}
                </div>
                <div style="flex:1;">
                  <div class="driver-name">{m['nome']}</div>
                  <div class="driver-truck">
                    {v['composicao']} {v['caracteristica']} · {v['modelo']} ·
                    {m['localizacao_atual']}/{m['localizacao_estado']}
                  </div>
                </div>
                <span class="driver-status-badge {badge_css}">{label}</span>
              </div>
              {route_html}
              {last_msg_html}
            </div>
            """, unsafe_allow_html=True)

    # -------------------------------------------------------------------------
    # Feed de atividade
    # -------------------------------------------------------------------------
    with col_feed:
        st.markdown('<div class="section-title">Feed de atividade</div>', unsafe_allow_html=True)

        events = collect_activity_feed(motoristas)

        if not events:
            st.markdown("""
            <div style="text-align:center; color:#555; padding:40px 0;">
              Nenhuma atividade ainda.<br/>
              <span style="font-size:0.8rem;">Inicie uma conversa no simulador WhatsApp.</span>
            </div>
            """, unsafe_allow_html=True)
        else:
            feed_html = ""
            for ev in events:
                feed_html += f"""
                <div class="activity-item">
                  <span class="activity-time">{ev['icon']} {ev['time']}</span>
                  <span class="activity-text">{ev['text']}</span>
                </div>"""
            st.markdown(feed_html, unsafe_allow_html=True)

    st.markdown("---")

    # -------------------------------------------------------------------------
    # Tabela de cargas em andamento
    # -------------------------------------------------------------------------
    cargas_ativas = []
    for m in motoristas:
        conv = conversations.get(m["id"], {})
        if conv.get("status") in ("matched", "in_trip", "delivered"):
            ctx = conv.get("context", {})
            carga = ctx.get("carga_aceita")
            if carga:
                status_label, _ = STATUS_CONFIG.get(conv["status"], ("—", ""))
                cargas_ativas.append({
                    "Motorista": m["nome"],
                    "Caminhão": f"{m['veiculo']['composicao']} {m['veiculo']['caracteristica']}",
                    "Carga": carga["tipo_carga"],
                    "Rota": f"{carga['origem_cidade']}/{carga['origem_estado']} → {carga['destino_cidade']}/{carga['destino_estado']}",
                    "Coleta": carga["data_prevista_coleta"],
                    "Valor": f"R$ {carga['valor_frete']:,.0f}".replace(",", "."),
                    "Status": status_label,
                })

    if cargas_ativas:
        st.markdown('<div class="section-title">Cargas em andamento</div>', unsafe_allow_html=True)
        import pandas as pd
        st.dataframe(
            pd.DataFrame(cargas_ativas),
            use_container_width=True,
            hide_index=True,
        )

    # Auto-refresh opcional
    st.markdown("---")
    if st.checkbox("Auto-atualizar a cada 5 segundos", value=False):
        import time
        time.sleep(5)
        st.rerun()


if __name__ == "__main__":
    main()
