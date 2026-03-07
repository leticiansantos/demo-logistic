"""
Simulação de WhatsApp para motoristas – Motz Demo
Interface web que simula a conversa do motorista com o sistema via WhatsApp.
"""
import json
import sys
from pathlib import Path

import streamlit as st

# Garante que o diretório app/ está no path para importar core/
sys.path.insert(0, str(Path(__file__).parent.parent))

from core import state as state_mgr
from core.agent import process_message
from core.transcriber import transcribe

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
MOTORISTAS_FILE = Path(__file__).parent.parent / "data" / "motoristas_demo.json"

st.set_page_config(
    page_title="Motz – WhatsApp Simulator",
    page_icon="💬",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# -----------------------------------------------------------------------------
# CSS – visual WhatsApp Web
# -----------------------------------------------------------------------------
st.markdown("""
<style>
  /* Reset geral */
  .stApp { background: #e5ddd5 !important; }
  section[data-testid="stSidebar"] { display: none !important; }

  /* Container do chat */
  .wa-wrapper {
    display: flex;
    height: calc(100vh - 100px);
    background: #e5ddd5;
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 4px 24px rgba(0,0,0,0.15);
    max-width: 1100px;
    margin: 0 auto;
  }

  /* Painel esquerdo - lista de contatos */
  .wa-contacts {
    width: 320px;
    min-width: 280px;
    background: #fff;
    border-right: 1px solid #e0e0e0;
    display: flex;
    flex-direction: column;
  }
  .wa-contacts-header {
    background: #075E54;
    color: white;
    padding: 14px 16px;
    font-size: 1.1rem;
    font-weight: 700;
    letter-spacing: 0.01em;
  }
  .wa-contact-item {
    display: flex;
    align-items: center;
    padding: 12px 16px;
    cursor: pointer;
    border-bottom: 1px solid #f0f0f0;
    transition: background 0.15s;
  }
  .wa-contact-item:hover { background: #f5f5f5; }
  .wa-contact-item.active { background: #ebebeb; }
  .wa-avatar {
    width: 44px; height: 44px;
    border-radius: 50%;
    background: #075E54;
    color: white;
    display: flex; align-items: center; justify-content: center;
    font-size: 1.2rem; font-weight: 700;
    margin-right: 12px; flex-shrink: 0;
  }
  .wa-contact-info { flex: 1; min-width: 0; }
  .wa-contact-name { font-weight: 600; font-size: 0.95rem; color: #111; }
  .wa-contact-sub { font-size: 0.78rem; color: #888; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .wa-status-badge {
    font-size: 0.65rem; font-weight: 700;
    padding: 2px 7px; border-radius: 10px;
    display: inline-block; margin-top: 3px;
  }
  .badge-idle { background: #e0e0e0; color: #666; }
  .badge-searching { background: #FFF9C4; color: #F57F17; }
  .badge-matched { background: #C8E6C9; color: #1B5E20; }
  .badge-in_trip { background: #BBDEFB; color: #0D47A1; }
  .badge-delivered { background: #D7CCC8; color: #4E342E; }

  /* Painel direito - chat */
  .wa-chat {
    flex: 1;
    display: flex;
    flex-direction: column;
    background: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='300' height='300'%3E%3Crect fill='%23ddd6cb' width='300' height='300'/%3E%3C/svg%3E");
  }
  .wa-chat-header {
    background: #075E54;
    color: white;
    padding: 12px 18px;
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .wa-chat-header .name { font-weight: 700; font-size: 1rem; }
  .wa-chat-header .sub { font-size: 0.78rem; opacity: 0.85; }

  /* Mensagens */
  .wa-messages {
    flex: 1;
    overflow-y: auto;
    padding: 16px 40px;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .wa-bubble-wrap { display: flex; margin-bottom: 2px; }
  .wa-bubble-wrap.driver { justify-content: flex-end; }
  .wa-bubble-wrap.assistant { justify-content: flex-start; }
  .wa-bubble {
    max-width: 68%;
    padding: 8px 12px 6px;
    border-radius: 8px;
    font-size: 0.88rem;
    line-height: 1.5;
    position: relative;
    word-wrap: break-word;
    white-space: pre-wrap;
  }
  .wa-bubble.driver {
    background: #DCF8C6;
    border-bottom-right-radius: 2px;
    color: #111;
  }
  .wa-bubble.assistant {
    background: white;
    border-bottom-left-radius: 2px;
    color: #111;
  }
  .wa-timestamp {
    font-size: 0.65rem;
    color: #aaa;
    text-align: right;
    margin-top: 2px;
  }
  .wa-bubble.driver .wa-timestamp { color: #7BAE72; }

  /* Input */
  .wa-input-area {
    background: #f0f0f0;
    padding: 10px 16px;
    display: flex;
    align-items: center;
    gap: 8px;
    border-top: 1px solid #ddd;
  }

  /* Ajustes Streamlit dentro do tema */
  .stTextInput > div > div > input {
    border-radius: 20px !important;
    border: none !important;
    background: white !important;
    padding: 10px 16px !important;
  }
  div[data-testid="stFileUploader"] {
    background: white;
    border-radius: 12px;
    padding: 8px 12px;
  }
  .stButton > button {
    background: #25D366 !important;
    color: white !important;
    border: none !important;
    border-radius: 20px !important;
    padding: 8px 20px !important;
    font-weight: 600 !important;
  }
  .stButton > button:hover {
    background: #128C7E !important;
  }
  .truck-info-box {
    background: #F1F8E9;
    border-left: 3px solid #25D366;
    border-radius: 6px;
    padding: 8px 12px;
    font-size: 0.82rem;
    color: #333;
    margin-bottom: 8px;
  }
  .reset-btn > button {
    background: #ff5252 !important;
    border-radius: 20px !important;
    font-size: 0.75rem !important;
    padding: 4px 12px !important;
  }
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# Funções helpers
# -----------------------------------------------------------------------------
def load_motoristas() -> list[dict]:
    with open(MOTORISTAS_FILE, encoding="utf-8") as f:
        return json.load(f)


STATUS_LABEL = {
    "idle": ("💤 Aguardando", "badge-idle"),
    "searching": ("🔍 Buscando carga", "badge-searching"),
    "matched": ("✅ Carga aceita", "badge-matched"),
    "in_trip": ("🚛 Em viagem", "badge-in_trip"),
    "delivered": ("🏁 Entregue", "badge-delivered"),
}


def render_bubble(msg: dict):
    role = msg["role"]
    content = msg["content"]
    ts = msg.get("timestamp", "")
    type_icon = "🎙️ " if msg.get("type") == "audio" else ""
    role_css = "driver" if role == "driver" else "assistant"

    st.markdown(f"""
    <div class="wa-bubble-wrap {role_css}">
      <div class="wa-bubble {role_css}">
        {type_icon}{content}
        <div class="wa-timestamp">{ts}</div>
      </div>
    </div>
    """, unsafe_allow_html=True)


def check_databricks_config() -> str | None:
    """Verifica se o Databricks SDK consegue autenticar. Retorna mensagem de erro ou None."""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        # Acessa host para disparar erro de config se não estiver configurado
        _ = w.config.host
        return None
    except Exception as e:
        return (
            "Databricks não configurado. Configure via CLI (`databricks configure`) "
            "ou variáveis de ambiente:\n"
            "```bash\nexport DATABRICKS_HOST=https://seu-workspace.azuredatabricks.net\n"
            "export DATABRICKS_TOKEN=dapi...\n```\n\n"
            f"Detalhe: {e}"
        )


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    motoristas = load_motoristas()

    # Verifica config Databricks
    db_error = check_databricks_config()
    if db_error:
        st.warning(db_error)

    # Cabeçalho da página
    st.markdown("""
    <div style="text-align:center; padding: 12px 0 4px;">
      <span style="font-size:1.4rem; font-weight:700; color:#075E54;">💬 Motz – Simulador WhatsApp</span>
      <br/>
      <span style="font-size:0.85rem; color:#666;">Simule a experiência do motorista no WhatsApp</span>
    </div>
    """, unsafe_allow_html=True)

    # Layout em colunas: contatos (1/4) | chat (3/4)
    col_contacts, col_chat = st.columns([1, 3])

    # -------------------------------------------------------------------------
    # Painel de contatos (esquerda)
    # -------------------------------------------------------------------------
    with col_contacts:
        st.markdown("""
        <div style="background:#075E54; color:white; padding:12px 14px; border-radius:8px 8px 0 0;
                    font-weight:700; font-size:1rem; margin-bottom:0;">
          🚛 Motoristas
        </div>
        """, unsafe_allow_html=True)

        # Selecionar motorista
        selected_id = st.session_state.get("selected_driver_id", motoristas[0]["id"])

        for m in motoristas:
            conv = state_mgr.get_conversation(m["id"])
            status = conv.get("status", "idle")
            label, badge_css = STATUS_LABEL.get(status, ("💤 Aguardando", "badge-idle"))
            initials = "".join(p[0] for p in m["nome"].split()[:2]).upper()
            is_active = m["id"] == selected_id

            # Botão estilizado de seleção
            btn_style = "border: 2px solid #075E54;" if is_active else "border: 1px solid #e0e0e0;"
            if st.button(
                f"{'●' if is_active else '○'} {m['nome']}",
                key=f"contact_{m['id']}",
                use_container_width=True,
            ):
                st.session_state["selected_driver_id"] = m["id"]
                st.rerun()

            # Info do status abaixo do botão
            last_msg = ""
            if conv.get("messages"):
                last_msg = conv["messages"][-1]["content"][:40] + "..."
            st.markdown(f"""
            <div style="font-size:0.72rem; color:#888; padding: 0 4px 8px; margin-top:-8px;">
              <span class="wa-status-badge {badge_css}">{label}</span>
              <br/><span style="color:#aaa;">{last_msg}</span>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("---")

        # Botão para resetar todas as conversas
        if st.button("🗑️ Limpar todas conversas", use_container_width=True):
            for m in motoristas:
                state_mgr.reset_conversation(m["id"])
            st.rerun()

    # -------------------------------------------------------------------------
    # Painel de chat (direita)
    # -------------------------------------------------------------------------
    with col_chat:
        # Pega o motorista selecionado
        selected_id = st.session_state.get("selected_driver_id", motoristas[0]["id"])
        driver = next((m for m in motoristas if m["id"] == selected_id), motoristas[0])
        conv = state_mgr.get_conversation(driver["id"])

        # Header do chat
        status = conv.get("status", "idle")
        status_label, _ = STATUS_LABEL.get(status, ("💤 Aguardando", "badge-idle"))
        st.markdown(f"""
        <div style="background:#075E54; color:white; padding:12px 18px; border-radius:8px;
                    margin-bottom:8px; display:flex; align-items:center; gap:12px;">
          <div style="width:44px;height:44px;border-radius:50%;background:#128C7E;
                      display:flex;align-items:center;justify-content:center;
                      font-size:1.3rem;font-weight:700;flex-shrink:0;">
            {"".join(p[0] for p in driver["nome"].split()[:2]).upper()}
          </div>
          <div>
            <div style="font-weight:700;font-size:1rem;">{driver["nome"]}</div>
            <div style="font-size:0.78rem;opacity:0.85;">
              {driver["veiculo"]["composicao"]} {driver["veiculo"]["caracteristica"]} •
              {driver["veiculo"]["modelo"]} • {status_label}
            </div>
          </div>
        </div>
        """, unsafe_allow_html=True)

        # Info do caminhão
        v = driver["veiculo"]
        ctx = conv.get("context", {})
        carga_aceita = ctx.get("carga_aceita")
        if carga_aceita:
            st.markdown(f"""
            <div class="truck-info-box">
              ✅ <b>Carga aceita:</b> {carga_aceita['tipo_carga']} |
              {carga_aceita['origem_cidade']}/{carga_aceita['origem_estado']} →
              {carga_aceita['destino_cidade']}/{carga_aceita['destino_estado']} |
              R$ {carga_aceita['valor_frete']:,.0f} | Coleta: {carga_aceita['data_prevista_coleta']}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
            <div class="truck-info-box">
              🚛 <b>{v['composicao']} {v['caracteristica']}</b> •
              {v['modelo']} • Cap. {v['capacidade_kg']/1000:.0f}t •
              Localização: {driver['localizacao_atual']}/{driver['localizacao_estado']}
            </div>
            """, unsafe_allow_html=True)

        # Área de mensagens
        messages = conv.get("messages", [])
        if not messages:
            st.markdown("""
            <div style="text-align:center; color:#aaa; padding:40px 20px; font-size:0.9rem;">
              💬 Nenhuma mensagem ainda.<br/>
              Digite abaixo ou envie um áudio para começar.
            </div>
            """, unsafe_allow_html=True)
        else:
            msgs_container = st.container()
            with msgs_container:
                for msg in messages:
                    render_bubble(msg)

        st.markdown("---")

        # -------------------------------------------------------------------------
        # Área de input
        # -------------------------------------------------------------------------
        st.markdown("**Enviar mensagem**")

        # Sugestões rápidas
        st.markdown("<span style='font-size:0.78rem;color:#888;'>Sugestões rápidas:</span>", unsafe_allow_html=True)
        sugestoes = [
            "Olá! Estou em São Paulo e quero ir pra Tocantins, tem carga?",
            "Quero a carga número 1",
            "Iniciei o trajeto, saí para coleta",
            "Entregue! Finalizei a entrega",
        ]
        cols_sug = st.columns(len(sugestoes))
        for i, (col, sug) in enumerate(zip(cols_sug, sugestoes)):
            with col:
                if st.button(sug[:30] + "...", key=f"sug_{i}", use_container_width=True):
                    st.session_state["quick_msg"] = sug

        # Input de texto
        default_text = st.session_state.pop("quick_msg", "")
        text_input = st.text_input(
            "Mensagem de texto",
            value=default_text,
            placeholder="Digite a mensagem do motorista...",
            label_visibility="collapsed",
            key="text_input_field",
        )

        col_send, col_audio, col_reset = st.columns([2, 2, 1])

        with col_send:
            if st.button("📤 Enviar texto", use_container_width=True, type="primary"):
                if text_input.strip():
                    with st.spinner("Processando..."):
                        try:
                            process_message(text_input.strip(), driver)
                        except Exception as e:
                            st.error(f"Erro ao processar mensagem: {e}")
                    st.rerun()
                else:
                    st.warning("Digite uma mensagem antes de enviar.")

        with col_audio:
            audio_file = st.file_uploader(
                "Áudio (ogg/mp3/wav/m4a)",
                type=["ogg", "mp3", "wav", "m4a", "webm", "opus"],
                label_visibility="collapsed",
                key="audio_uploader",
            )
            if audio_file and st.button("🎙️ Transcrever e enviar", use_container_width=True):
                with st.spinner("Transcrevendo áudio via Databricks..."):
                    try:
                        audio_bytes = audio_file.read()
                        texto_transcrito = transcribe(audio_bytes, audio_file.name)
                        st.info(f"🎙️ Transcrição: *{texto_transcrito}*")
                        state_mgr.add_message(driver["id"], "driver", f"🎙️ {texto_transcrito}", "audio")
                        process_message(texto_transcrito, driver)
                    except Exception as e:
                        st.error(str(e))
                st.rerun()

        with col_reset:
            if st.button("🗑️ Reset", use_container_width=True, key="reset_conv"):
                state_mgr.reset_conversation(driver["id"])
                st.rerun()


if __name__ == "__main__":
    main()
