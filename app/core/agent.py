"""
Agente de IA para processar mensagens dos motoristas.
Usa Claude via Databricks Model Serving (OpenAI-compatible API).
"""
import json
import os
import re
from datetime import date, datetime

from core.matcher import find_loads, format_loads_list, normalizar_estado, mark_load_accepted
from core import state as state_mgr

# Modelo disponível no workspace Databricks
LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

INTENT_SYSTEM = """Você analisa mensagens de motoristas de caminhão no app Motz.
Considere o ESTADO ATUAL da conversa ao interpretar a intenção — o contexto é fundamental.

Estados possíveis da conversa:
- idle: motorista sem conversa ativa
- searching: motorista está vendo uma lista de cargas disponíveis
- matched: motorista já aceitou uma carga e está se preparando para partir
- in_trip: motorista está em viagem com uma carga
- delivered: motorista acabou de entregar uma carga

Intents possíveis:
- search_load: quer buscar nova carga (válido em idle, searching, delivered; ou quando quer cancelar a atual)
- accept_load: aceita uma carga da lista (só faz sentido em searching)
- start_trip: informa que saiu / iniciou viagem (só faz sentido em matched)
- complete_delivery: informa que entregou / chegou no destino (só faz sentido em in_trip)
- greeting: saudação sem menção a carga ou viagem
- other: dúvida, reclamação, ou intenção que não se encaixa nas anteriores

Retorne APENAS JSON válido, sem explicações:
{
  "intent": "<intent>",
  "origem_cidade": "<cidade ou null>",
  "origem_estado": "<estado por extenso ou sigla ou null>",
  "destino_cidade": "<cidade ou null>",
  "destino_estado": "<estado por extenso ou sigla ou null>",
  "numero_carga": <número inteiro da carga aceita ou null>,
  "data_coleta": "<YYYY-MM-DD se o motorista mencionou uma data específica (hoje, amanhã, dia X, etc.) — null se não mencionou>"
}"""

RESPONSE_SYSTEM = """Você é o assistente virtual do app Motz, o Uber dos caminhões.
Responda em português brasileiro, de forma amigável, direta e profissional.
Use o nome do motorista quando souber. Seja conciso. Use emojis com moderação.
Considere o histórico da conversa para dar respostas coerentes e contextualizadas."""


def _get_openai_client():
    """Cria cliente OpenAI apontando para o Databricks Model Serving."""
    try:
        from databricks.sdk import WorkspaceClient
        from openai import OpenAI
    except ImportError as e:
        raise RuntimeError(
            f"Pacote não instalado: {e}. Execute: pip install databricks-sdk openai"
        )

    w = WorkspaceClient()
    return OpenAI(
        api_key=w.config.token,
        base_url=f"{w.config.host}/serving-endpoints",
    )


def _build_state_summary(status: str, context: dict) -> str:
    """Constrói um resumo do estado atual da conversa para o LLM."""
    summary = f"Estado atual da conversa: {status}"
    carga = context.get("carga_aceita")
    if carga:
        summary += (
            f"\nCarga em andamento: {carga.get('tipo_carga')} | "
            f"{carga.get('origem_estado')} → {carga.get('destino_estado')} | "
            f"R$ {carga.get('valor_frete', 0):,.0f} | "
            f"Coleta: {carga.get('data_prevista_coleta')}"
        )
    oferecidas = context.get("cargas_oferecidas", [])
    if oferecidas:
        summary += f"\nCargas apresentadas ao motorista: {len(oferecidas)} opções"
    return summary


def _build_history_messages(history: list, context: dict) -> list:
    """
    Constrói a lista de mensagens de histórico para o LLM, respeitando sessões.

    Sessões são delimitadas por 'sessao_msg_inicio' no contexto — índice na lista
    de mensagens onde a conversa atual começou. Mensagens anteriores a esse ponto
    pertencem a sessões encerradas e são apresentadas com um separador explícito,
    evitando que o LLM as trate como parte do fluxo atual.

    Retorna até 20 mensagens da sessão atual + resumo das sessões anteriores.
    """
    sessao_inicio = context.get("sessao_msg_inicio", 0)
    msgs_anteriores = history[:sessao_inicio]
    msgs_sessao     = history[sessao_inicio:]

    result = []

    if msgs_anteriores:
        # Inclui as últimas 4 mensagens das sessões anteriores com marcador claro
        result.append({
            "role": "user",
            "content": "[SESSÕES ANTERIORES ENCERRADAS — exibido apenas para contexto histórico:]",
        })
        result.append({"role": "assistant", "content": "Entendido."})
        for msg in msgs_anteriores[-4:]:
            role = "user" if msg.get("role") == "driver" else "assistant"
            result.append({"role": role, "content": msg.get("content", "")})
        result.append({
            "role": "user",
            "content": "[FIM DAS SESSÕES ANTERIORES — a conversa atual começa abaixo:]",
        })
        result.append({"role": "assistant", "content": "Ok, nova sessão iniciada."})

    # Sessão atual: últimos 10 turnos (20 mensagens)
    for msg in msgs_sessao[-20:]:
        role = "user" if msg.get("role") == "driver" else "assistant"
        result.append({"role": role, "content": msg.get("content", "")})

    return result


def extract_intent(message: str, status: str, context: dict, history: list) -> dict:
    """Usa Claude para extrair intenção considerando estado atual e histórico da conversa."""
    client = _get_openai_client()
    state_summary = _build_state_summary(status, context)

    messages = [{"role": "system", "content": INTENT_SYSTEM}]
    messages.extend(_build_history_messages(history, context))

    # Mensagem atual com resumo do estado + data de hoje (para resolver "hoje"/"amanhã")
    hoje_iso = date.today().isoformat()
    messages.append({
        "role": "user",
        "content": f"{state_summary}\nData de hoje: {hoje_iso}\n\nMensagem do motorista: {message}",
    })

    response = client.chat.completions.create(
        model=LLM_ENDPOINT,
        max_tokens=300,
        messages=messages,
    )
    raw = response.choices[0].message.content.strip()
    raw = re.sub(r"```(?:json)?\n?", "", raw).strip("` \n")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {
            "intent": "other",
            "origem_cidade": None, "origem_estado": None,
            "destino_cidade": None, "destino_estado": None,
            "numero_carga": None, "data_coleta": None,
        }


def generate_response(driver: dict, response_context: dict, history: list, context: dict) -> str:
    """Gera resposta em linguagem natural com base no contexto e histórico da conversa."""
    client = _get_openai_client()
    context_text = json.dumps(response_context, ensure_ascii=False, indent=2)

    messages = [{"role": "system", "content": RESPONSE_SYSTEM}]
    messages.extend(_build_history_messages(history, context))

    prompt = f"""Motorista: {driver['nome']}
Caminhão: {driver['veiculo']['composicao']} {driver['veiculo']['caracteristica']} ({driver['veiculo']['modelo']})
Capacidade: {driver['veiculo']['capacidade_kg'] / 1000:.0f}t

Situação atual:
{context_text}

Gere uma resposta amigável e direta para o motorista com base na situação acima e no histórico da conversa.
Se houver lista de cargas no contexto, apresente-a de forma clara e organizada.
Se nenhuma carga foi encontrada, explique os critérios usados e sugira alternativas."""

    messages.append({"role": "user", "content": prompt})

    response = client.chat.completions.create(
        model=LLM_ENDPOINT,
        max_tokens=600,
        messages=messages,
    )
    return response.choices[0].message.content.strip()


def process_message(message: str, driver: dict, save_driver_msg: bool = True,
                    stored_content: str = None, msg_type: str = "text") -> str:
    """
    Pipeline completo: extrai intenção (com contexto) → executa lógica de estado → gera resposta.

    Faz uma única leitura do DB no início e uma única escrita no final, evitando
    dados obsoletos causados por leituras intercaladas com escritas no warehouse.

    Máquina de estados:
      idle       → search_load → searching
      searching  → accept_load → matched  |  search_load → searching (nova busca)
      matched    → start_trip  → in_trip  |  search_load → searching (cancela atual)
      in_trip    → complete_delivery → delivered
      delivered  → search_load → searching (nova viagem)

    stored_content: texto a salvar na mensagem do motorista (ex: "🎙️ transcript").
    """
    driver_id = driver["id"]
    conv    = state_mgr.get_conversation(driver_id)  # leitura única
    status  = conv.get("status", "idle")
    context = dict(conv.get("context", {}))           # cópia mutável
    history = list(conv.get("messages", []))           # cópia mutável
    now     = datetime.now().strftime("%H:%M")

    # Extrai intenção com contexto e histórico da conversa
    intent_data = extract_intent(message, status, context, history)
    intent      = intent_data.get("intent", "other")

    response_context = {"intent": intent, "status_atual": status}
    new_status = status  # será atualizado pela máquina de estados

    # ── search_load ────────────────────────────────────────────────────────────
    if intent == "search_load":
        carga_atual     = context.get("carga_aceita") or {}
        data_referencia = None

        dest_cidade  = intent_data.get("destino_cidade")
        orig_cidade  = intent_data.get("origem_cidade")
        data_coleta  = intent_data.get("data_coleta")  # YYYY-MM-DD ou None

        # IDs já mostrados nesta sessão — não repetir nas próximas buscas
        ja_oferecidos = [c["id"] for c in context.get("cargas_oferecidas", []) if c.get("id")]

        if status == "in_trip" and carga_atual:
            orig_estado     = intent_data.get("origem_estado") or carga_atual.get("destino_estado")
            dest_estado     = intent_data.get("destino_estado")
            data_referencia = carga_atual.get("data_prevista_coleta")
            response_context["contexto_viagem"] = {
                "em_viagem_para": carga_atual.get("destino_estado"),
                "buscando_a_partir_de": orig_estado,
                "data_referencia": data_referencia,
            }
        else:
            orig_estado = intent_data.get("origem_estado") or driver.get("localizacao_estado")
            dest_estado = intent_data.get("destino_estado")

        loads, orig_match = find_loads(
            driver, dest_estado, orig_estado, data_referencia=data_referencia,
            dest_cidade=dest_cidade, exclude_ids=ja_oferecidos or None,
            data_coleta=data_coleta, orig_cidade=orig_cidade,
        )

        # Aviso de origem: se pediu cidade específica mas não encontrou lá
        if orig_cidade and orig_match == "estado":
            response_context["aviso_origem"] = (
                f"Não há cargas disponíveis saindo de {orig_cidade} no momento. "
                f"As opções abaixo saem de outras cidades do mesmo estado — cidades próximas."
            )
        elif orig_cidade and orig_match == "nenhuma":
            response_context["aviso_origem"] = (
                f"Não há cargas disponíveis saindo de {orig_cidade} nem de outras cidades "
                f"do estado no período solicitado."
            )

        # Aviso de data: se pediu data específica, verificar se os resultados batem
        if data_coleta and loads:
            datas_retornadas = {c["data_prevista_coleta"] for c in loads}
            if datas_retornadas != {data_coleta}:
                response_context["aviso_data"] = (
                    f"ATENÇÃO: o motorista pediu cargas para {data_coleta}, mas os resultados "
                    f"incluem datas diferentes ({', '.join(sorted(datas_retornadas))}). "
                    "Informe ao motorista as datas reais das cargas apresentadas."
                )
        elif data_coleta and not loads:
            response_context["aviso_data"] = (
                f"Nenhuma carga encontrada para a data exata {data_coleta}. "
                "Informe ao motorista que não há cargas disponíveis para esse dia específico."
            )

        context["origem_estado"]      = normalizar_estado(orig_estado) if orig_estado else driver.get("localizacao_estado")
        context["destino_estado"]     = normalizar_estado(dest_estado) if dest_estado else None
        context["cargas_oferecidas"]  = loads
        context["carga_aceita"]       = None
        context["viagem_iniciada_em"] = None
        # sessao_msg_inicio = índice da mensagem do motorista na lista final.
        # len(history) aponta para onde a mensagem do motorista será inserida.
        context["sessao_msg_inicio"]  = len(history)
        new_status = "searching"

        veiculo = f"{driver['veiculo']['composicao']} {driver['veiculo']['caracteristica']}"
        cidade_orig_label = orig_cidade or orig_estado or driver.get("localizacao_estado", "?")
        rota = f"{cidade_orig_label} → {dest_cidade or dest_estado or 'qualquer destino'}"
        response_context.update({
            "veiculo_motorista": veiculo,
            "rota_buscada": rota,
            "origem_match": orig_match,
            "cargas_encontradas": len(loads),
            "lista_cargas": format_loads_list(loads) if loads else f"Nenhuma carga disponível para {veiculo} nessa rota.",
        })
        if status not in ("idle", "searching", "delivered"):
            response_context["aviso"] = f"Conversa anterior (status: {status}) encerrada para nova busca."

    # ── accept_load ────────────────────────────────────────────────────────────
    elif intent == "accept_load":
        if status != "searching":
            response_context["mensagem"] = (
                "Você precisa buscar cargas primeiro antes de aceitar uma."
                if status == "idle"
                else f"Não é possível aceitar uma nova carga com status '{status}'. "
                     + ("Finalize a entrega antes." if status == "in_trip" else "")
            )
        else:
            numero            = intent_data.get("numero_carga")
            cargas_oferecidas = context.get("cargas_oferecidas", [])

            if numero and 1 <= numero <= len(cargas_oferecidas):
                carga = cargas_oferecidas[numero - 1]
            elif cargas_oferecidas:
                carga = cargas_oferecidas[0]
            else:
                carga = None

            if carga:
                context["carga_aceita"] = carga
                new_status = "matched"
                mark_load_accepted(carga["id"], driver_id)
                response_context.update({"carga_aceita": carga, "mensagem": "Carga aceita com sucesso!"})
            else:
                response_context["mensagem"] = "Não encontrei a carga informada. Tente buscar novamente."

    # ── start_trip ─────────────────────────────────────────────────────────────
    elif intent == "start_trip":
        if status != "matched":
            carga = context.get("carga_aceita")
            if carga and status == "searching":
                context["viagem_iniciada_em"] = datetime.now().isoformat()
                new_status = "in_trip"
                response_context.update({"carga": carga, "mensagem": "Viagem iniciada a partir da carga aceita."})
            else:
                response_context["mensagem"] = (
                    "Você ainda não aceitou nenhuma carga. Faça uma busca primeiro."
                    if status in ("idle", "searching")
                    else "Você já está em viagem." if status == "in_trip"
                    else "Aceite uma carga antes de iniciar a viagem."
                )
        else:
            carga = context.get("carga_aceita")
            context["viagem_iniciada_em"] = datetime.now().isoformat()
            new_status = "in_trip"
            response_context.update({"carga": carga, "mensagem": "Viagem iniciada! Boa viagem e segurança na estrada."})

    # ── complete_delivery ──────────────────────────────────────────────────────
    elif intent == "complete_delivery":
        if status != "in_trip":
            response_context["mensagem"] = (
                "Você não está em viagem no momento."
                if status != "matched"
                else "Você aceitou uma carga mas ainda não iniciou a viagem. Confirme a saída primeiro."
            )
        else:
            carga = context.get("carga_aceita")
            context["entregue_em"] = datetime.now().isoformat()
            new_status = "delivered"
            response_context.update({
                "carga": carga,
                "mensagem": "Entrega confirmada! Parabéns pela viagem concluída.",
                "dica": "Quando quiser uma nova carga, é só me avisar.",
            })

    # ── greeting ───────────────────────────────────────────────────────────────
    elif intent == "greeting":
        carga = context.get("carga_aceita")
        response_context.update({
            "mensagem": "Saudação recebida.",
            "status_conversa": status,
            "carga_ativa": carga,
            "dica": (
                "O motorista tem uma carga ativa — pode informar início de viagem ou entrega."
                if carga
                else "O motorista pode buscar uma carga informando origem e destino."
            ),
        })

    # ── other ──────────────────────────────────────────────────────────────────
    else:
        response_context.update({
            "mensagem": "Mensagem recebida, mas não entendi claramente a intenção.",
            "status_conversa": status,
            "carga_ativa": context.get("carga_aceita"),
        })

    response_text = generate_response(driver, response_context, history, context)

    # Constrói lista final de mensagens e faz uma única escrita no DB,
    # eliminando leituras intercaladas que retornam dados obsoletos do warehouse.
    final_messages = list(history)
    if save_driver_msg:
        final_messages.append({
            "role": "driver",
            "content": stored_content if stored_content is not None else message,
            "type": msg_type,
            "timestamp": now,
        })
    final_messages.append({
        "role": "assistant",
        "content": response_text,
        "type": "text",
        "timestamp": now,
    })

    state_mgr.save_conversation_full(driver_id, {
        "status": new_status,
        "context": context,
        "messages": final_messages,
    })

    return response_text
