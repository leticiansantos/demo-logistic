"""
Agente de IA para processar mensagens dos motoristas.
Usa Claude via Databricks Model Serving (OpenAI-compatible API).
"""
import json
import os
import re
from datetime import datetime

from core.matcher import find_loads, format_loads_list, normalizar_estado, mark_load_accepted
from core import state as state_mgr

# Modelo disponível no workspace Databricks
LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

INTENT_SYSTEM = """Você analisa mensagens de motoristas de caminhão no app Motz.
Extraia a intenção principal e entidades geográficas. Retorne APENAS JSON válido, sem explicações.

Intents possíveis:
- search_load: motorista quer encontrar ou buscar carga (qualquer menção a querer carga, estar disponível, origem, destino, rota — mesmo sem especificar destino)
- accept_load: motorista aceita uma carga da lista mostrada (ex: "quero a 1", "aceito a carga 2", "pode ser a primeira")
- start_trip: motorista informa que saiu / iniciou viagem / foi buscar a carga
- complete_delivery: motorista informa que entregou / chegou no destino
- greeting: saudação pura sem nenhuma menção a carga ou viagem
- other: outra intenção

Retorne JSON com exatamente estes campos:
{
  "intent": "<intent>",
  "origem_cidade": "<cidade ou null>",
  "origem_estado": "<estado por extenso ou sigla ou null>",
  "destino_cidade": "<cidade ou null>",
  "destino_estado": "<estado por extenso ou sigla ou null>",
  "numero_carga": <número inteiro da carga aceita ou null>
}"""

RESPONSE_SYSTEM = """Você é o assistente virtual do app Motz, o Uber dos caminhões.
Responda em português brasileiro, de forma amigável, direta e profissional.
Use o nome do motorista quando souber. Seja conciso. Use emojis com moderação."""


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


def extract_intent(message: str) -> dict:
    """Usa Claude (Databricks) para extrair intenção e entidades da mensagem."""
    client = _get_openai_client()
    response = client.chat.completions.create(
        model=LLM_ENDPOINT,
        max_tokens=300,
        messages=[
            {"role": "system", "content": INTENT_SYSTEM},
            {"role": "user", "content": message},
        ],
    )
    raw = response.choices[0].message.content.strip()
    # Remove blocos de código markdown se presentes
    raw = re.sub(r"```(?:json)?\n?", "", raw).strip("` \n")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {
            "intent": "other",
            "origem_cidade": None, "origem_estado": None,
            "destino_cidade": None, "destino_estado": None,
            "numero_carga": None,
        }


def generate_response(driver: dict, context: dict) -> str:
    """Gera resposta em linguagem natural com base no contexto da ação executada."""
    client = _get_openai_client()
    context_text = json.dumps(context, ensure_ascii=False, indent=2)
    prompt = f"""Motorista: {driver['nome']}
Caminhão: {driver['veiculo']['composicao']} {driver['veiculo']['caracteristica']} ({driver['veiculo']['modelo']})
Capacidade: {driver['veiculo']['capacidade_kg'] / 1000:.0f}t

Situação atual:
{context_text}

Gere uma resposta amigável e direta para o motorista com base na situação acima.
Se houver lista de cargas no contexto, apresente-a de forma clara e organizada.
Se nenhuma carga foi encontrada, sugira ampliar o raio de busca ou verificar outro destino."""

    response = client.chat.completions.create(
        model=LLM_ENDPOINT,
        max_tokens=600,
        messages=[
            {"role": "system", "content": RESPONSE_SYSTEM},
            {"role": "user", "content": prompt},
        ],
    )
    return response.choices[0].message.content.strip()


def process_message(message: str, driver: dict, save_driver_msg: bool = True) -> str:
    """
    Pipeline completo: extrai intenção → executa lógica → gera resposta.
    Atualiza o estado da conversa e retorna a resposta para o motorista.

    save_driver_msg=False quando o chamador já salvou a mensagem do motorista
    (ex: backend de áudio que salva com emoji 🎙️ antes de chamar esta função).
    """
    driver_id = driver["id"]
    conv = state_mgr.get_conversation(driver_id)
    context = conv.get("context", {})

    if save_driver_msg:
        state_mgr.add_message(driver_id, "driver", message)

    # Extrai intenção
    intent_data = extract_intent(message)
    intent = intent_data.get("intent", "other")

    response_context = {"intent": intent}

    if intent == "search_load":
        orig_estado = intent_data.get("origem_estado") or driver.get("localizacao_estado")
        dest_estado = intent_data.get("destino_estado")
        dest_cidade = intent_data.get("destino_cidade")

        loads = find_loads(driver, dest_estado, orig_estado)

        context["origem_estado"] = normalizar_estado(orig_estado) if orig_estado else driver.get("localizacao_estado")
        context["destino_estado"] = normalizar_estado(dest_estado) if dest_estado else None
        context["cargas_oferecidas"] = loads
        context["carga_aceita"] = None

        state_mgr.update_conversation(driver_id, {"status": "searching", "context": context})

        veiculo = f"{driver['veiculo']['composicao']} {driver['veiculo']['caracteristica']}"
        rota = f"{orig_estado or driver.get('localizacao_estado', '?')} → {dest_estado or 'qualquer destino'}"
        response_context.update({
            "veiculo_motorista": veiculo,
            "rota_buscada": rota,
            "cargas_encontradas": len(loads),
            "lista_cargas": format_loads_list(loads) if loads else f"Nenhuma carga disponível para {veiculo} nessa rota no momento.",
        })

    elif intent == "accept_load":
        numero = intent_data.get("numero_carga")
        cargas_oferecidas = context.get("cargas_oferecidas", [])

        if numero and 1 <= numero <= len(cargas_oferecidas):
            carga = cargas_oferecidas[numero - 1]
        elif cargas_oferecidas:
            carga = cargas_oferecidas[0]
        else:
            carga = None

        if carga:
            context["carga_aceita"] = carga
            state_mgr.update_conversation(driver_id, {"status": "matched", "context": context})
            mark_load_accepted(carga["id"], driver_id)
            response_context.update({"carga_aceita": carga, "mensagem": "Carga aceita com sucesso!"})
        else:
            response_context["mensagem"] = "Nenhuma carga disponível para aceitar. Faça uma busca primeiro."

    elif intent == "start_trip":
        carga = context.get("carga_aceita")
        if carga:
            context["viagem_iniciada_em"] = datetime.now().isoformat()
            state_mgr.update_conversation(driver_id, {"status": "in_trip", "context": context})
            response_context.update({"carga": carga, "mensagem": "Viagem iniciada! Boa viagem e segurança na estrada."})
        else:
            response_context["mensagem"] = "Nenhuma carga aceita ainda. Aceite uma carga antes de iniciar a viagem."

    elif intent == "complete_delivery":
        carga = context.get("carga_aceita")
        if carga:
            state_mgr.update_conversation(driver_id, {"status": "delivered", "context": context})
            response_context.update({"carga": carga, "mensagem": "Entrega confirmada! Parabéns pela viagem concluída."})
        else:
            response_context["mensagem"] = "Nenhuma carga em andamento encontrada."

    elif intent == "greeting":
        response_context.update({
            "mensagem": "Saudação recebida.",
            "dica": "O motorista pode dizer onde está e para onde quer ir para buscar cargas.",
        })
    else:
        response_context["mensagem"] = "Mensagem recebida. Não entendi claramente a intenção."

    response_text = generate_response(driver, response_context)
    state_mgr.add_message(driver_id, "assistant", response_text)
    return response_text
