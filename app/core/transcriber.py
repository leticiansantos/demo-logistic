"""
Transcrição de áudio via Databricks Model Serving.

Endpoints suportados:
  - faster-whisper-large-v3: payload {"dataframe_records": [{"instances": "<b64>"}]}
  - whisper-large-v3:        payload {"instances": ["<b64>"]}

Resposta: {"predictions": ["texto"]} ou {"predictions": {"0": "texto"}}
"""
import base64


WHISPER_ENDPOINT = "faster-whisper-large-v3"

# Endpoints que usam o formato legado {"instances": [b64]}
_LEGACY_INSTANCES_ENDPOINTS = {"whisper-large-v3"}


def transcribe(audio_bytes: bytes, filename: str = "audio.wav", endpoint_override: str | None = None) -> str:
    """Transcreve áudio usando o endpoint Whisper no Databricks Model Serving."""
    endpoint = endpoint_override or WHISPER_ENDPOINT

    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as e:
        raise RuntimeError(f"Pacote não instalado: {e}. Execute: pip install databricks-sdk")

    w = WorkspaceClient()
    audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")

    if endpoint in _LEGACY_INSTANCES_ENDPOINTS:
        body = {"instances": [audio_b64]}
    else:
        body = {"dataframe_records": [{"instances": audio_b64}]}

    try:
        result = w.api_client.do(
            "POST",
            f"/serving-endpoints/{endpoint}/invocations",
            body=body,
        )
    except Exception as e:
        raise RuntimeError(f"Erro ao chamar endpoint '{endpoint}': {type(e).__name__}: {e}")

    predictions = result.get("predictions")

    # Aceita lista ["texto"] ou dict {"0": "texto"}
    if isinstance(predictions, list):
        text = predictions[0] if predictions else ""
    elif isinstance(predictions, dict):
        text = predictions.get("0") or next(iter(predictions.values()), "")
    else:
        text = str(predictions) if predictions is not None else ""

    if isinstance(text, dict):
        text = text.get("text") or str(text)

    return str(text).strip()
