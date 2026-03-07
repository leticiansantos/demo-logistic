"""
Transcrição de áudio via Databricks Model Serving (system.ai.whisper_large_v3).

Chama o endpoint /invocations diretamente com o áudio em base64,
no formato MLflow: {"instances": ["<base64>"]}

A resposta tem formato: {"predictions": ["texto transcrito"]}
"""
import base64
import json
import urllib.request
import urllib.error


WHISPER_ENDPOINT = "whisper-large-v3"


def transcribe(audio_bytes: bytes, filename: str = "audio.ogg") -> str:
    """Transcreve áudio usando o endpoint Whisper no Databricks Model Serving."""
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as e:
        raise RuntimeError(f"Pacote não instalado: {e}. Execute: pip install databricks-sdk")

    w = WorkspaceClient()
    host = w.config.host.rstrip("/")
    token = w.config.token

    audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
    payload = json.dumps({"instances": [audio_b64]}).encode("utf-8")

    url = f"{host}/serving-endpoints/{WHISPER_ENDPOINT}/invocations"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read().decode("utf-8"))
        predictions = result.get("predictions") or []
        text = predictions[0] if predictions else ""
        return text.strip()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Erro ao chamar endpoint Whisper '{WHISPER_ENDPOINT}': HTTP {e.code}\n{body}"
        )
    except Exception as e:
        raise RuntimeError(f"Erro ao transcrever áudio: {e}")
