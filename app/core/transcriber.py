"""
Transcrição de áudio via Databricks Model Serving.

Databricks Foundation Model APIs não incluem Whisper nativamente.
Para habilitar transcrição de áudio real, faça o deploy de um endpoint
Whisper no seu workspace (ex: openai/whisper-large-v3 via MLflow).

Por enquanto, esta função levanta um erro claro com instruções.
O simulador WhatsApp funciona 100% via entrada de texto.
"""


WHISPER_ENDPOINT = "whisper-large-v3"  # nome do endpoint customizado, se existir


def transcribe(audio_bytes: bytes, filename: str = "audio.ogg") -> str:
    """
    Tenta transcrever áudio usando um endpoint Whisper no Databricks.
    Se o endpoint não existir, orienta o usuário.
    """
    try:
        from databricks.sdk import WorkspaceClient
        from openai import OpenAI, NotFoundError
    except ImportError as e:
        raise RuntimeError(f"Pacote não instalado: {e}. Execute: pip install databricks-sdk openai")

    w = WorkspaceClient()
    client = OpenAI(
        api_key=w.config.token,
        base_url=f"{w.config.host}/serving-endpoints",
    )

    try:
        transcription = client.audio.transcriptions.create(
            model=WHISPER_ENDPOINT,
            file=(filename, audio_bytes),
            language="pt",
        )
        return transcription.text.strip()
    except Exception as e:
        raise RuntimeError(
            f"Endpoint Whisper '{WHISPER_ENDPOINT}' não encontrado ou indisponível.\n\n"
            "Para habilitar transcrição de áudio:\n"
            "1. Faça deploy de openai/whisper-large-v3 no Databricks Model Serving\n"
            "2. Nomeie o endpoint como 'whisper-large-v3'\n"
            "3. Ou use a entrada de texto no simulador (funciona sem áudio).\n\n"
            f"Detalhe do erro: {e}"
        )
