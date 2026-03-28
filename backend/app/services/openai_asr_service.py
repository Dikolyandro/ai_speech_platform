from openai import OpenAI
from app.core.config import settings

client = None

def _get_client():
    global client
    if client is not None:
        return client
    api_key = (getattr(settings, "OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        return None
    low = api_key.lower()
    if (
        low in ("sk-your-key", "sk-replace-me")
        or "placeholder" in low
        or "replace-me" in low
        or low.startswith("sk-local")
    ):
        return None
    if api_key.startswith("sk-") and len(api_key) < 25:
        return None
    client = OpenAI(api_key=api_key)
    return client

def transcribe(file_path: str, *, language_hint: str | None = None) -> str:
    cl = _get_client()
    if cl is None:
        raise RuntimeError("OPENAI_API_KEY is not set; ASR is disabled")

    kwargs = {}
    hint = (language_hint or "").strip().lower()
    if hint in ("ru", "en", "kk"):
        kwargs["language"] = hint

    with open(file_path, "rb") as f:
        res = cl.audio.transcriptions.create(
            model=settings.OPENAI_ASR_MODEL,  # whisper-1 или gpt-4o-mini-transcribe
            file=f,
            **kwargs,
        )
    return res.text
