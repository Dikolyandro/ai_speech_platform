from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parents[2]  # backend/
ENV_PATH = BASE_DIR / ".env"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_PATH),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    DATABASE_URL: str
    OPENAI_API_KEY: str
    OPENAI_ASR_MODEL: str = "gpt-4o-mini-transcribe"
    ASR_TIMEOUT_SECONDS: int = 60
    ASR_MAX_AUDIO_BYTES: int = 10 * 1024 * 1024
    LOCAL_AUDIO_DIR: str

    # Auth (JWT)
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7

    # Email verification (optional in development; code is logged if SMTP is not configured)
    SMTP_HOST: str | None = None
    SMTP_PORT: int = 587
    SMTP_USERNAME: str | None = None
    SMTP_PASSWORD: str | None = None
    SMTP_FROM_EMAIL: str | None = None
    SMTP_FROM_NAME: str = "AI Analytics Assistant"
    SMTP_USE_TLS: bool = True

    # Папка с весами intent (tokenizer + config + model.safetensors). По умолчанию — app/models/intent1.0
    INTENT_MODEL_DIR: str | None = None
    # Порог softmax для intent; ниже — в ответе может быть fallback / эвристика
    INTENT_CONFIDENCE_THRESHOLD: float = 0.55

# ⚠️ ВАЖНО: ЭТА СТРОКА ОБЯЗАТЕЛЬНО ДОЛЖНА БЫТЬ
settings = Settings()
