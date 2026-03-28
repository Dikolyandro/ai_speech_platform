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
    LOCAL_AUDIO_DIR: str

    # Auth (JWT)
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 7

    # Папка с весами intent (tokenizer + config + model.safetensors). По умолчанию — app/models/intent1.0
    INTENT_MODEL_DIR: str | None = None
    # Порог softmax для intent; ниже — в ответе может быть fallback / эвристика
    INTENT_CONFIDENCE_THRESHOLD: float = 0.55

# ⚠️ ВАЖНО: ЭТА СТРОКА ОБЯЗАТЕЛЬНО ДОЛЖНА БЫТЬ
settings = Settings()
