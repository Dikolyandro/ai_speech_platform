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

    # Папка с весами intent (tokenizer + config + model.safetensors). По умолчанию — app/models/intent_model
    INTENT_MODEL_DIR: str | None = None

# ⚠️ ВАЖНО: ЭТА СТРОКА ОБЯЗАТЕЛЬНО ДОЛЖНА БЫТЬ
settings = Settings()
