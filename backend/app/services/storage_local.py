import os
from app.core.config import settings

def ensure_dir():
    os.makedirs(settings.LOCAL_AUDIO_DIR, exist_ok=True)

def save_bytes(file_bytes: bytes, filename: str) -> str:
    ensure_dir()
    path = os.path.join(settings.LOCAL_AUDIO_DIR, filename)
    with open(path, "wb") as f:
        f.write(file_bytes)
    return path
