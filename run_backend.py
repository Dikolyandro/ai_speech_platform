"""
Запуск API из корня распакованного архива (где лежит папка backend/).

Пример:
  py -3 run_backend.py
  py -3 run_backend.py --reload

Не нужно вручную делать cd в backend — путь к пакету app настраивается здесь.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    candidates = [root / "backend", root / "ai_speech_platform-main" / "backend"]
    backend = next((p for p in candidates if p.is_dir()), None)
    if backend is None:
        raise SystemExit(
            "Не найдена папка backend. Ожидается одна из:\n"
            f"  {candidates[0]}\n"
            f"  {candidates[1]}"
        )

    sys.path.insert(0, str(backend))
    os.chdir(backend)

    import uvicorn  # noqa: PLC0415 — после chdir

    uvicorn.run(
        "app.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
