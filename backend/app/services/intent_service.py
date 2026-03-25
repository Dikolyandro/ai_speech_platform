from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import json
import re
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

from app.core.config import settings


@dataclass
class IntentPrediction:
    intent: str
    confidence: float
    probs: Dict[str, float]


RU_KK_TOP = ("топ", "top", "ең көп", "ең жоғары", "көбірек")
RU_KK_SUM = ("сумма", "sum", "итого", "жалпы", "барлығы")
RU_KK_AVG = ("среднее", "average", "avg", "орташа")
RU_KK_COUNT = ("сколько", "count", "сан", "қанша")


def _normalize(s: str) -> str:
    s = (s or "").strip().lower()
    return re.sub(r"\s+", " ", s)


def _heuristic_intent_and_probs(text: str, threshold: float) -> IntentPrediction:
    """
    Запасной режим, чтобы backend работал даже когда весов intent-модели нет
    (или нет интернета для скачивания).
    """
    q = _normalize(text)
    ops = ["top", "sum", "avg", "count", "select"]

    intent = "select"
    confidence = 0.4  # ниже threshold по умолчанию, чтобы ответ всё равно мог перейти к эвристике
    if any(k in q for k in RU_KK_TOP):
        intent, confidence = "top", 0.9
    elif any(k in q for k in RU_KK_AVG):
        intent, confidence = "avg", 0.85
    elif any(k in q for k in RU_KK_SUM):
        intent, confidence = "sum", 0.85
    elif any(k in q for k in RU_KK_COUNT):
        intent, confidence = "count", 0.85

    # Простое распределение вероятностей (для совместимости API)
    remaining = max(0.0, 1.0 - confidence)
    other = remaining / (len(ops) - 1)
    probs = {op: (confidence if op == intent else other) for op in ops}

    if confidence < threshold:
        return IntentPrediction(intent=intent, confidence=confidence, probs=probs)
    return IntentPrediction(intent=intent, confidence=confidence, probs=probs)


class IntentService:
    def __init__(
        self,
        model_dir: str,
        threshold: float = 0.55,
    ):
        self.model_dir = Path(model_dir)
        self.threshold = float(threshold)
        self.tokenizer = None
        self.model = None
        self.label_map = None
        self._use_heuristic = True

        weights = list(self.model_dir.glob("*.safetensors")) + list(self.model_dir.glob("pytorch_model.bin"))

        # Частый кейс: модель лежит во вложенной папке (например, intent_model/intent_model/)
        if not weights and self.model_dir.exists():
            for child in self.model_dir.iterdir():
                if not child.is_dir():
                    continue
                w_child = list(child.glob("*.safetensors")) + list(child.glob("pytorch_model.bin"))
                if w_child:
                    self.model_dir = child
                    weights = w_child
                    break

        if not weights:
            # без весов работаем по эвристике
            return

        self._use_heuristic = False

        self.tokenizer = AutoTokenizer.from_pretrained(self.model_dir)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_dir)
        self.model.eval()

        label_map_path = self.model_dir / "label_map.json"
        if label_map_path.exists():
            with open(label_map_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict) and "classes" in raw and isinstance(raw["classes"], list):
                self.label_map = {str(i): str(name) for i, name in enumerate(raw["classes"])}
            elif isinstance(raw, dict):
                self.label_map = raw
            else:
                self.label_map = None

    def predict(self, text: str) -> IntentPrediction:
        text = (text or "").strip()
        if not text:
            return IntentPrediction(intent="fallback", confidence=0.0, probs={})

        if self.model is None or self.tokenizer is None:
            # В этой ветке weights не загрузились — возвращаем эвристический intent.
            return _heuristic_intent_and_probs(text=text, threshold=self.threshold)

        # Если запрос содержит явные ключевые слова операции,
        # эвристика обычно надёжнее (и спасает от неверного маппинга label->id2label).
        q = _normalize(text)
        has_kw_intent = (
            any(k in q for k in RU_KK_TOP)
            or any(k in q for k in RU_KK_AVG)
            or any(k in q for k in RU_KK_SUM)
            or any(k in q for k in RU_KK_COUNT)
        )

        inputs = self.tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            padding=True,
        )

        with torch.no_grad():
            outputs = self.model(**inputs)
            logits = outputs.logits
            probs_tensor = torch.softmax(logits, dim=-1)[0]

        probs_list = probs_tensor.tolist()
        best_idx = int(torch.argmax(probs_tensor).item())
        confidence = float(probs_list[best_idx])

        if self.label_map:
            # поддержка label_map вида {"0": "get_tasks", "1": "get_docs"}
            intent = self.label_map.get(str(best_idx), str(best_idx))
            probs = {
                self.label_map.get(str(i), str(i)): float(p)
                for i, p in enumerate(probs_list)
            }
        else:
            # fallback: берем labels из config модели
            id2label = getattr(self.model.config, "id2label", None) or {}
            intent = id2label.get(best_idx, str(best_idx))
            probs = {
                id2label.get(i, str(i)): float(p)
                for i, p in enumerate(probs_list)
            }

        if confidence < self.threshold:
            return IntentPrediction(intent="fallback", confidence=confidence, probs=probs)

        if has_kw_intent:
            heur = _heuristic_intent_and_probs(text=text, threshold=self.threshold)
            # При явных ключах используем эвристику, если ML не совпал.
            if heur.intent != "select" and heur.intent != intent:
                return heur

        return IntentPrediction(intent=intent, confidence=confidence, probs=probs)


def _default_model_dir() -> Path:
    if settings.INTENT_MODEL_DIR:
        return Path(settings.INTENT_MODEL_DIR).expanduser().resolve()
    return Path(__file__).resolve().parents[1] / "models" / "intent_model"


DEFAULT_MODEL_DIR = _default_model_dir()

_intent_service: Optional[IntentService] = None


def get_intent_service() -> IntentService:
    global _intent_service
    if _intent_service is None:
        _intent_service = IntentService(
            model_dir=str(DEFAULT_MODEL_DIR),
            threshold=0.55,
        )
    return _intent_service


def predict_intent(text: str) -> Dict[str, Any]:
    svc = get_intent_service()
    pred = svc.predict(text)
    return {
        "intent": pred.intent,
        "confidence": pred.confidence,
        "probs": pred.probs,
        "threshold": svc.threshold,
    }