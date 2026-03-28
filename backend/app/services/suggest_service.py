import re
from collections import Counter
from typing import List, Dict, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Chunk
from app.services.search_service import SearchService

RU_STOP = {"и","в","на","по","про","что","где","как","это","для","с","к","из","о","об","а","но","не","да"}
KK_STOP = {"және","мен","үшін","туралы","қалай","қайда","бұл","сол","да","де","не"}
KK_CHARS = set("әғқңөұүһі")


def _norm(text: str) -> str:
    text = (text or "").strip()
    return re.sub(r"\s+", " ", text)


def detect_lang(text: str) -> str:
    t = (text or "").lower()
    kk_hits = sum(1 for ch in t if ch in KK_CHARS)
    return "kk" if kk_hits >= 2 else "ru"


def tokenize(text: str) -> List[str]:
    t = re.sub(r"[^0-9a-zA-Zа-яА-Яәғқңөұүһі ]+", " ", (text or "").lower())
    return [w for w in t.split() if len(w) >= 3]


def extract_keywords(text: str, lang: str) -> List[str]:
    toks = tokenize(text)
    stop = KK_STOP if lang == "kk" else RU_STOP
    kws = [w for w in toks if w not in stop]
    out, seen = [], set()
    for w in kws:
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out[:6]


def build_vocab(chunks_texts: List[str], lang: str, top_n: int = 30) -> List[str]:
    stop = KK_STOP if lang == "kk" else RU_STOP
    all_words = []
    for c in chunks_texts:
        for w in tokenize(c):
            if w not in stop:
                all_words.append(w)
    cnt = Counter(all_words)
    return [w for w, _ in cnt.most_common(top_n)]


def generate_variants(query_text: str, lang: str, vocab: List[str]) -> List[Dict[str, str]]:
    kws = extract_keywords(query_text, lang)
    base = " ".join(kws) if kws else query_text

    variants: List[Dict[str, str]] = []

    def add(txt: str, vtype: str):
        txt = _norm(txt)
        if len(txt) >= 3:
            variants.append({"text": txt, "lang": lang, "type": vtype})

    add(query_text, "original")
    add(base, "short")

    if lang == "ru":
        add(f"условия {base}", "clarifying")
        add(f"как {base}", "question")
        add(f"{base} сроки", "expand")
        add(f"{base} правила", "expand")
    else:
        add(f"{base} шарттары", "clarifying")
        add(f"{base} қалай", "question")
        add(f"{base} мерзімі", "expand")
        add(f"{base} ережелері", "expand")

    for term in vocab[:10]:
        if term and term not in base:
            add(f"{base} {term}", "dataset_term")

    return variants


def dedupe(vars_: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for v in vars_:
        key = (v["lang"], v["text"])
        if key not in seen:
            seen.add(key)
            out.append(v)
    return out[:limit]


class SuggestService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def _load_chunk_texts(self, dataset_id: int, limit: int = 400) -> List[str]:
        stmt = select(Chunk.text).where(Chunk.dataset_id == dataset_id).limit(limit)
        rows = (await self.db.execute(stmt)).all()
        return [r[0] for r in rows]

    async def suggest(
        self,
        dataset_id: int,
        query_text: str,
        languages: List[str],
        preferred_language: str = "ru",
        n: int = 8,
        grounding: bool = True,
        top_k: int = 5,
    ) -> Dict[str, Any]:
        query_text = _norm(query_text)
        selected = (preferred_language or "ru").strip().lower()
        if selected not in ("ru", "en", "kk"):
            selected = "ru"

        chunk_texts = await self._load_chunk_texts(dataset_id)
        suggestions: List[Dict[str, Any]] = []

        langs = [selected]
        for lang in langs:
            vocab = build_vocab(chunk_texts, lang=lang, top_n=30) if chunk_texts else []
            suggestions.extend(generate_variants(query_text, lang=lang, vocab=vocab))

        suggestions = dedupe(suggestions, limit=max(n * 3, 20))

        # базовое ранжирование
        for s in suggestions:
            score = 0.65 if s["lang"] == selected else 0.50
            if s["type"] == "dataset_term":
                score += 0.05
            s["score"] = round(min(score, 0.99), 4)

        # grounding: проверяем, “цепляется” ли вариант к датасету
        if grounding:
            search = SearchService(self.db)
            for s in suggestions:
                hits = await search.search(dataset_id=dataset_id, query=s["text"], top_k=top_k)
                if not hits:
                    s["grounding"] = {"enabled": True, "hit": False, "max_score": 0.0, "matches": 0}
                    s["score"] = round(max(s["score"] - 0.10, 0.0), 4)
                else:
                    max_score = max(h["score"] for h in hits)
                    s["grounding"] = {
                        "enabled": True,
                        "hit": True,
                        "max_score": float(max_score),
                        "matches": len(hits),
                        "top_doc_id": hits[0]["document_id"],
                    }
                    s["score"] = round(min(s["score"] + 0.20, 0.99), 4)

        suggestions = sorted(suggestions, key=lambda x: x.get("score", 0), reverse=True)[:n]

        return {
            "dataset_id": dataset_id,
            "selected_language": selected,
            "suggestions": suggestions,
        }
