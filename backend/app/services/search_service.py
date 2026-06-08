import os
import re
import numpy as np
from typing import List, Dict, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Chunk, ChunkEmbedding
from app.services.embedding_service import Embedder


_STOP_WORDS = {
    "what", "where", "when", "which", "who", "why", "how", "the", "from", "uploaded",
    "document", "doc", "rag", "please", "tell", "about", "with", "this", "that", "is",
    "are", "was", "were", "какая", "какой", "какие", "что", "где", "когда", "зачем",
    "почему", "документ", "документа", "текст", "скажи", "покажи", "загруженного",
}


def cosine(a: np.ndarray, b: np.ndarray) -> float:
    # embeddings normalized => dot == cosine
    return float(np.dot(a, b))


class SearchService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.embedder = None
        self.embeddings_enabled = os.getenv("DOCUMENT_EMBEDDINGS_ENABLED", "0").strip().lower() in {"1", "true", "yes"}

    async def search(self, dataset_id: int, query: str, top_k: int = 10) -> List[Dict[str, Any]]:
        query = (query or "").strip()
        if not query:
            return []

        semantic = await self._semantic_search(dataset_id, query, top_k) if self.embeddings_enabled else []
        if semantic:
            return semantic

        return await self._lexical_search(dataset_id, query, top_k)

    async def _semantic_search(self, dataset_id: int, query: str, top_k: int) -> List[Dict[str, Any]]:
        # Есть ли embeddings для датасета?
        exists_stmt = (
            select(ChunkEmbedding.chunk_id)
            .join(Chunk, Chunk.id == ChunkEmbedding.chunk_id)
            .where(Chunk.dataset_id == dataset_id)
            .limit(1)
        )
        exists = (await self.db.execute(exists_stmt)).first()
        if not exists:
            return []

        if self.embedder is None:
            try:
                self.embedder = Embedder()
            except Exception:
                return []

        q_vec = self.embedder.encode_one(query)

        stmt = (
            select(Chunk.id, Chunk.document_id, Chunk.text, ChunkEmbedding.vector, ChunkEmbedding.dim)
            .join(ChunkEmbedding, ChunkEmbedding.chunk_id == Chunk.id)
            .where(Chunk.dataset_id == dataset_id)
        )
        rows = (await self.db.execute(stmt)).all()

        scored = []
        for cid, doc_id, text, blob, dim in rows:
            vec = np.frombuffer(blob, dtype=np.float32, count=dim)
            score = cosine(q_vec, vec)
            scored.append((score, cid, doc_id, text))

        scored.sort(key=lambda x: x[0], reverse=True)
        top = scored[:top_k]

        return [
            {"chunk_id": cid, "document_id": doc_id, "score": float(s), "text": txt[:400]}
            for s, cid, doc_id, txt in top
        ]

    async def _lexical_search(self, dataset_id: int, query: str, top_k: int) -> List[Dict[str, Any]]:
        tokens = [
            token.lower()
            for token in re.findall(r"[A-Za-zА-Яа-яЁё0-9_]{3,}", query or "")
            if token.lower() not in _STOP_WORDS
        ]
        if not tokens:
            return []

        stmt = select(Chunk.id, Chunk.document_id, Chunk.text).where(Chunk.dataset_id == dataset_id)
        rows = (await self.db.execute(stmt)).all()
        scored: list[tuple[float, int, int, str]] = []
        q_lower = (query or "").lower()
        for cid, doc_id, txt in rows:
            text_lower = (txt or "").lower()
            score = 0.0
            if q_lower and q_lower in text_lower:
                score += 10.0
            score += sum(1.0 for token in tokens if token in text_lower)
            if score > 0:
                scored.append((score, cid, doc_id, txt))

        scored.sort(key=lambda item: item[0], reverse=True)
        return [
            {"chunk_id": cid, "document_id": doc_id, "score": float(score), "text": txt[:800]}
            for score, cid, doc_id, txt in scored[:top_k]
        ]
