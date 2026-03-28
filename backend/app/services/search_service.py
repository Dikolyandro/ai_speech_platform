import numpy as np
from typing import List, Dict, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Chunk, ChunkEmbedding
from app.services.embedding_service import Embedder


def cosine(a: np.ndarray, b: np.ndarray) -> float:
    # embeddings normalized => dot == cosine
    return float(np.dot(a, b))


class SearchService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.embedder = None
        try:
            # Эмбеддер грузит модель из HF — если нет интернета/модели, не должны падать все запросы.
            self.embedder = Embedder()
        except Exception:
            self.embedder = None

    async def search(self, dataset_id: int, query: str, top_k: int = 10) -> List[Dict[str, Any]]:
        query = (query or "").strip()
        if not query:
            return []

        semantic = await self._semantic_search(dataset_id, query, top_k)
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
        pattern = f"%{query[:100]}%"
        stmt = (
            select(Chunk.id, Chunk.document_id, Chunk.text)
            .where(Chunk.dataset_id == dataset_id)
            .where(Chunk.text.like(pattern))
            .limit(top_k)
        )
        rows = (await self.db.execute(stmt)).all()
        return [{"chunk_id": cid, "document_id": doc_id, "score": 0.1, "text": txt[:400]} for cid, doc_id, txt in rows]
