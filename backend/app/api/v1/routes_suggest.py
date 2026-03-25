from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Literal, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.services.job_transcript import get_transcript_text
from app.services.suggest_service import SuggestService

router = APIRouter(prefix="/query", tags=["Query Suggest"])


class SuggestInput(BaseModel):
    type: Literal["voice", "text"] = "text"
    text: Optional[str] = None
    # ID задачи после POST /api/v1/asr/transcribe (число)
    job_id: Optional[int] = None
    # Устаревший алиас: строка с тем же числом, что и job_id
    audio_id: Optional[str] = None


class SuggestOptions(BaseModel):
    n_suggestions: int = Field(default=8, ge=3, le=20)
    languages: List[str] = Field(default=["ru", "kk"])
    grounding: bool = True
    top_k: int = 5


class SuggestRequest(BaseModel):
    dataset_id: int
    input: SuggestInput
    options: SuggestOptions = SuggestOptions()


@router.post("/suggest")
async def suggest_queries(req: SuggestRequest, db: AsyncSession = Depends(get_db)):
    if req.input.type == "text":
        if not req.input.text:
            raise HTTPException(status_code=400, detail="text is required")
        query_text = req.input.text
        voice_meta = None
    else:
        jid = req.input.job_id
        if jid is None and req.input.audio_id:
            try:
                jid = int(req.input.audio_id.strip())
            except (ValueError, AttributeError):
                raise HTTPException(status_code=400, detail="audio_id must be a numeric job_id")
        if jid is None:
            raise HTTPException(status_code=400, detail="job_id is required for voice (after /asr/transcribe)")
        raw = await get_transcript_text(db, jid)
        query_text = raw
        voice_meta = {"job_id": jid, "transcribed_text": raw}

    svc = SuggestService(db)
    result = await svc.suggest(
        dataset_id=req.dataset_id,
        query_text=query_text,
        languages=req.options.languages,
        n=req.options.n_suggestions,
        grounding=req.options.grounding,
        top_k=req.options.top_k,
    )
    if voice_meta is not None:
        result = {**result, "voice": voice_meta}
    return result
