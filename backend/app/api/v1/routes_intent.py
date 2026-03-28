from fastapi import APIRouter
from pydantic import BaseModel, Field
from app.services.intent_service import predict_intent

router = APIRouter(prefix="/intent", tags=["intent"])


class IntentRequest(BaseModel):
    text: str = Field(..., min_length=1)


@router.post("/")
async def get_intent(payload: IntentRequest):
    return predict_intent(payload.text)