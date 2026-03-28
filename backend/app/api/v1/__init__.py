from fastapi import APIRouter
from . import routes_answer, routes_asr, routes_auth, routes_chat, routes_intent, routes_saved_queries, routes_suggest

router = APIRouter()
router.include_router(routes_auth.router)
router.include_router(routes_intent.router)
router.include_router(routes_asr.router)
router.include_router(routes_suggest.router)
router.include_router(routes_answer.router)
router.include_router(routes_chat.router)
router.include_router(routes_saved_queries.router)
