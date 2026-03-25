from fastapi import APIRouter
from . import routes_intent, routes_asr, routes_search, routes_suggest, routes_answer

router = APIRouter()
router.include_router(routes_intent.router)
router.include_router(routes_asr.router)
# router.include_router(routes_search.router)
router.include_router(routes_suggest.router)
router.include_router(routes_answer.router)
