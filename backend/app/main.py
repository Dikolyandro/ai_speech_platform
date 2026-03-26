from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from app.api.v1 import router as api_router
from app.api.v1.routes_import import router as import_router
from app.api.v1.routes_datasets import router as datasets_router
from app.db.session import engine, Base


load_dotenv()

app = FastAPI(title="AI Speech Platform API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5173",
        "http://localhost:5173",
        "http://127.0.0.1:3000",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Все v1 роуты подключаем одинаково
app.include_router(api_router, prefix="/api/v1")
app.include_router(import_router, prefix="/api/v1")
app.include_router(datasets_router, prefix="/api/v1")


@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@app.get("/")
def root():
    return {
        "service": app.title,
        "docs": "/docs",
        "openapi": "/openapi.json",
        "health": "/health",
        "api_v1": "/api/v1",
    }


@app.get("/health")
def health():
    return {"status": "ok"}