from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from sqlalchemy.exc import OperationalError

from app.api.v1 import router as api_router
from app.api.v1.routes_import import router as import_router
from app.api.v1.routes_datasets import router as datasets_router
from app.db.session import engine, Base
from sqlalchemy import text


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
        # lightweight migrations for MySQL dev setups (no Alembic yet)
        dialect = (getattr(engine.dialect, "name", None) or "").lower()
        if dialect == "mysql":
            # add/backfill users.nickname if missing (safe for existing rows)
            col = (
                await conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE()
                          AND TABLE_NAME = 'users'
                          AND COLUMN_NAME = 'nickname'
                        """
                    )
                )
            ).scalar()
            if col == 0:
                # 1) add nullable column (no default) to avoid immediate duplicates
                await conn.execute(text("ALTER TABLE `users` ADD COLUMN `nickname` VARCHAR(64) NULL"))
            # 2) backfill nicknames for existing users (id-based to guarantee uniqueness)
            await conn.execute(
                text(
                    "UPDATE `users` "
                    "SET `nickname` = CONCAT('user', `id`) "
                    "WHERE `nickname` IS NULL OR `nickname` = '' OR `nickname` = 'user'"
                )
            )
            # 3) if there are any remaining duplicates, suffix them with _<id>
            await conn.execute(
                text(
                    """
                    UPDATE `users` u
                    JOIN (
                      SELECT nickname, MIN(id) AS keep_id
                      FROM `users`
                      GROUP BY nickname
                      HAVING COUNT(*) > 1
                    ) d
                      ON u.nickname = d.nickname
                    SET u.nickname = CONCAT(u.nickname, '_', u.id)
                    WHERE u.id <> d.keep_id
                    """
                )
            )
            # 4) make it NOT NULL (safe after backfill)
            await conn.execute(text("ALTER TABLE `users` MODIFY COLUMN `nickname` VARCHAR(64) NOT NULL"))

            # ensure unique index exists (after backfill)
            idx = (
                await conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM INFORMATION_SCHEMA.STATISTICS
                        WHERE TABLE_SCHEMA = DATABASE()
                          AND TABLE_NAME = 'users'
                          AND INDEX_NAME = 'ux_users_nickname'
                        """
                    )
                )
            ).scalar()
            if idx == 0:
                await conn.execute(text("ALTER TABLE `users` ADD UNIQUE KEY `ux_users_nickname` (`nickname`)"))
            # add preferred_language if missing
            col_lang = (
                await conn.execute(
                    text(
                        """
                        SELECT COUNT(*)
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE()
                          AND TABLE_NAME = 'users'
                          AND COLUMN_NAME = 'preferred_language'
                        """
                    )
                )
            ).scalar()
            if col_lang == 0:
                await conn.execute(
                    text(
                        "ALTER TABLE `users` "
                        "ADD COLUMN `preferred_language` VARCHAR(2) NOT NULL DEFAULT 'ru'"
                    )
                )
        elif dialect == "sqlite":
            # lightweight migration for existing sqlite users table
            col_lang = (
                await conn.execute(
                    text("SELECT COUNT(*) FROM pragma_table_info('users') WHERE name = 'preferred_language'")
                )
            ).scalar()
            if col_lang == 0:
                await conn.execute(
                    text("ALTER TABLE users ADD COLUMN preferred_language VARCHAR(2) NOT NULL DEFAULT 'ru'")
                )
        try:
            await conn.run_sync(Base.metadata.create_all)
        except OperationalError as exc:
            # Local SQLite may hit duplicate CREATE TABLE during startup.
            if "already exists" not in str(exc):
                raise


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