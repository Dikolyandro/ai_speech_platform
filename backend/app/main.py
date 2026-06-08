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
            user_email_verification_columns = {
                "is_email_verified": "ALTER TABLE `users` ADD COLUMN `is_email_verified` BOOLEAN NOT NULL DEFAULT TRUE",
                "email_verification_code_hash": "ALTER TABLE `users` ADD COLUMN `email_verification_code_hash` VARCHAR(255) NULL",
                "email_verification_expires_at": "ALTER TABLE `users` ADD COLUMN `email_verification_expires_at` DATETIME NULL",
                "email_verification_attempts": "ALTER TABLE `users` ADD COLUMN `email_verification_attempts` INT NOT NULL DEFAULT 0",
            }
            for column_name, ddl in user_email_verification_columns.items():
                col_exists = (
                    await conn.execute(
                        text(
                            """
                            SELECT COUNT(*)
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = DATABASE()
                              AND TABLE_NAME = 'users'
                              AND COLUMN_NAME = :column_name
                            """
                        ),
                        {"column_name": column_name},
                    )
                ).scalar()
                if col_exists == 0:
                    await conn.execute(text(ddl))
            privacy_columns = {
                ("datasets", "is_private"): "ALTER TABLE `datasets` ADD COLUMN `is_private` BOOLEAN NOT NULL DEFAULT TRUE",
                ("bigdata_datasets", "is_private"): "ALTER TABLE `bigdata_datasets` ADD COLUMN `is_private` BOOLEAN NOT NULL DEFAULT TRUE",
                ("saved_queries", "custom_title"): "ALTER TABLE `saved_queries` ADD COLUMN `custom_title` VARCHAR(255) NULL",
            }
            for (table_name, column_name), ddl in privacy_columns.items():
                col_exists = (
                    await conn.execute(
                        text(
                            """
                            SELECT COUNT(*)
                            FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = DATABASE()
                              AND TABLE_NAME = :table_name
                              AND COLUMN_NAME = :column_name
                            """
                        ),
                        {"table_name": table_name, "column_name": column_name},
                    )
                ).scalar()
                if col_exists == 0:
                    await conn.execute(text(ddl))
        elif dialect == "sqlite":
            # lightweight migration for existing sqlite users table (skip if DB is fresh)
            users_tbl = (
                await conn.execute(
                    text("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='users'")
                )
            ).scalar()
            if users_tbl:
                col_lang = (
                    await conn.execute(
                        text("SELECT COUNT(*) FROM pragma_table_info('users') WHERE name = 'preferred_language'")
                    )
                ).scalar()
                if col_lang == 0:
                    await conn.execute(
                        text("ALTER TABLE users ADD COLUMN preferred_language VARCHAR(2) NOT NULL DEFAULT 'ru'")
                    )
                user_email_verification_columns = {
                    "is_email_verified": "ALTER TABLE users ADD COLUMN is_email_verified BOOLEAN NOT NULL DEFAULT 1",
                    "email_verification_code_hash": "ALTER TABLE users ADD COLUMN email_verification_code_hash VARCHAR(255) NULL",
                    "email_verification_expires_at": "ALTER TABLE users ADD COLUMN email_verification_expires_at DATETIME NULL",
                    "email_verification_attempts": "ALTER TABLE users ADD COLUMN email_verification_attempts INTEGER NOT NULL DEFAULT 0",
                }
                for column_name, ddl in user_email_verification_columns.items():
                    col_exists = (
                        await conn.execute(
                            text("SELECT COUNT(*) FROM pragma_table_info('users') WHERE name = :column_name"),
                            {"column_name": column_name},
                        )
                    ).scalar()
                    if col_exists == 0:
                        await conn.execute(text(ddl))
                privacy_columns = {
                    ("datasets", "is_private"): "ALTER TABLE datasets ADD COLUMN is_private BOOLEAN NOT NULL DEFAULT 1",
                    ("bigdata_datasets", "is_private"): "ALTER TABLE bigdata_datasets ADD COLUMN is_private BOOLEAN NOT NULL DEFAULT 1",
                    ("saved_queries", "custom_title"): "ALTER TABLE saved_queries ADD COLUMN custom_title VARCHAR(255) NULL",
                }
                for (table_name, column_name), ddl in privacy_columns.items():
                    table_exists = (
                        await conn.execute(
                            text("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = :table_name"),
                            {"table_name": table_name},
                        )
                    ).scalar()
                    if not table_exists:
                        continue
                    col_exists = (
                        await conn.execute(
                            text(f"SELECT COUNT(*) FROM pragma_table_info('{table_name}') WHERE name = :column_name"),
                            {"column_name": column_name},
                        )
                    ).scalar()
                    if col_exists == 0:
                        await conn.execute(text(ddl))
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
