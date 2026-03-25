from __future__ import annotations

import re
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import engine, get_db
from app.services.intent_service import predict_intent
from app.services.job_transcript import get_transcript_text
from app.services.query_semantics import resolve_metric_and_group

router = APIRouter(prefix="/query", tags=["Query Answer"])


# ---------- Schemas ----------
class QueryInput(BaseModel):
    type: str = "text"  # "text" | "voice"
    text: Optional[str] = None
    # После POST /api/v1/asr/transcribe передайте job_id — текст подтянется из БД
    job_id: Optional[int] = None


class AnswerOptions(BaseModel):
    limit: int = 20
    explain: bool = True
    confidence_threshold: float = 0.55  # порог уверенности ML


class AnswerRequest(BaseModel):
    dataset_id: int
    input: QueryInput
    options: AnswerOptions = AnswerOptions()


# ---------- Helpers ----------
RU_KK_TOP = ("топ", "top", "ең көп", "ең жоғары", "көбірек")
RU_KK_SUM = ("сумма", "sum", "итого", "жалпы", "барлығы")
RU_KK_AVG = ("среднее", "average", "avg", "орташа")
RU_KK_COUNT = ("сколько", "count", "сан", "қанша")

def _normalize(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


async def _get_table_columns(db: AsyncSession, table_name: str) -> list[tuple[str, str]]:
    # защита (чтобы никто не подставил произвольное имя таблицы)
    if not re.fullmatch(r"ds_\d+_data", table_name):
        raise HTTPException(status_code=400, detail="invalid table name")

    # async URLs: sqlite+aiosqlite, mysql+aiomysql — drivername не всегда "sqlite"
    dialect = (getattr(engine.dialect, "name", None) or "").lower()
    durl = (engine.url.drivername or "").lower()
    if dialect == "sqlite" or durl == "sqlite" or durl.startswith("sqlite+"):
        q = text(f'PRAGMA table_info("{table_name}")')
        res = await db.execute(q)
        rows = res.fetchall()
        return [(r[1], (r[2] or "")) for r in rows]

    q = text(
        """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :tname
        ORDER BY ORDINAL_POSITION
        """
    )
    res = await db.execute(q, {"tname": table_name})
    rows = res.fetchall()
    return [(r[0], (r[1] or "")) for r in rows]


def _detect_operation_heuristic(query: str) -> str:
    q = query
    if any(k in q for k in RU_KK_TOP):
        return "top"
    if any(k in q for k in RU_KK_AVG):
        return "avg"
    if any(k in q for k in RU_KK_SUM):
        return "sum"
    if any(k in q for k in RU_KK_COUNT):
        return "count"
    return "select"


def _pick_year_filter(cols: list[tuple[str, str]], query: str) -> Optional[tuple[str, int]]:
    col_names = [c[0] for c in cols]
    m = re.search(r"\b(19\d{2}|20\d{2})\b", query)
    if not m:
        return None
    year = int(m.group(1))

    for cand in ("year", "yyyy", "год", "жыл"):
        for name in col_names:
            if name.lower() == cand:
                return (name, year)
    return None


# ---------- Main endpoint ----------
@router.post("/answer")
async def answer_query(req: AnswerRequest, db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    transcribed_raw: Optional[str] = None
    if req.input.job_id is not None:
        transcribed_raw = await get_transcript_text(db, req.input.job_id)
        qtext = _normalize(transcribed_raw)
    else:
        qtext = _normalize(req.input.text or "")
    if not qtext:
        raise HTTPException(
            status_code=400,
            detail="empty query: provide input.text or upload audio and pass input.job_id",
        )

    # 3) table + schema (раньше intent — нужны колонки для семантики; intent ниже по q_lex)
    table_name = f"ds_{req.dataset_id}_data"
    cols = await _get_table_columns(db, table_name)
    if not cols:
        raise HTTPException(status_code=404, detail=f"table {table_name} not found or has no columns")

    sem = resolve_metric_and_group(cols, qtext)
    q_lex = sem["query_lexical"]
    metric_col = sem["metric_col"]
    group_col = sem["group_col"]
    semantic_meta = {
        "query_normalized": qtext,
        "query_after_lexical_fixes": q_lex,
        "column_bindings": sem["bindings"],
        "resolved_metric": metric_col,
        "resolved_group_by": group_col,
    }

    # 1) Intent from ML — на тексте после лексических правок (ASR → «amount» и т.д.)
    intent_result = predict_intent(q_lex)
    # ожидаем формат: {"intent": "...", "confidence": 0.0..1.0, "top_k": [...]}
    ml_intent = intent_result.get("intent", "fallback")
    ml_conf = float(intent_result.get("confidence", 0.0))

    # 2) Fallback logic: если низкая уверенность — используем эвристику
    threshold = float(req.options.confidence_threshold)
    operation = ml_intent if (ml_intent != "fallback" and ml_conf >= threshold) else _detect_operation_heuristic(q_lex)

    # fallback group_by for TOP queries
    col_set = {c[0] for c in cols}
    if operation == "top" and not group_col:
        for cand in ("category", "type", "status"):
            if cand in col_set:
                group_col = cand
                semantic_meta["resolved_group_by"] = group_col
                semantic_meta["column_bindings"]["group_by"] = {
                    "column": cand,
                    "method": "default_for_top",
                    "detail": "в запросе не указано измерение; взята первая из category/type/status",
                }
                break

    year_filter = _pick_year_filter(cols, q_lex)

    limit = max(1, min(int(req.options.limit), 200))

    # 4) safe identifiers check
    if metric_col and metric_col not in col_set:
        metric_col = None
    if group_col and group_col not in col_set:
        group_col = None

    where_sql = ""
    params: dict[str, Any] = {}
    if year_filter:
        ycol, yval = year_filter
        if ycol in col_set:
            where_sql = f" WHERE `{ycol}` = :year "
            params["year"] = yval

    interpreted: dict[str, Any] = {
        "table": table_name,
        "operation": operation,
        "metric": metric_col,
        "group_by": [group_col] if group_col else [],
        "filter": {"year": params.get("year")} if "year" in params else {},
        "limit": limit,
        "intent": intent_result,
        "confidence_threshold": threshold,
        "used_ml_intent": (ml_intent != "fallback" and ml_conf >= threshold),
        "semantic": semantic_meta,
    }

    # 5) SQL build
    sql = ""

    # Если "top" — делаем SUM(metric) по group_by (если metric есть),
    # иначе COUNT(*) по group_by.
    if group_col and operation in ("top", "sum", "avg", "count"):
        if operation == "count" or (operation == "top" and not metric_col):
            agg_expr = "COUNT(*)"
            alias = "count"
        elif operation == "avg":
            if not metric_col:
                raise HTTPException(status_code=400, detail="cannot avg without numeric metric column")
            agg_expr = f"AVG(`{metric_col}`)"
            alias = "avg"
        else:
            if not metric_col:
                raise HTTPException(status_code=400, detail="cannot sum/top without numeric metric column")
            agg_expr = f"SUM(`{metric_col}`)"
            alias = "sum"

        order_sql = f" ORDER BY `{alias}` DESC " if operation == "top" else ""
        sql = f"""
            SELECT `{group_col}` AS group_key, {agg_expr} AS `{alias}`
            FROM `{table_name}`
            {where_sql}
            GROUP BY `{group_col}`
            {order_sql}
            LIMIT {limit}
        """
    else:
        # plain rows
        sql = f"SELECT * FROM `{table_name}` {where_sql} LIMIT {limit}"

    res = await db.execute(text(sql), params)
    rows = [dict(r._mapping) for r in res.fetchall()]

    # 6) Answer text
    if rows:
        if operation == "top" and group_col:
            if metric_col:
                answer_text = f"Топ значений по '{group_col}' по сумме '{metric_col}' (лимит {limit})."
            else:
                answer_text = f"Топ значений по '{group_col}' по количеству записей (лимит {limit})."
        elif operation == "sum" and group_col:
            answer_text = f"Сумма по '{metric_col}' сгруппирована по '{group_col}'."
        elif operation == "avg" and group_col:
            answer_text = f"Среднее по '{metric_col}' сгруппировано по '{group_col}'."
        elif operation == "count" and group_col:
            answer_text = f"Количество записей сгруппировано по '{group_col}'."
        else:
            answer_text = "Найдены результаты по вашему запросу."
    else:
        answer_text = "Ничего не найдено для вашего запроса."

    out: dict[str, Any] = {
        "dataset_id": req.dataset_id,
        "interpreted": interpreted,
        "rows": rows,
        "answer_text": answer_text,
    }
    if req.options.explain:
        out["sql"] = re.sub(r"\s+", " ", sql).strip()
    if transcribed_raw is not None:
        out["voice"] = {
            "job_id": req.input.job_id,
            "transcribed_text": transcribed_raw,
        }

    return out
