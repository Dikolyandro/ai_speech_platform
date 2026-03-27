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
from app.services.query_filters import (
    build_dynamic_filters_and_order,
    should_use_scalar_count_instead_of_group,
)
from app.services.query_semantics import _is_numeric_sql_type, resolve_metric_and_group
from app.services.profile_semantic_resolver import (
    compile_profile_filters_to_sql,
    resolve_profile_value_filters,
)
from app.auth.security import get_current_user
from app.db.models import Dataset, DatasetTableMeta, User
from sqlalchemy import select
from app.services.i18n_service import normalize_preferred_language, validate_query_language

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
_OP_KEYWORDS = {
    "ru": {
        "top": ("топ", "максимум", "минимум", "наибольш", "наименьш"),
        "sum": ("сумма", "итого", "всего"),
        "avg": ("среднее",),
        "count": ("сколько", "количество", "кол-во", "число"),
    },
    "en": {
        "top": ("top", "max", "min", "maximum", "minimum"),
        "sum": ("sum", "total"),
        "avg": ("average", "avg", "mean"),
        "count": ("count", "how many", "number of"),
    },
    "kk": {
        "top": ("топ", "ең көп", "ең жоғары", "максимум", "минимум"),
        "sum": ("жалпы", "барлығы", "қосынды"),
        "avg": ("орташа",),
        "count": ("сан", "қанша"),
    },
}

def _normalize(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _dedupe_voice_transcript(raw: str) -> str:
    """Убирает типичный мусор ASR: вся фраза продублирована подряд второй раз."""
    t = (raw or "").strip()
    if len(t) > 24:
        h = len(t) // 2
        if t[:h] == t[h:]:
            t = t[:h].strip()
    return t


def _query_has_numeric_comparison(q: str) -> bool:
    """Чтобы «quantity меньше 10» не превращалось в LIMIT 10."""
    return bool(
        re.search(r"\b(?:больше|меньше|не\s+больше|не\s+меньше)\s+\d", q, re.IGNORECASE)
    ) or bool(re.search(r"\b\w+\s*(?:>|<|>=|<=)\s*\d", q))


def _comparison_demands_filtered_rows(q_lex: str) -> bool:
    """«строки где quantity > 5» — всегда выборка строк, не TOP по категориям."""
    if not _query_has_numeric_comparison(q_lex):
        return False
    return bool(
        re.search(
            r"\b(где|where|строк|запис|покажи|show|display|выведи|которые)\b",
            q_lex,
            re.IGNORECASE,
        )
    )


def _cols_by_lower(cols: list[tuple[str, str]]) -> dict[str, str]:
    return {c[0].lower(): c[0] for c in cols}


def _numeric_col_names(cols: list[tuple[str, str]]) -> set[str]:
    return {c[0] for c in cols if _is_numeric_sql_type(c[1] or "")}


_PO_SKIP_ORDER = frozenset(
    {
        "убыванию",
        "убывающему",
        "возрастанию",
        "возрастающему",
        "каналу",
        "каналам",
        "городу",
        "категории",
        "менеджеру",
        "каждому",
        "каждой",
        "каждым",
    }
)


def _resolve_top_n_or_extreme_row_ranking(
    q_lex: str,
    operation: str,
    group_col: Optional[str],
    metric_col: Optional[str],
    cols: list[tuple[str, str]],
    col_set: set[str],
    limit_cap: int,
) -> Optional[tuple[tuple[str, str], int]]:
    """
    Топ-N строк по числовой колонке («top 5 by quantity») или одна строка max/min.
    Возвращает ((column, ASC|DESC), limit).
    """
    col_by_lower = _cols_by_lower(cols)
    numeric_set = _numeric_col_names(cols)
    if not numeric_set:
        return None

    if re.search(r"\bгрупп\w*\b", q_lex, re.IGNORECASE):
        return None

    wants_max = bool(
        re.search(r"\b(максим\w*|наибол\w*|наибольш\w*|\bmax\b)\b", q_lex, re.IGNORECASE)
    )
    wants_min = bool(
        re.search(r"\b(миним\w*|наимен\w*|наименьш\w*|\bmin\b)\b", q_lex, re.IGNORECASE)
    )

    def _sort_col_from_by_po() -> Optional[str]:
        chosen: Optional[str] = None
        for m in re.finditer(
            r"\b(?:by|по)\s+([a-zA-Zа-яА-ЯёЁ_][\w]*)\b",
            q_lex,
            re.IGNORECASE,
        ):
            tok = m.group(1).lower()
            if tok in _PO_SKIP_ORDER:
                continue
            cname = col_by_lower.get(tok)
            if cname and cname in numeric_set:
                chosen = cname
        return chosen

    m_top = re.search(r"\b(?:топ|top)\s+(\d{1,4})\b", q_lex, re.IGNORECASE)
    if m_top:
        n = max(1, min(int(m_top.group(1)), 200))
        sort_col = _sort_col_from_by_po()
        if sort_col:
            return ((sort_col, "DESC"), n)
        if operation == "top" and group_col and metric_col and group_col == metric_col:
            return ((metric_col, "DESC"), n)

    if wants_max ^ wants_min:
        direction = "DESC" if wants_max else "ASC"
        sort_col = _sort_col_from_by_po()
        if not sort_col and metric_col and metric_col in numeric_set:
            sort_col = metric_col
        if sort_col:
            return ((sort_col, direction), 1)

    return None


_ROW_TABLE_INTENT = re.compile(
    r"\b(покажи|показать|show|display|выведи|строк\w*|запис\w*"
    r"|данн\w*|таблиц\w*|где|which|rows|records)\b",
    re.IGNORECASE,
)


def _keyword_in_query_as_word(query: str, keyword: str) -> bool:
    """Слово целиком (\b в Unicode), чтобы «top» не ловилось внутри «quantity»."""
    kw = keyword.strip()
    if not kw:
        return False
    if " " in kw:
        return bool(re.search(re.escape(kw), query, re.IGNORECASE))
    return bool(re.search(rf"\b{re.escape(kw)}\b", query, re.IGNORECASE))


def _should_force_select_for_table_rows(q_lex: str, operation: str) -> bool:
    """
    Табличный вывод (строки), а не агрегат: перебиваем top/avg/count/sum от ML,
    если в запросе нет явной просьбы об агрегате.
    """
    if operation not in ("top", "avg", "count", "sum"):
        return False
    if not _ROW_TABLE_INTENT.search(q_lex):
        return False
    if operation == "avg" and re.search(r"\b(средн|average|\bavg\b|орташа)\b", q_lex):
        return False
    if operation == "sum" and re.search(r"\b(сумм|\bsum\b|итого|жалпы|барлығы)\b", q_lex):
        return False
    if operation == "count" and re.search(
        r"\b(сколько|how\s+many|количеств\w*|кол-во|число\s+запис|count\b)\b", q_lex
    ):
        return False
    if operation == "top" and re.search(r"\b(?:топ|top)\s*\d+", q_lex) and re.search(
        r"\b(категор|канал|город|group|групп)\w*", q_lex
    ):
        return False
    return True


def _fmt_answer_num(x: Any) -> str:
    if isinstance(x, float):
        t = f"{x:.6f}".rstrip("0").rstrip(".")
        return t or "0"
    return str(x)


def _lt(lang: str, ru: str, en: str, kk: str) -> str:
    if lang == "en":
        return en
    if lang == "kk":
        return kk
    return ru


def _build_answer_text(
    operation: str,
    rows: list[dict[str, Any]],
    *,
    lang: str,
    group_col: Optional[str],
    metric_col: Optional[str],
    limit: int,
    filter_interpreted: dict[str, Any],
    col_labels: list[str],
) -> str:
    dyn = filter_interpreted.get("dynamic") or []
    cond_parts: list[str] = []
    for f in dyn:
        c, op, val = f.get("column"), f.get("op"), f.get("value")
        if not c:
            continue
        if op == "=":
            cond_parts.append(f"«{c}» = {val!r}")
        elif op:
            cond_parts.append(f"«{c}» {op} {_fmt_answer_num(val)}")
    if filter_interpreted.get("year") is not None:
        cond_parts.append(
            _lt(
                lang,
                f"год = {filter_interpreted['year']}",
                f"year = {filter_interpreted['year']}",
                f"жыл = {filter_interpreted['year']}",
            )
        )
    if filter_interpreted.get("status"):
        cond_parts.append(
            _lt(
                lang,
                f"статус = {filter_interpreted['status']!r}",
                f"status = {filter_interpreted['status']!r}",
                f"мәртебе = {filter_interpreted['status']!r}",
            )
        )
    cond_human = "; ".join(cond_parts) if cond_parts else ""

    ob = filter_interpreted.get("order_by")
    sort_human = ""
    if isinstance(ob, dict) and ob.get("column"):
        sort_human = _lt(
            lang,
            f"Сортировка: столбец «{ob['column']}», направление {ob.get('direction', 'ASC')}. ",
            f"Sorting: column '{ob['column']}', direction {ob.get('direction', 'ASC')}. ",
            f"Сұрыптау: «{ob['column']}» бағаны, бағыты {ob.get('direction', 'ASC')}. ",
        )

    if not rows:
        tail = (
            _lt(lang, f" Фильтрлер: {cond_human}.", f" Filters: {cond_human}.", f" Сүзгілер: {cond_human}.")
            if cond_human
            else ""
        )
        return (
            _lt(
                lang,
                "Ничего не найдено для вашего запроса.",
                "No results were found for your query.",
                "Сұрауыңыз бойынша нәтиже табылмады.",
            )
            + tail
        ).strip()

    if operation == "count" and len(rows) == 1 and "count" in rows[0]:
        n = rows[0]["count"]
        base = _lt(
            lang,
            f"После фильтров подходит {n} строк. Число в колонке «count» — это количество записей в таблице.",
            f"After filtering, {n} rows match. The 'count' column is the number of matching records.",
            f"Сүзгіден кейін {n} жол сәйкес келеді. 'count' бағаны сәйкес жазбалар санын көрсетеді.",
        )
        if cond_human:
            base += f" Условия: {cond_human}."
        return base

    if operation == "sum" and len(rows) == 1 and "sum" in rows[0]:
        v = _fmt_answer_num(rows[0]["sum"])
        base = _lt(
            lang,
            f"Сумма по столбцу «{metric_col}» равна {v}.",
            f"The sum for column '{metric_col}' is {v}.",
            f"«{metric_col}» бағаны бойынша қосынды {v}.",
        )
        if cond_human:
            base += f" Фильтры: {cond_human}."
        return base

    if operation == "avg" and len(rows) == 1 and "avg" in rows[0]:
        v = _fmt_answer_num(rows[0]["avg"])
        base = _lt(
            lang,
            f"Среднее значение по столбцу «{metric_col}» = {v}.",
            f"Average value for column '{metric_col}' = {v}.",
            f"«{metric_col}» бағаны бойынша орташа мән = {v}.",
        )
        if cond_human:
            base += f" Условия: {cond_human}."
        return base

    if operation == "sum" and group_col and rows and "group_key" in rows[0]:
        base = _lt(
            lang,
            f"Сумма по «{metric_col}» для каждого значения «{group_col}».",
            f"Sum of '{metric_col}' for each '{group_col}' value.",
            f"Әрбір «{group_col}» мәні үшін «{metric_col}» қосындысы.",
        )
        if cond_human:
            base += f" Условия: {cond_human}."
        return base

    if operation == "avg" and group_col and rows and "group_key" in rows[0]:
        base = _lt(
            lang,
            f"Среднее по «{metric_col}» в разрезе «{group_col}».",
            f"Average '{metric_col}' grouped by '{group_col}'.",
            f"«{group_col}» бойынша «{metric_col}» орташа мәні.",
        )
        if cond_human:
            base += f" Условия: {cond_human}."
        return base

    if operation == "top" and group_col and rows and "group_key" in rows[0]:
        key_name = metric_col or "числу записей или сумме"
        base = _lt(
            lang,
            f"Топ групп по колонке «{group_col}» (показатель: «{key_name}»).",
            f"Top groups by column '{group_col}' (metric: '{key_name}').",
            f"«{group_col}» бағаны бойынша топ топтар («{key_name}» көрсеткіші).",
        )
        if cond_human:
            base += f" Фильтры: {cond_human}."
        return base

    if operation == "count" and group_col and rows and "group_key" in rows[0]:
        base = _lt(
            lang,
            f"Группировка по «{group_col}»: количество строк в каждой группе.",
            f"Grouped by '{group_col}': number of rows in each group.",
            f"«{group_col}» бойынша топтау: әр топтағы жолдар саны.",
        )
        if cond_human:
            base += f" Условия: {cond_human}."
        return base

    if operation == "select":
        n = len(rows)
        shown = col_labels[:6] + (["…"] if len(col_labels) > 6 else [])
        sample_cols = ", ".join(f"«{c}»" for c in shown)
        base = _lt(
            lang,
            f"Ниже таблица: {n} строк (лимит {limit}). Столбцы: {sample_cols}. ",
            f"Table below: {n} rows (limit {limit}). Columns: {sample_cols}. ",
            f"Төменде кесте: {n} жол (лимит {limit}). Бағандар: {sample_cols}. ",
        )
        if cond_human:
            base += _lt(lang, f"Фильтр: {cond_human}. ", f"Filter: {cond_human}. ", f"Сүзгі: {cond_human}. ")
        if sort_human:
            base += sort_human
        base += _lt(
            lang,
            "Числа в ячейках — значения полей датасета для каждой строки.",
            "Numbers in cells are dataset field values for each row.",
            "Ұяшықтағы сандар — әр жол үшін датасет өрістерінің мәндері.",
        )
        return base.strip()

    return _lt(
        lang,
        "Найдены результаты по вашему запросу.",
        "Results were found for your query.",
        "Сұрауыңыз бойынша нәтижелер табылды.",
    )


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


def _cols_from_schema_profile(columns_json: Any) -> Optional[list[tuple[str, str]]]:
    """
    Accepts DatasetTableMeta.columns_json in either legacy form:
      - [{"name": "...", "type": "..."}]
    or profiled form:
      - [{"name": "...", "type": "...", "profile": {...}}]
    Returns list[(name, type)] or None if unusable.
    """
    if not columns_json:
        return None
    if not isinstance(columns_json, list):
        return None
    out: list[tuple[str, str]] = []
    for item in columns_json:
        if not isinstance(item, dict):
            continue
        n = item.get("name")
        t = item.get("type")
        if isinstance(n, str) and n.strip():
            out.append((n, str(t or "")))
    return out or None


def _detect_operation_heuristic(query: str, lang: str) -> str:
    q = query
    kw = _OP_KEYWORDS.get(lang, _OP_KEYWORDS["ru"])
    if any(_keyword_in_query_as_word(q, k) for k in kw["top"]):
        return "top"
    if any(_keyword_in_query_as_word(q, k) for k in kw["avg"]):
        return "avg"
    if any(_keyword_in_query_as_word(q, k) for k in kw["sum"]):
        return "sum"
    if any(_keyword_in_query_as_word(q, k) for k in kw["count"]):
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


def _pick_limit_from_query(q_lex: str, default_limit: int, max_limit: int = 200) -> int:
    """
    Пытаемся извлечь лимит из естественного языка.
    Примеры:
      - "топ 10 ..." -> 10
      - "покажи все" / "полностью" -> 200 (из-за safety-clamp)
      - "лимит 20" -> 20
    """
    q = (q_lex or "").lower()
    if _query_has_numeric_comparison(q):
        return default_limit

    # "все/полностью" => показать максимально возможное (по safety-клампу)
    if any(k in q for k in ("все", "всё", "полностью", "весь", "без лимита", "безлимита", "полное")):
        return max_limit

    # топ N
    m = re.search(r"\b(?:топ|top)\s*(\d{1,4})\b", q)
    if m:
        n = int(m.group(1))
        return max(1, min(n, max_limit))

    # лимит N
    m = re.search(r"\b(?:лимит|limit|max)\s*(\d{1,4})\b", q)
    if m:
        n = int(m.group(1))
        return max(1, min(n, max_limit))

    # fallback: первое число, не похожее на год
    for mm in re.finditer(r"\b(\d{1,4})\b", q):
        n = int(mm.group(1))
        if 1900 <= n <= 2099:
            continue
        return max(1, min(n, max_limit))

    return default_limit


def _pick_status_filter(col_set: set[str], q_lex: str, params: dict[str, Any]) -> tuple[str, Optional[str]]:
    """
    MVP-фильтр для фраз вида "completed" / "returned" / "refunded".
    Используется, когда операция агрегации не требует group_by, но в запросе есть значение статусного столбца.
    """
    # try to find actual column name in schema (case-insensitive)
    status_col = None
    for c in col_set:
        if c.lower() == "status":
            status_col = c
            break
    if not status_col:
        return ("", None)

    q = q_lex.lower()
    status_keywords = {
        "completed": "completed",
        "returned": "returned",
        "refunded": "refunded",
        "cancelled": "cancelled",
        "canceled": "canceled",
        "pending": "pending",
    }
    for key, val in status_keywords.items():
        if key in q:
            params["status"] = val
            return (status_col, val)
    return ("", None)


# ---------- Main endpoint ----------
@router.post("/answer")
async def answer_query(
    req: AnswerRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    user_lang = normalize_preferred_language(getattr(user, "preferred_language", "ru"))
    # dataset ownership check
    ds = (
        await db.execute(select(Dataset).where(Dataset.id == req.dataset_id, Dataset.user_id == user.id))
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(
            status_code=404,
            detail=_lt(user_lang, "датасет не найден", "dataset not found", "датасет табылмады"),
        )

    transcribed_raw: Optional[str] = None
    if req.input.job_id is not None:
        transcribed_raw = await get_transcript_text(db, req.input.job_id, user_id=user.id)
        qtext = _normalize(_dedupe_voice_transcript(transcribed_raw or ""))
    else:
        qtext = _normalize(req.input.text or "")
    if not qtext:
        raise HTTPException(
            status_code=400,
            detail=_lt(
                user_lang,
                "пустой запрос: передайте input.text или input.job_id",
                "empty query: provide input.text or input.job_id",
                "бос сұрау: input.text немесе input.job_id жіберіңіз",
            ),
        )

    ok_lang, reason = validate_query_language(qtext, user_lang)
    if not ok_lang:
        detail = _lt(
            user_lang,
            "Язык запроса не совпадает с языком аккаунта. Используйте только выбранный язык в профиле или смените язык в меню.",
            "Query language does not match your account language. Use only your selected profile language or change it in the menu.",
            "Сұрау тілі аккаунт тілімен сәйкес емес. Профильде таңдалған тілді ғана қолданыңыз немесе мәзірден тілді ауыстырыңыз.",
        )
        if reason == "need_en":
            detail = _lt(
                user_lang,
                "Ваш язык аккаунта: English. Введите запрос на английском или переключите язык в меню.",
                "Your account language is English. Enter the query in English or switch language in the menu.",
                "Аккаунт тілі: English. Сұрауды ағылшынша енгізіңіз немесе мәзірден тілді ауыстырыңыз.",
            )
        elif reason == "need_ru":
            detail = _lt(
                user_lang,
                "Ваш язык аккаунта: Русский. Введите запрос на русском или переключите язык в меню.",
                "Your account language is Russian. Enter the query in Russian or switch language in the menu.",
                "Аккаунт тілі: Орыс тілі. Сұрауды орысша енгізіңіз немесе мәзірден тілді ауыстырыңыз.",
            )
        elif reason == "need_kk":
            detail = _lt(
                user_lang,
                "Ваш язык аккаунта: Қазақша. Введите запрос на казахском или переключите язык в меню.",
                "Your account language is Kazakh. Enter the query in Kazakh or switch language in the menu.",
                "Аккаунт тілі: Қазақша. Сұрауды қазақша енгізіңіз немесе мәзірден тілді ауыстырыңыз.",
            )
        raise HTTPException(status_code=400, detail=detail)

    # 3) table + schema (раньше intent — нужны колонки для семантики; intent ниже по q_lex)
    table_name = f"ds_{req.dataset_id}_data"
    cols: Optional[list[tuple[str, str]]] = None
    meta = (
        await db.execute(select(DatasetTableMeta).where(DatasetTableMeta.dataset_id == req.dataset_id))
    ).scalar_one_or_none()
    if meta is not None:
        cols = _cols_from_schema_profile(meta.columns_json)
    if not cols:
        cols = await _get_table_columns(db, table_name)
    if not cols:
        raise HTTPException(
            status_code=404,
            detail=_lt(
                user_lang,
                f"таблица {table_name} не найдена или не содержит колонок",
                f"table {table_name} not found or has no columns",
                f"{table_name} кестесі табылмады немесе бағандары жоқ",
            ),
        )

    sem = resolve_metric_and_group(cols, qtext)
    q_lex = sem["query_lexical"]
    metric_col = sem["metric_col"]
    group_col = sem["group_col"]
    semantic_meta = {
        "query_normalized": qtext,
        "query_after_lexical_fixes": q_lex,
        "column_bindings": sem["bindings"],
        "debug": sem.get("debug") or {},
        "resolved_metric": metric_col,
        "resolved_group_by": group_col,
    }

    # 1) Intent from ML — на тексте после лексических правок (ASR → «amount» и т.д.)
    intent_result = predict_intent(q_lex)
    # ожидаем формат: {"intent": "...", "confidence": 0.0..1.0, "top_k": [...]}
    ml_intent = intent_result.get("intent", "fallback")
    if ml_intent == "unknown":
        ml_intent = "fallback"
    ml_conf = float(intent_result.get("confidence", 0.0))

    # 2) Fallback logic: если низкая уверенность — используем эвристику
    threshold = float(req.options.confidence_threshold)
    operation = (
        ml_intent
        if (ml_intent != "fallback" and ml_conf >= threshold)
        else _detect_operation_heuristic(q_lex, user_lang)
    )

    if _comparison_demands_filtered_rows(q_lex):
        operation = "select"
        group_col = None
        semantic_meta["resolved_group_by"] = None
        semantic_meta["operation_override"] = "numeric_comparison_rows"

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

    if _should_force_select_for_table_rows(q_lex, operation):
        operation = "select"
        group_col = None
        semantic_meta["resolved_group_by"] = None
        semantic_meta["operation_override"] = "row_table_intent"

    year_filter = _pick_year_filter(cols, q_lex)

    base_limit = max(1, min(int(req.options.limit), 200))
    limit = _pick_limit_from_query(q_lex, default_limit=base_limit, max_limit=200)

    # 4) safe identifiers check
    if metric_col and metric_col not in col_set:
        metric_col = None
    if group_col and group_col not in col_set:
        group_col = None

    params: dict[str, Any] = {}
    where_parts: list[str] = []
    if year_filter:
        ycol, yval = year_filter
        if ycol in col_set:
            where_parts.append(f"`{ycol}` = :year")
            params["year"] = yval

    # Optional status filter for count-like questions (e.g. "сколько Completed?")
    status_col, _status_val = _pick_status_filter(col_set=col_set, q_lex=q_lex, params=params)
    if status_col and params.get("status") is not None:
        where_parts.append(f"`{status_col}` = :status")

    skip_dyn: set[str] = set()
    if year_filter and year_filter[0] in col_set:
        skip_dyn.add(year_filter[0])
    if status_col:
        skip_dyn.add(status_col)

    # Dataset-agnostic: resolve value-only filters via stored dataset profile (if available).
    profile_debug: dict[str, Any] = {}
    if meta is not None and meta.columns_json:
        prof_filters, prof_dbg = resolve_profile_value_filters(
            query_text=q_lex,
            columns_json=meta.columns_json,
            max_filters=6,
        )
        profile_debug = prof_dbg
        if prof_filters:
            # keep only safe columns
            prof_filters = [f for f in prof_filters if f.column in col_set]
            pf_frags, pf_params, pf_meta, pf_used_cols = compile_profile_filters_to_sql(
                prof_filters, param_prefix="pf"
            )
            # merge before dynamic filters so explicit "col value" can override (skip_columns prevents dupes)
            where_parts.extend(pf_frags)
            params.update(pf_params)
            skip_dyn.update(pf_used_cols)
            # attach into interpreted.filter.dynamic later (we'll merge meta)
            semantic_meta.setdefault("profile_value_debug", profile_debug)
            semantic_meta.setdefault("profile_value_filters", pf_meta)
    else:
        semantic_meta.setdefault("profile_value_debug", {"enabled": False})

    dyn_frags, dyn_params, dyn_meta, order_by = build_dynamic_filters_and_order(
        cols, q_lex, skip_columns=skip_dyn
    )
    params.update(dyn_params)
    where_parts.extend(dyn_frags)

    row_rank = _resolve_top_n_or_extreme_row_ranking(
        q_lex, operation, group_col, metric_col, cols, col_set, limit
    )
    if row_rank:
        ob_pair, lim_rank = row_rank
        oc, od = ob_pair
        if oc in col_set:
            operation = "select"
            group_col = None
            semantic_meta["resolved_group_by"] = None
            order_by = (oc, od)
            limit = lim_rank
            semantic_meta["operation_override"] = (
                semantic_meta.get("operation_override") or "row_ranking_top_or_extreme"
            )

    filtered_eq_cols = {m["column"] for m in dyn_meta if m.get("op") == "="}
    if operation == "count" and should_use_scalar_count_instead_of_group(q_lex, group_col, filtered_eq_cols):
        group_col = None
        semantic_meta["resolved_group_by"] = None
        if "group_by" in semantic_meta.get("column_bindings", {}):
            semantic_meta["column_bindings"]["group_by"] = {
                "column": None,
                "method": "scalar_count_with_filter",
                "detail": "равенство по колонке вместо GROUP BY",
            }

    where_sql = f" WHERE {' AND '.join(where_parts)} " if where_parts else ""

    filter_interpreted: dict[str, Any] = {}
    if "year" in params:
        filter_interpreted["year"] = params["year"]
    if params.get("status") is not None:
        filter_interpreted["status"] = params["status"]
    if dyn_meta:
        filter_interpreted["dynamic"] = dyn_meta
    if order_by:
        filter_interpreted["order_by"] = {"column": order_by[0], "direction": order_by[1]}

    interpreted: dict[str, Any] = {
        "table": table_name,
        "operation": operation,
        "metric": metric_col,
        "group_by": [group_col] if group_col else [],
        "filter": filter_interpreted,
        "limit": limit,
        "intent": intent_result,
        "confidence_threshold": threshold,
        "used_ml_intent": (ml_intent != "fallback" and ml_conf >= threshold),
        "semantic": semantic_meta,
    }

    # 5) SQL build
    sql = ""

    # SQL build.
    # Важно: если операция агрегации, но group_by не найден,
    # нельзя отдавать весь датасет (`SELECT *`).
    # Вместо этого считаем итоговую агрегацию без группировки.
    order_sql_select = ""
    if operation == "select" and order_by:
        oc, od = order_by
        if oc in col_set:
            order_sql_select = f" ORDER BY `{oc}` {od} "

    if operation == "top":
        if not group_col:
            # на случай если группировка не нашлась — безопасно возвращаем выборку
            sql = f"SELECT * FROM `{table_name}` {where_sql} {order_sql_select} LIMIT {limit}"
        else:
            if metric_col:
                agg_expr = f"SUM(`{metric_col}`)"
                alias = "sum"
            else:
                agg_expr = "COUNT(*)"
                alias = "count"

            order_sql = f" ORDER BY `{alias}` DESC "
            sql = f"""
                SELECT `{group_col}` AS group_key, {agg_expr} AS `{alias}`
                FROM `{table_name}`
                {where_sql}
                GROUP BY `{group_col}`
                {order_sql}
                LIMIT {limit}
            """

    elif operation == "count":
        if group_col:
            sql = f"""
                SELECT `{group_col}` AS group_key, COUNT(*) AS `count`
                FROM `{table_name}`
                {where_sql}
                GROUP BY `{group_col}`
                LIMIT {limit}
            """
        else:
            # Total count without grouping
            sql = f"""
                SELECT COUNT(*) AS `count`
                FROM `{table_name}`
                {where_sql}
                LIMIT 1
            """

    elif operation == "sum":
        if not metric_col:
            raise HTTPException(status_code=400, detail="cannot sum without metric column")

        if group_col:
            sql = f"""
                SELECT `{group_col}` AS group_key, SUM(`{metric_col}`) AS `sum`
                FROM `{table_name}`
                {where_sql}
                GROUP BY `{group_col}`
                LIMIT {limit}
            """
        else:
            sql = f"""
                SELECT SUM(`{metric_col}`) AS `sum`
                FROM `{table_name}`
                {where_sql}
                LIMIT 1
            """

    elif operation == "avg":
        if not metric_col:
            raise HTTPException(status_code=400, detail="cannot avg without numeric metric column")
        if group_col:
            sql = f"""
                SELECT `{group_col}` AS group_key, AVG(`{metric_col}`) AS `avg`
                FROM `{table_name}`
                {where_sql}
                GROUP BY `{group_col}`
                LIMIT {limit}
            """
        else:
            sql = f"""
                SELECT AVG(`{metric_col}`) AS `avg`
                FROM `{table_name}`
                {where_sql}
                LIMIT 1
            """

    else:
        # plain rows
        sql = f"SELECT * FROM `{table_name}` {where_sql} {order_sql_select} LIMIT {limit}"

    res = await db.execute(text(sql), params)
    rows = [dict(r._mapping) for r in res.fetchall()]

    col_labels = [c[0] for c in cols]
    answer_text = _build_answer_text(
        operation,
        rows,
        lang=user_lang,
        group_col=group_col,
        metric_col=metric_col,
        limit=limit,
        filter_interpreted=filter_interpreted,
        col_labels=col_labels,
    )

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
