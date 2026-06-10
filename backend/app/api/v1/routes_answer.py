from __future__ import annotations

import re
from dataclasses import asdict
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import engine, get_db
from app.services.intent_service import predict_intent
from app.services.job_transcript import get_transcript_text
from app.services.query_filters import (
    _col_aliases,
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
from app.services.analytics_semantic_layer import AnalyticsSemanticResult, analyze_query_semantics
from app.services.dataset_overview_service import unwrap_dataset_columns_json
from app.services.search_service import SearchService

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
    execution_source: Optional[str] = None


class AnswerRequest(BaseModel):
    dataset_id: int
    input: QueryInput
    options: AnswerOptions = AnswerOptions()


# ---------- Semantic debug (no SQL) ----------
class SemanticDebugRequest(BaseModel):
    """Developer payload: inspect legacy vs analytics semantic layer for a query."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"dataset_id": 1, "text": "show top revenue by category"},
                {"dataset_id": 1, "text": "покажи топ категорий по выручке"},
                {"dataset_id": 1, "text": "санат бойынша ең көп табысты көрсет"},
            ]
        }
    )

    dataset_id: int = Field(..., ge=1)
    text: str = Field(..., min_length=1, max_length=4000)


class SemanticDebugLegacySemantics(BaseModel):
    """Output of ``resolve_metric_and_group`` (same inputs as ``/query/answer``)."""

    resolved_metric: Optional[str] = None
    resolved_group_by: Optional[str] = None
    query_after_lexical_fixes: str = ""
    debug: dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None


class SemanticDebugComparison(BaseModel):
    """Side-by-side agreement between legacy resolver and semantic layer v1."""

    metric_same: bool
    group_same: bool
    semantic_metric_better_candidate: Optional[str] = None
    semantic_group_better_candidate: Optional[str] = None


class SemanticDebugResponse(BaseModel):
    """Full debug payload; never executes SQL."""

    dataset_id: int
    query: str
    table_name: str
    legacy_semantics: SemanticDebugLegacySemantics
    semantic_layer_v1: Optional[dict[str, Any]] = None
    semantic_layer_error: Optional[str] = None
    comparison: SemanticDebugComparison


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


def _is_canonical_suggestion_query(q: str) -> bool:
    """English templates generated internally for suggested-question chips."""
    return bool(
        re.match(r"^show average [a-zA-Z0-9_ .-]+$", q)
        or re.match(r"^count records by [a-zA-Z0-9_ .-]+$", q)
        or re.match(r"^show trend by [a-zA-Z0-9_ .-]+$", q)
        or re.match(r"^show sample values from [a-zA-Z0-9_ .-]+$", q)
        or re.match(r"^show top values by [a-zA-Z0-9_ .-]+$", q)
        or re.match(r"^show minimum [a-zA-Z0-9_ .-]+$", q)
        or re.match(r"^show maximum [a-zA-Z0-9_ .-]+$", q)
        or re.match(r"^show sum of [a-zA-Z0-9_ .-]+$", q)
        or q == "show first rows"
    )


def _resolve_canonical_suggestion_query(
    q: str,
    cols: list[tuple[str, str]] | None,
) -> Optional[dict[str, Any]]:
    """Resolve our internal suggested-question templates deterministically."""
    query = _normalize(q)
    if query == "show first rows":
        return {"intent": "first_rows", "column": None}

    columns_by_lower = {str(name).lower(): str(name) for name, _typ in cols or [] if str(name or "").strip()}
    if not columns_by_lower:
        return None

    patterns: list[tuple[str, str]] = [
        ("average", r"^show average (.+)$"),
        ("count_by", r"^count records by (.+)$"),
        ("trend_by", r"^show trend by (.+)$"),
        ("sample", r"^show sample values from (.+)$"),
        ("top_values", r"^show top values by (.+)$"),
        ("min", r"^show minimum (.+)$"),
        ("max", r"^show maximum (.+)$"),
        ("sum", r"^show sum of (.+)$"),
    ]
    for intent, pattern in patterns:
        m = re.match(pattern, query)
        if not m:
            continue
        raw_col = m.group(1).strip().strip("`\"'")
        col = columns_by_lower.get(raw_col.lower())
        if col:
            return {"intent": intent, "column": col}
    return None


def _strip_dataset_columns_for_language_validation(q: str, cols: list[tuple[str, str]] | None) -> str:
    text_value = q or ""
    for name, _typ in cols or []:
        col = str(name or "").strip()
        if not col:
            continue
        escaped = re.escape(col.lower())
        text_value = re.sub(rf"(?<![a-zA-Z0-9_]){escaped}(?![a-zA-Z0-9_])", " ", text_value, flags=re.IGNORECASE)
    return _normalize(text_value)


def _canonicalize_localized_suggestion_query(q: str, cols: list[tuple[str, str]] | None) -> str:
    query = _normalize(q)
    for name, _typ in cols or []:
        col = str(name or "").strip()
        if not col:
            continue
        col_low = col.lower()
        col_pattern = re.escape(col_low)
        has_col = re.search(rf"(?<![a-zA-Z0-9_]){col_pattern}(?![a-zA-Z0-9_])", query, re.IGNORECASE)
        if not has_col:
            continue

        if ("бойынша" in query and "орташа" in query) or re.search(rf"средн\w*\s+.*\bпо\s+{col_pattern}\b", query):
            return f"show average {col}"
        if ("бойынша" in query and ("жазбалар сан" in query or "санын" in query)) or re.search(
            rf"(посчитать|количеств\w*)\s+.*\bпо\s+{col_pattern}\b", query
        ):
            return f"count records by {col}"
        if ("бойынша" in query and ("үрдіс" in query or "тренд" in query)) or re.search(
            rf"тренд\s+.*\bпо\s+{col_pattern}\b", query
        ):
            return f"show trend by {col}"
        if ("бағанынан" in query and "мысал" in query) or re.search(rf"пример\w*\s+.*\bиз\s+{col_pattern}\b", query):
            return f"show sample values from {col}"
        if ("бойынша" in query and ("ең жиі" in query or "жиі мән" in query)) or re.search(
            rf"(част\w*|частые)\s+.*\bпо\s+{col_pattern}\b", query
        ):
            return f"show top values by {col}"
        if ("бойынша" in query and ("ең кіші" in query or "миним" in query)) or re.search(
            rf"миним\w*\s+.*\bпо\s+{col_pattern}\b", query
        ):
            return f"show minimum {col}"
        if ("бойынша" in query and ("ең үлкен" in query or "максим" in query)) or re.search(
            rf"максим\w*\s+.*\bпо\s+{col_pattern}\b", query
        ):
            return f"show maximum {col}"
        if ("бойынша" in query and ("соманы" in query or "қосынды" in query)) or re.search(
            rf"сумм\w*\s+.*\bпо\s+{col_pattern}\b", query
        ):
            return f"show sum of {col}"

    if "алғашқы жол" in query or "первые строки" in query:
        return "show first rows"
    return q


def _dedupe_voice_transcript(raw: str) -> str:
    """Убирает типичный мусор ASR: вся фраза продублирована подряд второй раз."""
    t = (raw or "").strip()
    if len(t) > 24:
        h = len(t) // 2
        if t[:h] == t[h:]:
            t = t[:h].strip()
    return t


def _query_has_numeric_comparison(q: str) -> bool:
    """Чтобы «quantity меньше 10» / «score more than 70» не превращались в LIMIT."""
    if re.search(r"\b(?:больше|меньше|не\s+больше|не\s+меньше)\s+\d", q, re.IGNORECASE):
        return True
    if re.search(r"\b\w+\s*(?:>|<|>=|<=)\s*\d", q):
        return True
    # English: «more than 70», «at least 5», «above 80» — иначе 70 уходит в pick_limit_from_query
    if re.search(
        r"\b(?:more|less|greater|fewer)\s+than\s+\d",
        q,
        re.IGNORECASE,
    ):
        return True
    if re.search(
        r"\b(?:at\s+least|at\s+most|not\s+less\s+than|not\s+more\s+than)\s+\d",
        q,
        re.IGNORECASE,
    ):
        return True
    if re.search(r"\b(?:above|below|over|under)\s+\d", q, re.IGNORECASE):
        return True
    if re.search(
        r"\b(?:higher|lower)\s+than\s+\d",
        q,
        re.IGNORECASE,
    ):
        return True
    if re.search(
        r"\b(?:оценк\w*|балл\w*|результат\w*)"
        r"(?:\s+[a-zA-Zа-яА-ЯёЁ0-9_-]+){0,6}"
        r"\s+(?:больше|меньше|выше|ниже|не\s+меньше|не\s+больше)\s+\d",
        q,
        re.IGNORECASE,
    ):
        return True
    if re.search(r"\bбольше\s+\d+(?:\.\d+)?\s+(?:баллов|балла|балл|очков)\b", q, re.IGNORECASE):
        return True
    if re.search(
        r"\b(?:more|greater|higher)(?:\s+than)?\s+\d+\s+but\s+(?:less|lower|fewer)(?:\s+than)?\s+\d",
        q,
        re.IGNORECASE,
    ):
        return True
    if re.search(r"\bбольше\s+\d+\s+(?:но|а)\s+меньше\s+\d", q, re.IGNORECASE):
        return True
    if re.search(r"\bменьше\s+\d+\s+(?:но|а)\s+больше\s+\d", q, re.IGNORECASE):
        return True
    return False


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


def _is_average_metric_candidate(column: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "", column.lower())
    if normalized.endswith("id"):
        return False
    return normalized not in {
        "vendorid",
        "ratecodeid",
        "pulocationid",
        "dolocationid",
        "paymenttype",
        "storeandfwdflag",
    }


def _resolve_explicit_average_by(
    q_lex: str,
    cols: list[tuple[str, str]],
) -> Optional[tuple[str, str]]:
    """Resolve safe phrases like "average fare_amount by VendorID" deterministically."""
    if not re.search(r"\b(?:average|avg|mean)\b", q_lex, re.IGNORECASE):
        return None
    numeric_set = _numeric_col_names(cols)
    aliases: list[tuple[str, str]] = []
    for col, _typ in cols:
        for alias in _col_aliases(col):
            aliases.append((alias.lower(), col))
    aliases.sort(key=lambda item: len(item[0]), reverse=True)

    q = q_lex.lower()
    for metric_alias, metric_col in aliases:
        if metric_col not in numeric_set or not _is_average_metric_candidate(metric_col):
            continue
        metric_pat = re.escape(metric_alias)
        for group_alias, group_col in aliases:
            if group_col == metric_col:
                continue
            group_pat = re.escape(group_alias)
            if re.search(rf"\b(?:average|avg|mean)\s+{metric_pat}\s+(?:by|per|for each|grouped by)\s+{group_pat}\b", q):
                return metric_col, group_col
    return None


def _resolve_explicit_top_values(q_lex: str, cols: list[tuple[str, str]]) -> Optional[str]:
    """Resolve safe phrases like "show top payment_type" as top grouped values."""
    aliases: list[tuple[str, str]] = []
    for col, _typ in cols:
        for alias in _col_aliases(col):
            aliases.append((alias.lower(), col))
    aliases.sort(key=lambda item: len(item[0]), reverse=True)

    q = q_lex.lower().strip().rstrip(".")
    for alias, col in aliases:
        if re.fullmatch(rf"(?:show\s+)?top\s+{re.escape(alias)}", q, re.IGNORECASE):
            return col
    return None


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


def _numeric_sort_col_superlative_phrase(
    q_lex: str,
    col_names: list[str],
    numeric_set: set[str],
) -> Optional[str]:
    """«lowest attendance», «attendance highest», «минимальная посещаемость»."""
    qlow = q_lex.lower()
    low_pat = r"(?:lowest|minimum|\bmin\b|smallest|least|миним\w*|наименьш\w*|наимен\w*)"
    high_pat = r"(?:highest|maximum|\bmax\b|largest|greatest|максим\w*|наибольш\w*|наибол\w*)"
    for name in sorted(col_names, key=len, reverse=True):
        if name not in numeric_set:
            continue
        for alias in _col_aliases(name):
            esc = re.escape(alias)
            if (
                re.search(rf"\b{low_pat}\s+{esc}\b", qlow, re.IGNORECASE)
                or re.search(rf"\b{esc}\s+{low_pat}\b", qlow, re.IGNORECASE)
                or re.search(rf"\b{high_pat}\s+{esc}\b", qlow, re.IGNORECASE)
                or re.search(rf"\b{esc}\s+{high_pat}\b", qlow, re.IGNORECASE)
            ):
                return name
    return None


def _resolve_top_n_or_extreme_row_ranking(
    q_lex: str,
    operation: str,
    group_col: Optional[str],
    metric_col: Optional[str],
    cols: list[tuple[str, str]],
    col_set: set[str],
    limit_cap: int,
) -> Optional[tuple[Any, ...]]:
    """
    Топ-N («top 5 by quantity»), экстремум строк, или lowest+highest (UNION).
    Возвращает:
      ("single", column, "ASC"|"DESC", limit)
      ("dual", column, half_limit)  — по half_limit строк с каждого конца
    """
    col_by_lower = _cols_by_lower(cols)
    col_names = [c[0] for c in cols]
    numeric_set = _numeric_col_names(cols)
    if not numeric_set:
        return None

    if re.search(r"\bгрупп\w*\b", q_lex, re.IGNORECASE):
        return None

    wants_max = bool(
        re.search(
            r"\b(максим\w*|наибол\w*|наибольш\w*|\bmax\b|\bhighest\b|\blargest\b|\bgreatest\b)\b",
            q_lex,
            re.IGNORECASE,
        )
    )
    wants_min = bool(
        re.search(
            r"\b(миним\w*|наимен\w*|наименьш\w*|\bmin\b|\blowest\b|\bsmallest\b|\bleast\b)\b",
            q_lex,
            re.IGNORECASE,
        )
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
            return ("single", sort_col, "DESC", n)
        if operation == "top" and group_col and metric_col and group_col == metric_col:
            return ("single", metric_col, "DESC", n)

    if wants_max and wants_min:
        sort_col = _sort_col_from_by_po()
        if not sort_col:
            sort_col = _numeric_sort_col_superlative_phrase(q_lex, col_names, numeric_set)
        if not sort_col and metric_col and metric_col in numeric_set:
            sort_col = metric_col
        if sort_col and sort_col in numeric_set:
            half = max(1, min(limit_cap // 2, 100))
            return ("dual", sort_col, half)

    if wants_max ^ wants_min:
        direction = "DESC" if wants_max else "ASC"
        sort_col = _sort_col_from_by_po()
        if not sort_col:
            sort_col = _numeric_sort_col_superlative_phrase(q_lex, col_names, numeric_set)
        if not sort_col and metric_col and metric_col in numeric_set:
            sort_col = metric_col
        if sort_col:
            single_val = bool(
                re.search(
                    r"\b(what\s+is|what's|what\s+are|how\s+much|how\s+many|сколько|каков)\b",
                    q_lex,
                    re.IGNORECASE,
                )
            )
            lim = 1 if single_val else max(1, min(limit_cap, 200))
            return ("single", sort_col, direction, lim)

    return None


def _wants_largest_score_delta_from_previous(q_lex: str) -> bool:
    """
    «Biggest improvement in exam score from previous score» → ORDER BY (exam - previous) DESC.
    """
    q = q_lex.lower()
    if not re.search(
        r"\b(?:biggest|largest|max(?:imum)?|greatest|highest|most|top|"
        r"наибольш|максимальн|самый\s+больш|самая\s+больш|самое\s+больш)\b",
        q,
        re.IGNORECASE,
    ):
        return False
    if not re.search(
        r"\b(?:development|improvement|growth|gain|increase|progress|change|difference|delta|jump|"
        r"прирост|рост|улучшен|разниц|динамик|развити)\b",
        q,
        re.IGNORECASE,
    ):
        return False
    if not re.search(
        r"\b(?:previous|prior|предыдущ|прошл|from\s+previous)\b",
        q,
        re.IGNORECASE,
    ) and not re.search(r"\b(?:exam\s+score|exam_score)\b", q, re.IGNORECASE):
        return False
    return True


def _find_current_and_previous_score_columns(
    col_names: list[str],
    numeric_set: set[str],
) -> Optional[tuple[str, str]]:
    """Возвращает (текущий балл, предыдущий) для разности."""
    prev_cols: list[str] = []
    exam_cols: list[str] = []
    for c in col_names:
        if c not in numeric_set:
            continue
        low = c.lower()
        if ("previous" in low or "prior" in low) and ("score" in low or "scores" in low):
            prev_cols.append(c)
        elif "exam" in low and ("score" in low or "scores" in low):
            exam_cols.append(c)
    if not exam_cols or not prev_cols:
        return None
    exam_cols.sort(key=len)
    prev_cols.sort(key=len)
    return (exam_cols[0], prev_cols[0])


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


_NETFLIX_CONFLICT_OVERRIDES: frozenset[str] = frozenset(
    {
        "canonical_suggestion",
        "numeric_comparison_rows",
        "row_table_intent",
        "score_delta_vs_previous",
        "row_ranking_dual_extreme",
        "row_ranking_top_or_extreme",
    }
)


def _detect_netflix_query_pattern(
    q_lex: str,
    col_set: set[str],
    semantic_layer_result: Optional[AnalyticsSemanticResult],
) -> dict[str, Any]:
    """
    Controlled Netflix-style catalog intents (column names must exist in ``col_set``).

    Returns a dict with ``matched``, ``pattern``, ``operation``, ``metric_col``,
    ``group_col``, ``order_by``, and ``debug``. Does not execute SQL.
    """
    ql = (q_lex or "").strip().lower()
    out: dict[str, Any] = {
        "matched": False,
        "pattern": None,
        "operation": None,
        "metric_col": None,
        "group_col": None,
        "order_by": None,
        "debug": {},
    }
    dd = semantic_layer_result.detected_dimension if semantic_layer_result else None
    dd_col = dd.column if dd else None
    dd_score = float(dd.score) if dd else 0.0

    def _ok_dim(col: str, min_score: float = 0.35) -> bool:
        return bool(dd and dd_col == col and dd_score >= min_score)

    # B) compare movies vs TV (high-specificity first)
    if "type" in col_set:
        has_movie = bool(re.search(r"\b(movies?|film|films|фильм|кино)\b", ql))
        has_tv = bool(re.search(r"\b(tv\s*shows?|television|series|сериал|шоу)\b", ql))
        wants_compare = bool(
            re.search(r"\b(compare|comparison|versus|vs|between)\b", ql)
            or re.search(r"\bmovies\s+and\s+tv\b", ql)
            or re.search(r"\bfilms?\s+and\s+(tv|series)\b", ql)
        )
        if wants_compare and has_movie and has_tv:
            out.update(
                matched=True,
                pattern="compare_type",
                operation="count",
                metric_col=None,
                group_col="type",
                order_by="count_desc",
                debug={"signals": ["compare", "movie", "tv"]},
            )
            return out

    # E) longest movies (duration text + superlative)
    if "duration" in col_set and re.search(r"\b(longest|длинн|max\s+runtime|runtime)\b", ql):
        if re.search(r"\b(movies?|фильм|кино)\b", ql):
            title_c = "title" if "title" in col_set else None
            sid = "show_id" if "show_id" in col_set else None
            group_disp = title_c or sid
            if group_disp:
                out.update(
                    matched=True,
                    pattern="longest_movies",
                    operation="top",
                    metric_col="duration",
                    group_col=group_disp,
                    order_by="sum_desc",
                    debug={"title_used": title_c is not None, "group_col": group_disp},
                )
                return out

    # C) release-year trend
    if "release_year" in col_set and re.search(
        r"\b(trend|trends|releases?\s+over|over\s+time|timeline|year\s+over|"
        r"динамика|тренд|по\s+годам|жылдар|шыққан)\b",
        ql,
    ):
        out.update(
            matched=True,
            pattern="release_trend",
            operation="count",
            metric_col=None,
            group_col="release_year",
            order_by="release_year_asc",
            debug={"signals": ["release_axis"]},
        )
        return out

    # A) top / most common genres (semantic layer must point at listed_in)
    if "listed_in" in col_set and _ok_dim("listed_in", 0.35):
        if re.search(
            r"\b(genres?|жанр|top\s+genres|most\s+common\s+genres|common\s+genres|"
            r"which\s+genres|dominat\w*)\b",
            ql,
        ):
            out.update(
                matched=True,
                pattern="top_genres",
                operation="top",
                metric_col=None,
                group_col="listed_in",
                order_by="count_desc",
                debug={"semantic_dimension": "listed_in", "score": dd_score},
            )
            return out

    # D) countries producing content
    if "country" in col_set and (
        _ok_dim("country", 0.32)
        or re.search(r"\b(countries?|стран|елдер|nation|nations|region|regions)\b", ql)
    ):
        if re.search(r"\b(produce|production|content|контент|шыхара|шығар)\b", ql) or re.search(
            r"\b(which|what|какие|қай)\s+\w*\s*(countries?|стран|ел)\b",
            ql,
        ):
            out.update(
                matched=True,
                pattern="top_countries",
                operation="top",
                metric_col=None,
                group_col="country",
                order_by="count_desc",
                debug={"semantic_dimension": dd_col if dd_col == "country" else None},
            )
            return out

    return out


def _netflix_duration_agg_expr_sql(duration_col: str) -> str:
    """
    Aggregate first integer token from a textual duration (e.g. ``90 min``, ``1 Season``).

    Dialect-specific for MySQL vs SQLite dev setups.
    """
    qc = f"`{duration_col}`"
    dialect = (getattr(engine.dialect, "name", "") or "").lower()
    if dialect in ("mysql", "mariadb"):
        return f"MAX(CAST(REGEXP_SUBSTR({qc}, '[0-9]+') AS UNSIGNED))"
    trimmed = f"TRIM({qc})"
    return (
        f"MAX(CASE WHEN {trimmed} = '' OR {trimmed} IS NULL THEN 0 "
        f"ELSE CAST("
        f"SUBSTR({trimmed}, 1, MAX(1, INSTR({trimmed} || ' ', ' ') - 1))"
        f" AS INTEGER) END)"
    )


def _netflix_listed_in_nonempty_where_clause(where_sql: str) -> str:
    """Append ``listed_in`` non-empty guard to an existing ``WHERE ...`` fragment (or create one)."""
    dialect = (getattr(engine.dialect, "name", "") or "").lower()
    if dialect in ("mysql", "mariadb"):
        cond = "`listed_in` IS NOT NULL AND TRIM(CAST(`listed_in` AS CHAR)) != ''"
    else:
        cond = "`listed_in` IS NOT NULL AND TRIM(CAST(`listed_in` AS TEXT)) != ''"
    ws = (where_sql or "").strip()
    if not ws:
        return f" WHERE {cond} "
    if ws.upper().startswith("WHERE"):
        return f"{ws} AND {cond} "
    return f" WHERE {ws} AND {cond} "


def _append_nonempty_group_where_clause(where_sql: str, group_col: str) -> str:
    """Exclude NULL/blank grouping keys so charts are not dominated by empty buckets."""
    dialect = (getattr(engine.dialect, "name", "") or "").lower()
    cast_type = "CHAR" if dialect in ("mysql", "mariadb") else "TEXT"
    cond = f"`{group_col}` IS NOT NULL AND TRIM(CAST(`{group_col}` AS {cast_type})) != ''"
    ws = (where_sql or "").strip()
    if not ws:
        return f" WHERE {cond} "
    if ws.upper().startswith("WHERE"):
        return f"{ws} AND {cond} "
    return f" WHERE {ws} AND {cond} "


def _mysql_supports_recursive_split_cte(col_set: set[str]) -> bool:
    """MySQL 8.0.4+ ``WITH RECURSIVE`` + stable row id via ``show_id`` (Netflix CSV)."""
    if "show_id" not in col_set:
        return False
    v = getattr(engine.dialect, "server_version_info", None)
    if not v:
        return False
    try:
        return bool(v >= (8, 0, 4))
    except Exception:
        return False


def _netflix_top_genres_listed_in_split_sql(
    table_name: str,
    where_sql: str,
    limit: int,
    col_set: set[str],
) -> tuple[str, str, bool]:
    """
    Build grouped genre counts by splitting ``listed_in`` on commas.

    Returns ``(sql, genre_split_strategy, genre_split_enabled)`` where
    ``genre_split_enabled`` is True only for the recursive-CTE path.
    """
    dialect = (getattr(engine.dialect, "name", "") or "").lower()
    base_where = _netflix_listed_in_nonempty_where_clause(where_sql)
    qt = f"`{table_name}`"

    if dialect in ("mysql", "mariadb") and _mysql_supports_recursive_split_cte(col_set):
        sql = f"""
WITH RECURSIVE split_genres AS (
  SELECT
    `show_id` AS _rid,
    TRIM(SUBSTRING_INDEX(`listed_in`, ',', 1)) AS genre,
    TRIM(
      CASE
        WHEN LOCATE(',', `listed_in`) > 0
        THEN SUBSTRING(`listed_in`, LOCATE(',', `listed_in`) + 1)
        ELSE ''
      END
    ) AS remainder
  FROM {qt}
  {base_where}

  UNION ALL

  SELECT
    _rid,
    TRIM(SUBSTRING_INDEX(remainder, ',', 1)),
    TRIM(
      CASE
        WHEN LOCATE(',', remainder) > 0
        THEN SUBSTRING(remainder, LOCATE(',', remainder) + 1)
        ELSE ''
      END
    )
  FROM split_genres
  WHERE remainder <> '' AND remainder IS NOT NULL
)
SELECT genre AS `listed_in`, COUNT(*) AS `count`
FROM split_genres
WHERE genre <> '' AND genre IS NOT NULL
GROUP BY genre
ORDER BY `count` DESC
LIMIT {int(limit)}
""".strip()
        return sql, "recursive_cte", True

    if dialect == "sqlite" or (
        (getattr(engine.url, "drivername", "") or "").lower().startswith("sqlite")
        and dialect not in ("mysql", "mariadb")
    ):
        sql = f"""
WITH RECURSIVE split_genres AS (
  SELECT
    ROWID AS _rid,
    TRIM(SUBSTR(`listed_in`, 1, INSTR(`listed_in` || ',', ',') - 1)) AS genre,
    CASE
      WHEN INSTR(`listed_in`, ',') > 0
      THEN SUBSTR(`listed_in`, INSTR(`listed_in`, ',') + 1)
      ELSE ''
    END AS remainder
  FROM {qt}
  {base_where}

  UNION ALL

  SELECT
    _rid,
    TRIM(SUBSTR(remainder, 1, INSTR(remainder || ',', ',') - 1)),
    CASE
      WHEN INSTR(remainder, ',') > 0
      THEN SUBSTR(remainder, INSTR(remainder, ',') + 1)
      ELSE ''
    END
  FROM split_genres
  WHERE remainder != '' AND remainder IS NOT NULL
)
SELECT genre AS `listed_in`, COUNT(*) AS `count`
FROM split_genres
WHERE genre != '' AND genre IS NOT NULL
GROUP BY genre
ORDER BY `count` DESC
LIMIT {int(limit)}
""".strip()
        return sql, "recursive_cte", True

    # Safe fallback: whole ``listed_in`` string as one bucket (previous behaviour).
    fb_where = _netflix_listed_in_nonempty_where_clause(where_sql)
    sql = f"""
SELECT `listed_in` AS `listed_in`, COUNT(*) AS `count`
FROM {qt}
{fb_where}
GROUP BY `listed_in`
ORDER BY `count` DESC
LIMIT {int(limit)}
""".strip()
    return sql, "fallback_grouped_string", False


def _chart_row_keys(rows: list[dict[str, Any]]) -> set[str]:
    if not rows or not isinstance(rows[0], dict):
        return set()
    try:
        return set(rows[0].keys())
    except Exception:
        return set()


def _chart_infer_category_key(
    rows: list[dict[str, Any]],
    group_col: Optional[str],
    prefer: Optional[list[str]] = None,
) -> Optional[str]:
    ks = _chart_row_keys(rows)
    if not ks:
        return None
    if prefer:
        for k in prefer:
            if k and k in ks:
                return k
    for k in ("group_key", "listed_in"):
        if k in ks:
            return k
    if group_col and group_col in ks:
        return group_col
    return None


def _chart_infer_value_key(
    rows: list[dict[str, Any]],
    operation: str,
    metric_col: Optional[str],
) -> Optional[str]:
    ks = _chart_row_keys(rows)
    for k in ("sum", "count", "avg", "min", "max"):
        if k in ks:
            return k
    if metric_col and metric_col in ks:
        return metric_col
    return None


def _build_chart_suggestion(
    operation: str,
    group_col: Optional[str],
    metric_col: Optional[str],
    rows: list[dict[str, Any]],
    semantic_meta: dict[str, Any],
    limit: int,
) -> dict[str, Any]:
    """
    Lightweight chart hint for the frontend (never raises; tolerates odd row shapes).
    """
    disabled: dict[str, Any] = {
        "chart_type": None,
        "x": None,
        "y": None,
        "title": None,
        "reason": "no_rows",
        "enabled": False,
    }
    try:
        if not rows:
            return disabled

        dp_raw = semantic_meta.get("domain_pattern") if isinstance(semantic_meta, dict) else None
        dp: dict[str, Any] = dp_raw if isinstance(dp_raw, dict) else {}
        pat = dp.get("pattern") if dp.get("matched") else None

        gc = (group_col or "").strip() if isinstance(group_col, str) else (group_col or None)
        if not gc:
            return {
                "chart_type": "table",
                "x": None,
                "y": None,
                "title": "Results",
                "reason": "no_group_column",
                "enabled": True,
            }

        prefer_x: Optional[list[str]] = None
        if pat == "top_genres":
            prefer_x = ["listed_in", "group_key"]

        x_key = _chart_infer_category_key(rows, gc, prefer=prefer_x)
        y_key = _chart_infer_value_key(rows, operation, metric_col)

        title = "Results"
        reason = "grouped_aggregate"
        chart_type: Optional[str] = None

        if pat == "release_trend" and gc == "release_year":
            chart_type, title, reason = "line", "Releases over time", "domain_release_trend"
        elif pat == "compare_type":
            chart_type, title, reason = "pie", "Movies vs TV shows", "domain_compare_type"
        elif pat == "top_genres":
            chart_type, title, reason = "bar", "Top genres", "domain_top_genres"
        elif pat == "top_countries":
            chart_type, title, reason = "bar", "Content by country", "domain_top_countries"
        elif pat == "longest_movies":
            chart_type, title, reason = "bar", "Longest titles", "domain_longest_movies"
        elif operation in ("top", "count", "sum", "avg"):
            chart_type, title, reason = "bar", "Grouped results", "generic_grouped_aggregate"
        else:
            return {
                "chart_type": "table",
                "x": x_key,
                "y": y_key,
                "title": title,
                "reason": "non_aggregated_or_unsupported_operation",
                "enabled": True,
            }

        if chart_type in ("bar", "line", "pie") and (not x_key or not y_key):
            return {
                "chart_type": "table",
                "x": x_key,
                "y": y_key,
                "title": title,
                "reason": "unexpected_row_shape",
                "enabled": True,
            }

        return {
            "chart_type": chart_type,
            "x": x_key,
            "y": y_key,
            "title": title,
            "reason": reason,
            "enabled": True,
        }
    except Exception as exc:
        return {
            "chart_type": None,
            "x": None,
            "y": None,
            "title": None,
            "reason": f"chart_suggestion_error:{type(exc).__name__}",
            "enabled": False,
        }


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


def _localized_control_response(kind: str, lang: str, *, dataset_name: str | None = None) -> str:
    if kind == "safety":
        return _lt(
            lang,
            "Я могу предоставлять достоверные аналитические ответы только на основе выбранного датасета. Пожалуйста, задавайте вопросы, связанные с загруженными данными или возможностями платформы.",
            "I can only provide reliable analytical insights based on the currently selected dataset. Please ask questions related to the uploaded data or the platform's capabilities.",
            "Мен тек таңдалған деректер жиыны негізінде сенімді аналитикалық жауаптар бере аламын. Өтінемін, жүктелген деректерге немесе платформаның мүмкіндіктеріне қатысты сұрақтар қойыңыз.",
        )
    if kind == "general":
        return _lt(
            lang,
            "Данная платформа предназначена для аналитики на основе данных, а не для ответов на вопросы общего характера. Пожалуйста, задавайте вопросы, связанные с выбранным датасетом или самой платформой.",
            "This platform is designed for dataset-driven analytics rather than general knowledge assistance. Please ask questions related to the selected dataset or the platform itself.",
            "Бұл платформа жалпы білім сұрақтарына жауап беру үшін емес, деректерге негізделген аналитика үшін әзірленген. Өтінемін, таңдалған деректер жиынына немесе платформаның өзіне қатысты сұрақтар қойыңыз.",
        )
    if kind == "capabilities":
        return _lt(
            lang,
            "Я могу анализировать датасеты с помощью запросов на естественном языке, строить визуализации, выявлять закономерности, создавать сводки по данным и помогать пользователям исследовать данные без знания SQL.",
            "I can analyze datasets using natural language queries, generate visualizations, identify trends, summarize large datasets, and assist users in exploring data without requiring SQL knowledge.",
            "Мен табиғи тілдегі сұраныстар арқылы деректер жиындарын талдай аламын, визуализациялар құра аламын, үрдістерді анықтай аламын және SQL білімінсіз деректерді зерттеуге көмектесе аламын.",
        )
    if kind == "current_dataset":
        name = dataset_name or ""
        return _lt(
            lang,
            f"В настоящее время я работаю с датасетом '{name}'. Вы можете задавать аналитические вопросы, связанные с его содержимым.",
            f"I am currently working with the dataset '{name}'. You may ask analytical questions related to its contents.",
            f"Қазіргі уақытта мен '{name}' деректер жиынымен жұмыс істеп жатырмын. Оның мазмұнына қатысты аналитикалық сұрақтар қоя аласыз.",
        )
    if kind == "clarify":
        return _lt(
            lang,
            "Пожалуйста, уточните, какой аспект данных вы хотите проанализировать. Например: тренды, распределения, сравнения, взаимосвязи или сводную информацию.",
            "Could you clarify which aspect of the dataset you would like to analyze? For example, trends, distributions, comparisons, correlations, or summaries.",
            "Қандай талдау түрі қажет екенін нақтылаңыз. Мысалы: үрдістер, үлестірімдер, салыстырулар, байланыстар немесе жиынтық ақпарат.",
        )
    return _localized_control_response("safety", lang)


def _control_payload(req: AnswerRequest, lang: str, kind: str, *, dataset_name: str | None = None) -> dict[str, Any]:
    return {
        "dataset_id": req.dataset_id,
        "interpreted": {
            "operation": "control_response",
            "intent": kind,
            "used_ml_intent": False,
            "confidence_threshold": req.options.confidence_threshold,
            "chart": {"enabled": False, "chart_type": None, "x": None, "y": None, "title": None},
        },
        "rows": [],
        "answer_text": _localized_control_response(kind, lang, dataset_name=dataset_name),
    }


def _detect_control_intent(query: str) -> str | None:
    q = _normalize(query)
    if not q:
        return "clarify"
    if re.search(r"\b(what can you do|what are your capabilities|capabilities|help)\b", q, re.IGNORECASE):
        return "capabilities"
    if re.search(r"\b(what dataset are you analyzing|what data are you working with|current dataset|selected dataset)\b", q, re.IGNORECASE):
        return "current_dataset"
    if re.search(r"\b(who is|who wrote|what is\s+2\s*\+\s*2|exchange rate|president of|weather today|capital of)\b", q, re.IGNORECASE):
        return "general"
    if re.search(r"(кто президент|кто написал|курс валют|сколько будет|погода|столица|кім президент|кім жазды|валюта бағамы|ауа райы|астанасы)", q, re.IGNORECASE):
        return "general"
    if re.fullmatch(r"(analy[sz]e|analysis|анализ|проанализируй|талдау|талда)", q, re.IGNORECASE):
        return "clarify"
    return None


def _query_relevant_to_dataset(
    q_lex: str,
    cols: list[tuple[str, str]],
    semantic_meta: dict[str, Any],
    semantic_layer_result: Optional[AnalyticsSemanticResult],
) -> bool:
    q = _normalize(q_lex)
    if not q:
        return False

    for col, _typ in cols:
        aliases = {col.lower(), *_col_aliases(col)}
        if any(alias and re.search(rf"\b{re.escape(alias.lower())}\b", q, re.IGNORECASE) for alias in aliases):
            return True

    bindings = semantic_meta.get("column_bindings") or {}
    grounding_methods = {"exact_in_query", "alias", "fuzzy", "pattern_po", "semantic_layer_v1"}
    if any(
        isinstance(v, dict)
        and v.get("column")
        and (v.get("method") in grounding_methods or float(v.get("score") or 0.0) >= 0.72)
        for v in bindings.values()
    ):
        return True

    if semantic_layer_result is not None:
        if semantic_layer_result.confidence >= 0.45 and (
            semantic_layer_result.detected_metric
            or semantic_layer_result.detected_dimension
            or semantic_layer_result.detected_date_column
        ):
            return True
        if semantic_layer_result.confidence >= 0.45 and semantic_layer_result.detected_filters:
            return True

    analytic_terms = (
        "average", "avg", "mean", "count", "sum", "total", "top", "trend", "distribution",
        "compare", "correlation", "summary", "min", "max", "show", "list",
        "сред", "колич", "сколько", "сумм", "топ", "тренд", "распредел", "сравн", "свод",
        "орташа", "саны", "қанша", "барлығы", "топ", "үрдіс", "үлестір", "салыстыр", "жиынтық",
    )
    return any(term in q for term in analytic_terms)


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

    if operation == "min" and len(rows) == 1 and "min" in rows[0]:
        v = _fmt_answer_num(rows[0]["min"])
        base = _lt(
            lang,
            f"Минимальное значение по столбцу «{metric_col}» = {v}.",
            f"Minimum value for column '{metric_col}' = {v}.",
            f"«{metric_col}» бағаны бойынша ең кіші мән = {v}.",
        )
        if cond_human:
            base += f" Условия: {cond_human}."
        return base

    if operation == "max" and len(rows) == 1 and "max" in rows[0]:
        v = _fmt_answer_num(rows[0]["max"])
        base = _lt(
            lang,
            f"Максимальное значение по столбцу «{metric_col}» = {v}.",
            f"Maximum value for column '{metric_col}' = {v}.",
            f"«{metric_col}» бағаны бойынша ең үлкен мән = {v}.",
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
        key_name = metric_col or _lt(lang, "числу записей", "record count", "жазбалар саны")
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
        shown = col_labels[:6] + (["..."] if len(col_labels) > 6 else [])
        sample_cols = ", ".join(str(c) for c in shown)
        base = _lt(
            lang,
            f"Ниже показана таблица из {n} строк.\nСтолбцы: {sample_cols}.\nПоказаны первые {limit} строк из выбранного датасета.",
            f"Below is a table with {n} rows.\nColumns: {sample_cols}.\nShowing the first {limit} rows from the selected dataset.",
            f"Төменде {n} жолдан тұратын кесте көрсетілген.\nБағандар: {sample_cols}.\nТаңдалған деректер жиынынан алғашқы {limit} жол көрсетілді.",
        )
        if cond_human:
            base += _lt(lang, f"\nФильтр: {cond_human}.", f"\nFilter: {cond_human}.", f"\nСүзгі: {cond_human}.")
        if sort_human:
            base += "\n" + sort_human.strip()
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
    or wrapped import form:
      - {"columns": [...], "dataset_summary": {...}}
    Returns list[(name, type)] or None if unusable.
    """
    if not columns_json:
        return None
    cols_list = unwrap_dataset_columns_json(columns_json)
    if not cols_list:
        return None
    out: list[tuple[str, str]] = []
    for item in cols_list:
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


def _looks_like_document_question(query: str) -> bool:
    q = (query or "").lower()
    return bool(
        re.search(
            r"\b(document|doc|rag|uploaded text|text file|secret|phrase|business goal|context)\b",
            q,
            re.IGNORECASE,
        )
        or re.search(r"\b(документ|документа|текст|файл|секрет|фраз|цель|контекст)\b", q, re.IGNORECASE)
    )


async def _answer_from_uploaded_documents(
    db: AsyncSession,
    dataset_id: int,
    query: str,
    *,
    explain: bool,
) -> Optional[dict[str, Any]]:
    if not _looks_like_document_question(query):
        return None

    hits = await SearchService(db).search(dataset_id=dataset_id, query=query, top_k=3)
    if not hits:
        return None

    snippets = [str(hit.get("text") or "").strip() for hit in hits if str(hit.get("text") or "").strip()]
    if not snippets:
        return None

    answer_text = "\n\n".join(snippets)
    out: dict[str, Any] = {
        "dataset_id": dataset_id,
        "interpreted": {
            "operation": "document_search",
            "source": "uploaded_documents",
            "hits": [
                {
                    "chunk_id": hit.get("chunk_id"),
                    "document_id": hit.get("document_id"),
                    "score": hit.get("score"),
                }
                for hit in hits
            ],
            "chart": {"enabled": False, "chart_type": None, "title": None},
        },
        "rows": [],
        "answer_text": answer_text,
    }
    if explain:
        out["sql"] = None
    return out


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

    control_intent = _detect_control_intent(qtext)
    if control_intent is not None:
        return _control_payload(req, user_lang, control_intent, dataset_name=ds.name)

    # Load schema before language validation so Latin dataset column names in localized
    # queries (for example "release_year бойынша ...") do not make the query look English.
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

    canonicalized_suggestion_query = _normalize(_canonicalize_localized_suggestion_query(qtext, cols))
    is_localized_suggestion_query = canonicalized_suggestion_query != qtext
    language_check_text = _strip_dataset_columns_for_language_validation(qtext, cols)
    skip_language_guard = (
        req.options.execution_source == "suggestion"
        or _is_canonical_suggestion_query(qtext)
        or is_localized_suggestion_query
    )
    ok_lang, reason = (True, "") if skip_language_guard else validate_query_language(language_check_text, user_lang)
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

    qtext = canonicalized_suggestion_query
    canonical_suggestion = _resolve_canonical_suggestion_query(qtext, cols)

    document_answer = await _answer_from_uploaded_documents(
        db,
        req.dataset_id,
        qtext,
        explain=bool(req.options.explain),
    )
    if document_answer is not None:
        if transcribed_raw is not None:
            document_answer["voice"] = {
                "job_id": req.input.job_id,
                "transcribed_text": transcribed_raw,
            }
        return document_answer

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
    if canonical_suggestion:
        semantic_meta["canonical_suggestion"] = canonical_suggestion

    # Optional semantic layer: snapshot (always when run) + safe assist (fills gaps only; does not drive operation/SQL).
    semantic_layer_result: Optional[AnalyticsSemanticResult] = None
    q_norm = qtext
    try:
        semantic_layer_result = analyze_query_semantics(
            query=q_norm,
            columns=cols,
            columns_json=meta.columns_json if meta and meta.columns_json else None,
            user_lang=(getattr(user, "preferred_language", None) or "en"),
        )
        semantic_meta["semantic_layer_v1"] = asdict(semantic_layer_result)
    except Exception as e:
        semantic_meta["semantic_layer_v1_error"] = str(e)

    # 1) Intent from ML — на тексте после лексических правок (ASR → «amount» и т.д.)
    intent_result = predict_intent(q_lex)
    # ожидаем формат: {"intent": "...", "confidence": 0.0..1.0, "top_k": [...]}
    ml_intent = intent_result.get("intent", "fallback")
    if ml_intent == "unknown":
        ml_intent = "fallback"
    ml_conf = float(intent_result.get("confidence", 0.0))

    # 2) Fallback logic: если низкая уверенность — используем эвристику
    threshold = float(req.options.confidence_threshold)
    dataset_relevance = _query_relevant_to_dataset(q_lex, cols, semantic_meta, semantic_layer_result)
    semantic_meta["dataset_relevance"] = dataset_relevance
    if ml_conf < threshold and not dataset_relevance:
        return _control_payload(req, user_lang, "safety", dataset_name=ds.name)

    operation = (
        ml_intent
        if (ml_intent != "fallback" and ml_conf >= threshold)
        else _detect_operation_heuristic(q_lex, user_lang)
    )
    if canonical_suggestion:
        suggestion_intent = str(canonical_suggestion.get("intent") or "")
        suggestion_col = canonical_suggestion.get("column")
        metric_col = None
        group_col = None
        semantic_meta["operation_override"] = "canonical_suggestion"
        semantic_meta["canonical_suggestion_intent"] = suggestion_intent
        if suggestion_intent == "average" and isinstance(suggestion_col, str):
            operation = "avg"
            metric_col = suggestion_col
        elif suggestion_intent == "count_by" and isinstance(suggestion_col, str):
            operation = "count"
            group_col = suggestion_col
        elif suggestion_intent == "trend_by" and isinstance(suggestion_col, str):
            operation = "count"
            group_col = suggestion_col
            semantic_meta["canonical_order_by_group_key"] = "ASC"
        elif suggestion_intent == "sample" and isinstance(suggestion_col, str):
            operation = "select"
            semantic_meta["select_columns"] = [suggestion_col]
        elif suggestion_intent == "top_values" and isinstance(suggestion_col, str):
            operation = "top"
            group_col = suggestion_col
        elif suggestion_intent == "min" and isinstance(suggestion_col, str):
            operation = "min"
            metric_col = suggestion_col
        elif suggestion_intent == "max" and isinstance(suggestion_col, str):
            operation = "max"
            metric_col = suggestion_col
        elif suggestion_intent == "sum" and isinstance(suggestion_col, str):
            operation = "sum"
            metric_col = suggestion_col
        elif suggestion_intent == "first_rows":
            operation = "select"
        semantic_meta["resolved_metric"] = metric_col
        semantic_meta["resolved_group_by"] = group_col

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

    if not canonical_suggestion and _should_force_select_for_table_rows(q_lex, operation):
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

    explicit_avg_by = None if canonical_suggestion else _resolve_explicit_average_by(q_lex, cols)
    if explicit_avg_by:
        metric_col, group_col = explicit_avg_by
        operation = "avg"
        semantic_meta["resolved_metric"] = metric_col
        semantic_meta["resolved_group_by"] = group_col
        semantic_meta["operation_override"] = "explicit_average_by"
    else:
        explicit_top_values = None if canonical_suggestion else _resolve_explicit_top_values(q_lex, cols)
        if explicit_top_values:
            operation = "top"
            metric_col = None
            group_col = explicit_top_values
            semantic_meta["resolved_metric"] = None
            semantic_meta["resolved_group_by"] = group_col
            semantic_meta["operation_override"] = "explicit_top_values"

    # Safe assist mode (not full semantic control): only fill metric/group when still missing
    # after legacy resolution + sanitization, and only above confidence thresholds.
    sl_assist: dict[str, Any] = {
        "enabled": semantic_layer_result is not None,
        "metric_used": False,
        "group_used": False,
        "reason": "not_needed",
    }
    if semantic_layer_result is None:
        sl_assist["enabled"] = False
        sl_assist["reason"] = "low_confidence" if semantic_meta.get("semantic_layer_v1_error") else "not_needed"
    else:
        dm = semantic_layer_result.detected_metric
        dd = semantic_layer_result.detected_dimension
        need_metric = not (metric_col and str(metric_col).strip())
        need_group = not (group_col and str(group_col).strip())
        if canonical_suggestion:
            need_metric = False
            need_group = False
        elif semantic_meta.get("operation_override") == "explicit_top_values":
            need_metric = False
        filled_m = (
            need_metric
            and dm is not None
            and float(dm.score) >= 0.65
            and dm.column in col_set
        )
        filled_g = (
            need_group
            and dd is not None
            and float(dd.score) >= 0.60
            and dd.column in col_set
        )
        if filled_m:
            metric_col = dm.column
            sl_assist["metric_used"] = True
        if filled_g:
            group_col = dd.column
            sl_assist["group_used"] = True
        if filled_m or filled_g:
            if filled_m and filled_g:
                sl_assist["reason"] = "filled_missing_metric"
            elif filled_m:
                sl_assist["reason"] = "filled_missing_metric"
            else:
                sl_assist["reason"] = "filled_missing_group"
        elif need_metric or need_group:
            sl_assist["reason"] = "low_confidence"
        else:
            sl_assist["reason"] = "not_needed"
        if sl_assist["metric_used"] or sl_assist["group_used"]:
            semantic_meta["resolved_metric"] = metric_col
            semantic_meta["resolved_group_by"] = group_col
    semantic_meta["semantic_layer_assist"] = sl_assist

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

    col_names_list = [c[0] for c in cols]
    numeric_set_names = _numeric_col_names(cols)
    delta_order_cols: Optional[tuple[str, str]] = None
    if _wants_largest_score_delta_from_previous(q_lex):
        dp = _find_current_and_previous_score_columns(col_names_list, numeric_set_names)
        if dp and dp[0] in col_set and dp[1] in col_set:
            delta_order_cols = dp

    dual_extreme: Optional[tuple[str, int]] = None
    row_rank: Optional[tuple[Any, ...]] = None
    if not delta_order_cols:
        row_rank = _resolve_top_n_or_extreme_row_ranking(
            q_lex, operation, group_col, metric_col, cols, col_set, limit
        )
    else:
        operation = "select"
        group_col = None
        semantic_meta["resolved_group_by"] = None
        order_by = None
        semantic_meta["operation_override"] = "score_delta_vs_previous"

    if row_rank:
        mode = row_rank[0]
        if mode == "dual":
            _, oc_half, half_lim = row_rank
            if oc_half in col_set:
                dual_extreme = (oc_half, half_lim)
                operation = "select"
                group_col = None
                semantic_meta["resolved_group_by"] = None
                order_by = None
                limit = half_lim * 2
                semantic_meta["operation_override"] = (
                    semantic_meta.get("operation_override") or "row_ranking_dual_extreme"
                )
        elif mode == "single":
            _, oc, od, lim_rank = row_rank
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

    netflix = _detect_netflix_query_pattern(q_lex, col_set, semantic_layer_result)
    semantic_meta["domain_pattern"] = netflix
    if netflix.get("matched"):
        oo = semantic_meta.get("operation_override")
        if oo in _NETFLIX_CONFLICT_OVERRIDES:
            netflix = {
                **netflix,
                "matched": False,
                "pattern": None,
                "operation": None,
                "metric_col": None,
                "group_col": None,
                "order_by": None,
                "debug": {**(netflix.get("debug") or {}), "skipped": "operation_override", "override": oo},
            }
            semantic_meta["domain_pattern"] = netflix
        else:
            g_n = netflix.get("group_col")
            m_n = netflix.get("metric_col")
            op_n = netflix.get("operation")
            if not g_n or g_n not in col_set:
                netflix = {
                    **netflix,
                    "matched": False,
                    "pattern": None,
                    "operation": None,
                    "metric_col": None,
                    "group_col": None,
                    "order_by": None,
                    "debug": {**(netflix.get("debug") or {}), "reason": "group_col_not_in_schema"},
                }
                semantic_meta["domain_pattern"] = netflix
            elif m_n is not None and m_n not in col_set:
                netflix = {
                    **netflix,
                    "matched": False,
                    "pattern": None,
                    "operation": None,
                    "metric_col": None,
                    "group_col": None,
                    "order_by": None,
                    "debug": {**(netflix.get("debug") or {}), "reason": "metric_col_not_in_schema"},
                }
                semantic_meta["domain_pattern"] = netflix
            else:
                operation = str(op_n)
                group_col = g_n
                metric_col = m_n
                semantic_meta["resolved_metric"] = metric_col
                semantic_meta["resolved_group_by"] = group_col
                if not semantic_meta.get("operation_override"):
                    semantic_meta["operation_override"] = "netflix_domain_pattern"

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
    if dual_extreme:
        filter_interpreted["dual_extreme"] = {
            "column": dual_extreme[0],
            "per_direction_limit": dual_extreme[1],
        }
    if delta_order_cols:
        c_new, c_prev = delta_order_cols
        filter_interpreted["order_delta"] = {
            "current_column": c_new,
            "previous_column": c_prev,
            "expression": f"`{c_new}` - `{c_prev}`",
            "direction": "DESC",
        }

    dp_dom = semantic_meta.get("domain_pattern") or {}
    if dp_dom.get("matched") and dp_dom.get("order_by"):
        filter_interpreted["domain_order"] = {
            "pattern": dp_dom.get("pattern"),
            "order_by": dp_dom.get("order_by"),
        }

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
    select_columns_raw = semantic_meta.get("select_columns")
    select_columns = (
        [c for c in select_columns_raw if isinstance(c, str) and c in col_set]
        if isinstance(select_columns_raw, list)
        else []
    )
    select_sql = ", ".join(f"`{c}`" for c in select_columns) if select_columns else "*"
    if operation == "select" and order_by:
        oc, od = order_by
        if oc in col_set:
            order_sql_select = f" ORDER BY `{oc}` {od} "

    nf_dom = semantic_meta.get("domain_pattern") or {}
    nf_pat = nf_dom.get("pattern")

    if operation == "top" and nf_pat == "longest_movies" and group_col and metric_col == "duration":
        expr_sql = _netflix_duration_agg_expr_sql(metric_col)
        long_where = _append_nonempty_group_where_clause(where_sql, group_col)
        if "type" in col_set:
            cond = "LOWER(TRIM(CAST(`type` AS CHAR))) = 'movie'"
            if long_where.strip():
                long_where = long_where.rstrip() + f" AND {cond} "
            else:
                long_where = f" WHERE {cond} "
        sql = f"""
            SELECT `{group_col}` AS group_key, {expr_sql} AS `sum`
            FROM `{table_name}`
            {long_where}
            GROUP BY `{group_col}`
            ORDER BY `sum` DESC
            LIMIT {limit}
        """

    elif (
        operation == "top"
        and nf_pat == "top_genres"
        and group_col == "listed_in"
        and "listed_in" in col_set
    ):
        sql, _genre_strat, _genre_split = _netflix_top_genres_listed_in_split_sql(
            table_name, where_sql, limit, col_set
        )
        _dp_nf = dict(semantic_meta.get("domain_pattern") or {})
        _dbg_nf = dict(_dp_nf.get("debug") or {})
        _dbg_nf["genre_split"] = bool(_genre_split)
        _dbg_nf["genre_split_strategy"] = _genre_strat
        if _genre_strat == "fallback_grouped_string":
            _dbg_nf["genre_split_fallback"] = "recursive_cte_unsupported_or_mysql_without_show_id"
        _dp_nf["debug"] = _dbg_nf
        semantic_meta["domain_pattern"] = _dp_nf

    elif operation == "top":
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
            group_where = _append_nonempty_group_where_clause(where_sql, group_col)
            sql = f"""
                SELECT `{group_col}` AS group_key, {agg_expr} AS `{alias}`
                FROM `{table_name}`
                {group_where}
                GROUP BY `{group_col}`
                {order_sql}
                LIMIT {limit}
            """

    elif operation == "count":
        if group_col:
            group_where = _append_nonempty_group_where_clause(where_sql, group_col)
            ord_sql = ""
            if nf_dom.get("matched") and nf_dom.get("order_by") == "release_year_asc":
                ord_sql = " ORDER BY `group_key` ASC "
            elif nf_dom.get("matched") and nf_dom.get("order_by") == "count_desc":
                ord_sql = " ORDER BY `count` DESC "
            elif semantic_meta.get("canonical_order_by_group_key") == "ASC":
                ord_sql = " ORDER BY `group_key` ASC "
            sql = f"""
                SELECT `{group_col}` AS group_key, COUNT(*) AS `count`
                FROM `{table_name}`
                {group_where}
                GROUP BY `{group_col}`
                {ord_sql}
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

    elif operation == "min":
        if not metric_col:
            raise HTTPException(status_code=400, detail="cannot min without metric column")
        sql = f"""
            SELECT MIN(`{metric_col}`) AS `min`
            FROM `{table_name}`
            {where_sql}
            LIMIT 1
        """

    elif operation == "max":
        if not metric_col:
            raise HTTPException(status_code=400, detail="cannot max without metric column")
        sql = f"""
            SELECT MAX(`{metric_col}`) AS `max`
            FROM `{table_name}`
            {where_sql}
            LIMIT 1
        """

    elif operation == "sum":
        if not metric_col:
            raise HTTPException(status_code=400, detail="cannot sum without metric column")

        if group_col:
            group_where = _append_nonempty_group_where_clause(where_sql, group_col)
            sql = f"""
                SELECT `{group_col}` AS group_key, SUM(`{metric_col}`) AS `sum`
                FROM `{table_name}`
                {group_where}
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
            group_where = _append_nonempty_group_where_clause(where_sql, group_col)
            sql = f"""
                SELECT `{group_col}` AS group_key, AVG(`{metric_col}`) AS `avg`
                FROM `{table_name}`
                {group_where}
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
        # plain rows (опционально: нижние + верхние строки по одной метрике)
        if delta_order_cols:
            ca, cb = delta_order_cols
            sql = (
                f"SELECT {select_sql} FROM `{table_name}` {where_sql} "
                f"ORDER BY (`{ca}` - `{cb}`) DESC LIMIT {limit}"
            )
        elif dual_extreme:
            oc, half = dual_extreme
            sql = (
                f"(SELECT {select_sql} FROM `{table_name}` {where_sql} ORDER BY `{oc}` ASC LIMIT {half}) "
                f"UNION ALL "
                f"(SELECT {select_sql} FROM `{table_name}` {where_sql} ORDER BY `{oc}` DESC LIMIT {half})"
            )
        else:
            sql = f"SELECT {select_sql} FROM `{table_name}` {where_sql} {order_sql_select} LIMIT {limit}"

    res = await db.execute(text(sql), params)
    rows = [dict(r._mapping) for r in res.fetchall()]

    _dp_after = semantic_meta.get("domain_pattern") or {}
    if _dp_after.get("matched") and _dp_after.get("pattern") == "top_genres":
        for _r in rows:
            if "listed_in" in _r and "group_key" not in _r:
                _r["group_key"] = _r["listed_in"]

    try:
        interpreted["chart"] = _build_chart_suggestion(
            operation, group_col, metric_col, rows, semantic_meta, limit
        )
    except Exception:
        interpreted["chart"] = {
            "chart_type": None,
            "x": None,
            "y": None,
            "title": None,
            "reason": "chart_attach_failed",
            "enabled": False,
        }

    col_labels = [c[0] for c in cols]
    answer_col_labels = select_columns or col_labels
    answer_text = _build_answer_text(
        operation,
        rows,
        lang=user_lang,
        group_col=group_col,
        metric_col=metric_col,
        limit=limit,
        filter_interpreted=filter_interpreted,
        col_labels=answer_col_labels,
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


@router.post(
    "/semantic-debug",
    response_model=SemanticDebugResponse,
    summary="Debug NL semantics vs analytics layer (no SQL)",
    tags=["Query Debug"],
)
async def semantic_debug(
    body: SemanticDebugRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> SemanticDebugResponse:
    """
    Compare ``resolve_metric_and_group`` with ``analyze_query_semantics`` for a dataset.

    Authenticated like ``/query/answer``; does **not** run answer/analytics SQL.
    If ``DatasetTableMeta.columns_json`` is empty, the same metadata introspection
    as ``/query/answer`` may run (e.g. ``INFORMATION_SCHEMA`` / ``PRAGMA``) to load column names.
    """
    user_lang = normalize_preferred_language(getattr(user, "preferred_language", "ru"))
    try:
        ds = (
            await db.execute(select(Dataset).where(Dataset.id == body.dataset_id, Dataset.user_id == user.id))
        ).scalar_one_or_none()
        if not ds:
            raise HTTPException(
                status_code=404,
                detail=_lt(user_lang, "датасет не найден", "dataset not found", "датасет табылмады"),
            )

        qtext = _normalize(body.text)
        if not qtext:
            raise HTTPException(status_code=400, detail="empty text after normalization")

        table_name = f"ds_{body.dataset_id}_data"
        meta = (
            await db.execute(select(DatasetTableMeta).where(DatasetTableMeta.dataset_id == body.dataset_id))
        ).scalar_one_or_none()

        cols: Optional[list[tuple[str, str]]] = None
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

        legacy = SemanticDebugLegacySemantics()
        try:
            sem = resolve_metric_and_group(cols, qtext)
            legacy = SemanticDebugLegacySemantics(
                resolved_metric=sem.get("metric_col"),
                resolved_group_by=sem.get("group_col"),
                query_after_lexical_fixes=sem.get("query_lexical") or "",
                debug=dict(sem.get("debug") or {}),
            )
        except Exception as e:
            legacy = SemanticDebugLegacySemantics(error=str(e))

        semantic_layer_v1: Optional[dict[str, Any]] = None
        semantic_layer_error: Optional[str] = None
        sl_result: Optional[AnalyticsSemanticResult] = None
        try:
            sl_result = analyze_query_semantics(
                query=qtext,
                columns=cols,
                columns_json=meta.columns_json if meta and meta.columns_json else None,
                user_lang=(getattr(user, "preferred_language", None) or "en"),
            )
            semantic_layer_v1 = asdict(sl_result)
        except Exception as e:
            semantic_layer_error = str(e)

        leg_m = legacy.resolved_metric
        leg_g = legacy.resolved_group_by
        sem_m = sl_result.detected_metric.column if sl_result and sl_result.detected_metric else None
        sem_g = sl_result.detected_dimension.column if sl_result and sl_result.detected_dimension else None

        # When the semantic layer did not run, ``metric_same`` / ``group_same`` are false (not comparable).
        if sl_result is None:
            comparison = SemanticDebugComparison(
                metric_same=False,
                group_same=False,
                semantic_metric_better_candidate=None,
                semantic_group_better_candidate=None,
            )
        else:
            metric_same = leg_m == sem_m
            group_same = leg_g == sem_g
            metric_candidate = None if metric_same else sem_m
            group_candidate = None if group_same else sem_g
            comparison = SemanticDebugComparison(
                metric_same=metric_same,
                group_same=group_same,
                semantic_metric_better_candidate=metric_candidate,
                semantic_group_better_candidate=group_candidate,
            )

        return SemanticDebugResponse(
            dataset_id=body.dataset_id,
            query=qtext,
            table_name=table_name,
            legacy_semantics=legacy,
            semantic_layer_v1=semantic_layer_v1,
            semantic_layer_error=semantic_layer_error,
            comparison=comparison,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=_lt(
                user_lang,
                f"ошибка semantic-debug: {e!s}",
                f"semantic-debug error: {e!s}",
                f"semantic-debug қатесі: {e!s}",
            ),
        ) from e
