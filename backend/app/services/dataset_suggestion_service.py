from __future__ import annotations

import re
from typing import Any, Literal

SuggestionIntent = Literal[
    "average",
    "count_by",
    "trend_by",
    "sample",
    "top_values",
    "min",
    "max",
    "sum",
    "first_rows",
]
SuggestionType = Literal["numeric", "categorical", "date", "text", "boolean"]


def _column_name(column: dict[str, Any]) -> str:
    value = column.get("name")
    return value.strip() if isinstance(value, str) else ""


def _column_type(column: dict[str, Any]) -> str:
    return str(column.get("type") or column.get("data_type") or "").lower()


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def _profile_number(column: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    profile = column.get("profile")
    if not isinstance(profile, dict):
        return None
    for key in keys:
        value = profile.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return None


def _is_numeric(column: dict[str, Any]) -> bool:
    typ = _column_type(column)
    return bool(re.search(r"\b(int|integer|bigint|long|double|float|real|decimal|numeric|number)\b", typ))


def _is_numeric_measure(column: dict[str, Any]) -> bool:
    if not _is_numeric(column):
        return False
    name = _norm(_column_name(column))
    return not (
        name.endswith("id")
        or name
        in {
            "vendorid",
            "ratecodeid",
            "pulocationid",
            "dolocationid",
            "paymenttype",
            "storeandfwdflag",
        }
    )


def _is_date(column: dict[str, Any]) -> bool:
    typ = _column_type(column)
    name = _column_name(column).lower()
    return "date" in typ or "time" in typ or any(token in name for token in ("date", "time", "year", "month"))


def _is_boolean(column: dict[str, Any]) -> bool:
    typ = _column_type(column)
    if "bool" in typ:
        return True
    name = _norm(_column_name(column))
    if name in {"verified", "hasimage", "image"}:
        return True
    distinct = _profile_number(column, ("distinct", "distinct_count", "unique_count", "n_unique"))
    return bool(distinct is not None and distinct <= 2)


def _is_text(column: dict[str, Any]) -> bool:
    typ = _column_type(column)
    name = _norm(_column_name(column))
    if not re.search(r"(text|string|varchar|char|object)", typ):
        return False
    return any(token in name for token in ("reviewtext", "summary", "description", "comment", "text", "content"))


def _is_categorical(column: dict[str, Any]) -> bool:
    if _is_date(column) or _is_text(column):
        return False
    if _is_boolean(column):
        return True
    if _is_numeric(column):
        distinct = _profile_number(column, ("distinct", "distinct_count", "unique_count", "n_unique"))
        return bool(distinct is not None and distinct <= 40)
    typ = _column_type(column)
    distinct = _profile_number(column, ("distinct", "distinct_count", "unique_count", "n_unique"))
    if distinct is not None and distinct <= 120:
        return True
    return bool(re.search(r"(text|string|varchar|char|object)", typ))


def _canonical_query(intent: SuggestionIntent, column: str | None = None) -> str:
    if intent == "average" and column:
        return f"Show average {column}"
    if intent == "count_by" and column:
        return f"Count records by {column}"
    if intent == "trend_by" and column:
        return f"Show trend by {column}"
    if intent == "sample" and column:
        return f"Show sample values from {column}"
    if intent == "top_values" and column:
        return f"Show top values by {column}"
    if intent == "min" and column:
        return f"Show minimum {column}"
    if intent == "max" and column:
        return f"Show maximum {column}"
    if intent == "sum" and column:
        return f"Show sum of {column}"
    return "Show first rows"


def _display_text(intent: SuggestionIntent, column: str | None = None) -> dict[str, str]:
    canonical = _canonical_query(intent, column)
    if intent == "average" and column:
        return {
            "en": canonical,
            "ru": f"Показать среднее значение по {column}",
            "kk": f"{column} бойынша орташа мәнді көрсету",
        }
    if intent == "count_by" and column:
        return {
            "en": canonical,
            "ru": f"Посчитать количество записей по {column}",
            "kk": f"{column} бойынша жазбалар санын көрсету",
        }
    if intent == "trend_by" and column:
        return {
            "en": canonical,
            "ru": f"Показать тренд по {column}",
            "kk": f"{column} бойынша үрдісті көрсету",
        }
    if intent == "sample" and column:
        return {
            "en": canonical,
            "ru": f"Показать примеры из {column}",
            "kk": f"{column} бағанынан мысалдарды көрсету",
        }
    if intent == "top_values" and column:
        return {
            "en": canonical,
            "ru": f"Показать самые частые значения по {column}",
            "kk": f"{column} бойынша ең жиі мәндерді көрсету",
        }
    if intent == "min" and column:
        return {
            "en": canonical,
            "ru": f"Показать минимальное значение по {column}",
            "kk": f"{column} бойынша ең кіші мәнді көрсету",
        }
    if intent == "max" and column:
        return {
            "en": canonical,
            "ru": f"Показать максимальное значение по {column}",
            "kk": f"{column} бойынша ең үлкен мәнді көрсету",
        }
    if intent == "sum" and column:
        return {
            "en": canonical,
            "ru": f"Показать сумму по {column}",
            "kk": f"{column} бойынша соманы көрсету",
        }
    return {
        "en": canonical,
        "ru": "Показать первые строки",
        "kk": "Алғашқы жолдарды көрсету",
    }


def _make(intent: SuggestionIntent, column: str | None, kind: SuggestionType) -> dict[str, Any]:
    return {
        "intent": intent,
        "column": column,
        "type": kind,
        "category": kind,
        "required_columns": [column] if column else [],
        "displayText": _display_text(intent, column),
        "canonicalQuery": _canonical_query(intent, column),
    }


def build_dataset_suggestions(
    *,
    dataset_id: int,
    columns: list[dict[str, Any]],
    dataset_summary: dict[str, Any] | None = None,
    max_suggestions: int = 36,
    language: str = "en",
) -> list[dict[str, Any]]:
    """Return structured dataset suggestions with localized labels and canonical execution text."""
    del dataset_id, dataset_summary, language
    real_columns = [c for c in columns if isinstance(c, dict) and _column_name(c)]

    buckets: dict[SuggestionType, list[str]] = {
        "numeric": [],
        "categorical": [],
        "date": [],
        "text": [],
        "boolean": [],
    }
    seen_bucket_columns: set[tuple[SuggestionType, str]] = set()

    def add_to_bucket(kind: SuggestionType, name: str) -> None:
        key = (kind, _norm(name))
        if key[1] and key not in seen_bucket_columns:
            seen_bucket_columns.add(key)
            buckets[kind].append(name)

    for column in real_columns:
        name = _column_name(column)
        if not _norm(name):
            continue
        if _is_numeric_measure(column):
            add_to_bucket("numeric", name)
        if _is_date(column):
            add_to_bucket("date", name)
        if _is_boolean(column):
            add_to_bucket("boolean", name)
        elif _is_text(column):
            add_to_bucket("text", name)
        elif _is_categorical(column):
            add_to_bucket("categorical", name)

    suggestions: list[dict[str, Any]] = []
    suggestions.append(_make("first_rows", None, "categorical"))
    for column in buckets["numeric"][:8]:
        suggestions.append(_make("average", column, "numeric"))
    for column in buckets["categorical"][:8]:
        suggestions.append(_make("count_by", column, "categorical"))
    for column in buckets["date"][:6]:
        suggestions.append(_make("trend_by", column, "date"))
    for column in buckets["text"][:6]:
        suggestions.append(_make("sample", column, "text"))
    for column in buckets["boolean"][:4]:
        suggestions.append(_make("count_by", column, "boolean"))
    for column in buckets["numeric"][:8]:
        suggestions.append(_make("min", column, "numeric"))
        suggestions.append(_make("max", column, "numeric"))
        suggestions.append(_make("sum", column, "numeric"))
    for column in buckets["categorical"][:8]:
        suggestions.append(_make("top_values", column, "categorical"))

    return suggestions[:max_suggestions]
