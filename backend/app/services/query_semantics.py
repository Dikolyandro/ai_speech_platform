"""
Семантический слой для NL→таблица: нормализация после ASR, сопоставление с реальными колонками.

Цель — устойчивость к «амаунт», «категории» и опечаткам без изменения схемы БД.
"""

from __future__ import annotations

import difflib
import re
from typing import Any, Optional

# Типичные искажения распознавания / разговорные замены → лексемы как в CSV (латиница)
_ASR_LEXICAL_FIXES: list[tuple[str, str]] = [
    (r"\bамаунт\b", "amount"),
    (r"\bэмоунт\b", "amount"),
    (r"\bемаунт\b", "amount"),
    (r"\bамонт\b", "amount"),
    (r"\bсум\b", "sum"),
    (r"\bкатегори[ияеи]\b", "category"),
    (r"\bкатегориям\b", "category"),
    (r"\bкатегорий\b", "category"),
    (r"\bгод[уае]?\b", "year"),
    (r"\bпо\s+году\b", "year"),
]

# Русские подсказки к именам колонок (если такая колонка есть в таблице)
_COLUMN_ALIASES_TO_CANON: dict[str, list[str]] = {
    "amount": ["сумма", "выручка", "итог", "деньги"],
    "category": ["категория", "класс", "тип", "группа"],
    "year": ["год"],
    "price": ["цена", "стоимость"],
    "qty": ["количество", "число", "шт"],
    "quantity": ["количество", "число"],
}


def normalize_query_lexical(query: str) -> str:
    """После lower+strip: подмена частых ASR-ошибок на токены, совпадающие с именами колонок."""
    q = query.strip().lower()
    q = re.sub(r"\s+", " ", q)
    for pat, repl in _ASR_LEXICAL_FIXES:
        q = re.sub(pat, repl, q, flags=re.IGNORECASE)
    return q


def _is_numeric_sql_type(typ: str) -> bool:
    t = (typ or "").lower()
    for k in ("int", "decimal", "float", "double", "real", "numeric", "money", "bigint", "smallint"):
        if k in t:
            return True
    return False


def _split_schema(cols: list[tuple[str, str]]) -> tuple[list[str], list[str]]:
    names = [c[0] for c in cols]
    numeric = [c[0] for c in cols if _is_numeric_sql_type(c[1])]
    return names, numeric


def _word_fuzzy_score(a: str, b: str) -> float:
    return float(difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio())


def _best_fuzzy_token_match(target: str, query_words: list[str], min_score: float) -> Optional[float]:
    best = 0.0
    for w in query_words:
        if len(w) < 2:
            continue
        best = max(best, _word_fuzzy_score(w, target))
    return best if best >= min_score else None


def resolve_metric_and_group(
    cols: list[tuple[str, str]],
    query_normalized: str,
) -> dict[str, Any]:
    """
    Возвращает metric_col, group_col и объяснение, как сопоставили с колонками схемы.
    """
    col_names, numeric_cols = _split_schema(cols)
    col_set = set(col_names)
    q_lex = normalize_query_lexical(query_normalized)
    q_words = re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9_]+", q_lex)

    bindings: dict[str, Any] = {}

    def bind_metric(name: str, method: str, detail: str) -> None:
        bindings["metric"] = {"column": name, "method": method, "detail": detail}

    def bind_group(name: str, method: str, detail: str) -> None:
        bindings["group_by"] = {"column": name, "method": method, "detail": detail}

    metric_col: Optional[str] = None
    group_col: Optional[str] = None

    # --- metric: exact mention in lexical query
    for name in numeric_cols:
        if name.lower() in q_lex:
            metric_col = name
            bind_metric(name, "exact_in_query", f"substring `{name}`")
            break

    # --- metric: alias (рус.) → каноническое имя колонки
    if metric_col is None:
        for canon, aliases in _COLUMN_ALIASES_TO_CANON.items():
            if canon not in col_set:
                continue
            for al in aliases:
                if al in q_lex:
                    metric_col = canon
                    bind_metric(canon, "alias", f"«{al}» → `{canon}`")
                    break
            if metric_col:
                break

    # --- metric: fuzzy to numeric column names
    if metric_col is None:
        best_name = None
        best_score = 0.0
        for name in numeric_cols:
            s = _best_fuzzy_token_match(name, q_words, 0.72)
            if s is not None and s > best_score:
                best_score = s
                best_name = name
        if best_name:
            metric_col = best_name
            bind_metric(best_name, "fuzzy", f"score≈{best_score:.2f}")

    # --- metric: приоритетные имена
    if metric_col is None:
        for cand in ("amount", "price", "total", "qty", "quantity"):
            if cand in col_set and cand in numeric_cols:
                metric_col = cand
                bind_metric(cand, "default_priority", f"fallback `{cand}`")
                break
    if metric_col is None and numeric_cols:
        metric_col = numeric_cols[0]
        bind_metric(metric_col, "first_numeric", "first numeric column in schema")

    # --- group_by: по <col>
    m = re.search(r"\b(по|by|бойынша)\s+([a-zA-Zа-яА-ЯёЁ_][\w]*)", q_lex)
    if m:
        cand = m.group(2)
        for name in col_names:
            if name.lower() == cand.lower():
                group_col = name
                bind_group(name, "pattern_po", "по/by + identifier")
                break

    # --- group_by: exact column name in query
    if group_col is None:
        for name in col_names:
            if name == metric_col:
                continue
            if name.lower() in q_lex:
                group_col = name
                bind_group(name, "exact_in_query", f"substring `{name}`")
                break

    # --- group_by: alias
    if group_col is None:
        for canon, aliases in _COLUMN_ALIASES_TO_CANON.items():
            if canon not in col_set or canon == metric_col:
                continue
            for al in aliases:
                if al in q_lex:
                    group_col = canon
                    bind_group(canon, "alias", f"«{al}» → `{canon}`")
                    break
            if group_col:
                break

    # --- group_by: fuzzy non-numeric columns
    if group_col is None:
        text_cols = [n for n in col_names if n not in numeric_cols or n == metric_col]
        best_name = None
        best_score = 0.0
        for name in text_cols:
            if name == metric_col:
                continue
            s = _best_fuzzy_token_match(name, q_words, 0.68)
            if s is not None and s > best_score:
                best_score = s
                best_name = name
        if best_name:
            group_col = best_name
            bind_group(best_name, "fuzzy", f"score≈{best_score:.2f}")

    return {
        "query_lexical": q_lex,
        "metric_col": metric_col,
        "group_col": group_col,
        "bindings": bindings,
    }

