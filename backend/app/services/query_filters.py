"""
Извлечение фильтров и ORDER BY из NL-запроса по реальной схеме таблицы (whitelist колонок).
"""

from __future__ import annotations

import re
from typing import Any, Optional

from app.services.query_semantics import _is_numeric_sql_type

# Слова, которые не считаем значениями фильтра (часть речи / мусор ASR)
_VALUE_STOPWORDS = frozenset(
    {
        "мне",
        "нам",
        "покажи",
        "показать",
        "дай",
        "дайте",
        "хочу",
        "нужно",
        "таблицу",
        "таблица",
        "таблице",
        "строки",
        "строка",
        "записи",
        "запись",
        "данные",
        "данных",
        "результат",
        "результаты",
        "все",
        "всё",
        "только",
        "лишь",
        "полностью",
        "с",
        "со",
        "из",
        "по",
        "для",
        "где",
        "какие",
        "какой",
        "сколько",
        "сорт",
        "сортировка",
        "order",
        "sort",
        "asc",
        "desc",
        "top",
        "топ",
        "лимит",
        "limit",
    }
)

# RU-фраза «имя X» → колонка manager (если есть)
_NAME_TO_MANAGER = re.compile(r"\bимен[иеи]\s+([a-zA-Zа-яА-ЯёЁ0-9_.-]+)\b", re.IGNORECASE)

# Явная просьба «разбить по измерению» — оставляем GROUP BY
_GROUP_INTENT_PATTERNS = [
    re.compile(r"\bпо\s+кажд", re.IGNORECASE),
    re.compile(r"\bдля\s+кажд", re.IGNORECASE),
    re.compile(r"\bгрупп", re.IGNORECASE),
    re.compile(r"\bразбив", re.IGNORECASE),
    re.compile(r"\bраспредел", re.IGNORECASE),
    re.compile(r"\bbreak\s*down\b", re.IGNORECASE),
    re.compile(r"\bgroup\s+by\b", re.IGNORECASE),
]


def explicit_group_breakdown_intent(q_lex: str) -> bool:
    return any(p.search(q_lex) for p in _GROUP_INTENT_PATTERNS)


def explicit_group_by_column(q_lex: str, group_col: str) -> bool:
    """«по channel», «по каналу» и т.п. для конкретной колонки."""
    gl = group_col.lower()
    if re.search(rf"\bпо\s+{re.escape(gl)}\b", q_lex):
        return True
    if re.search(rf"\bby\s+{re.escape(gl)}\b", q_lex):
        return True
    alias = {
        "channel": r"канал\w*",
        "city": r"город\w*",
        "category": r"категор\w*",
        "manager": r"менеджер\w*",
        "status": r"статус\w*",
        "product": r"продукт\w*",
    }.get(gl)
    if alias and re.search(rf"\bпо\s+{alias}\b", q_lex):
        return True
    return False


def should_use_scalar_count_instead_of_group(
    q_lex: str,
    group_col: Optional[str],
    filtered_columns: set[str],
) -> bool:
    if not group_col:
        return False
    if group_col not in filtered_columns:
        return False
    if explicit_group_breakdown_intent(q_lex):
        return False
    if explicit_group_by_column(q_lex, group_col):
        return False
    return True


def _norm_val(raw: str) -> Optional[str]:
    v = (raw or "").strip()
    if len(v) < 1:
        return None
    if v.lower() in _VALUE_STOPWORDS:
        return None
    return v


def _quote_col(name: str) -> str:
    return f"`{name}`"


def _col_aliases(name: str) -> list[str]:
    """
    Build language-agnostic text aliases for arbitrary column names:
      school_type -> ["school_type", "school type", "schooltype"]
      customerCity -> ["customercity", "customer city"]
    """
    raw = (name or "").strip()
    if not raw:
        return []
    low = raw.lower()
    spaced = re.sub(r"[_\-]+", " ", low)
    spaced = re.sub(r"\s+", " ", spaced).strip()
    compact = spaced.replace(" ", "")
    out = {low, spaced, compact}
    # tokenized snake from camel fallback
    camel = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", raw).lower()
    camel = re.sub(r"\s+", " ", camel).strip()
    out.add(camel)
    out.add(camel.replace(" ", ""))
    return sorted(x for x in out if x)


def build_dynamic_filters_and_order(
    cols: list[tuple[str, str]],
    q_lex: str,
    skip_columns: Optional[set[str]] = None,
) -> tuple[list[str], dict[str, Any], list[dict[str, Any]], Optional[tuple[str, str]]]:
    """
    Возвращает:
      - where_fragments: куски для AND (...), уже с плейсхолдерами :df0 ...
      - params: словарь параметров
      - filter_meta: для interpreted
      - order_by: (column, ASC|DESC) или None
    """
    skip_columns = skip_columns or set()
    col_names = [c[0] for c in cols]
    col_by_lower = {c[0].lower(): c[0] for c in cols}
    col_types = {c[0]: (c[1] or "") for c in cols}
    col_alias_to_name: dict[str, str] = {}
    for cname in col_names:
        for alias in _col_aliases(cname):
            col_alias_to_name.setdefault(alias, cname)
    q = q_lex.lower().strip()

    where_fragments: list[str] = []
    params: dict[str, Any] = {}
    meta: list[dict[str, Any]] = []
    used_cols: set[str] = set()
    pidx = 0

    def next_key() -> str:
        nonlocal pidx
        k = f"df{pidx}"
        pidx += 1
        return k

    def add_eq_text(col: str, val: str, reason: str) -> None:
        if col in skip_columns or col in used_cols:
            return
        key = next_key()
        params[key] = val
        where_fragments.append(f"LOWER({_quote_col(col)}) = LOWER(:{key})")
        meta.append({"column": col, "op": "=", "value": val, "reason": reason})
        used_cols.add(col)

    def add_cmp_numeric(col: str, op: str, val: float, reason: str) -> None:
        if col in skip_columns or col in used_cols:
            return
        if not _is_numeric_sql_type(col_types.get(col, "")):
            return
        key = next_key()
        params[key] = val
        where_fragments.append(f"{_quote_col(col)} {op} :{key}")
        meta.append({"column": col, "op": op, "value": val, "reason": reason})
        used_cols.add(col)

    # --- Имя менеджера: «имени томми»
    m = _NAME_TO_MANAGER.search(q)
    if m and "manager" in col_by_lower:
        v = _norm_val(m.group(1))
        if v:
            add_eq_text(col_by_lower["manager"], v, "pattern_imeni")

    # --- «с category X» / «category X» (после лексики)
    m = re.search(r"\bс\s+category\s+([a-zA-Zа-яА-ЯёЁ0-9_.-]+)\b", q)
    if m and "category" in col_by_lower:
        v = _norm_val(m.group(1))
        if v:
            add_eq_text(col_by_lower["category"], v, "pattern_s_category")

    # После лексики: «покажи категорию home» → «покажи category home»
    m = re.search(r"\bcategory\s+([a-zA-Zа-яА-ЯёЁ0-9_.-]+)\b", q)
    if m and "category" in col_by_lower:
        v = _norm_val(m.group(1))
        if v and v.lower() != "category":
            add_eq_text(col_by_lower["category"], v, "pattern_category_word")

    m = re.search(
        r"\b(?:покаж\w*|show|display)\s+категори\w+\s+([a-zA-Zа-яА-ЯёЁ0-9_.-]+)\b",
        q,
        re.IGNORECASE,
    )
    if m and "category" in col_by_lower:
        v = _norm_val(m.group(1))
        if v and v.lower() != "category":
            add_eq_text(col_by_lower["category"], v, "pattern_pokazhi_kategoriyu")

    m = re.search(r"\b(?:категор\w+)\s+([a-zA-Zа-яА-ЯёЁ0-9_.-]+)\b", q)
    if m and "category" in col_by_lower:
        v = _norm_val(m.group(1))
        if v and v.lower() != "category":
            add_eq_text(col_by_lower["category"], v, "pattern_kategoriya_word")

    # --- RU город / канал + значение (если колонка есть)
    if "city" in col_by_lower:
        m = re.search(r"\bгород\w*\s+([a-zA-Zа-яА-ЯёЁ0-9_.-]+)\b", q)
        if m:
            v = _norm_val(m.group(1))
            if v:
                add_eq_text(col_by_lower["city"], v, "pattern_gorod")
    if "channel" in col_by_lower:
        m = re.search(r"\bканал\w*\s+([a-zA-Zа-яА-ЯёЁ0-9_.-]+)\b", q)
        if m:
            v = _norm_val(m.group(1))
            if v:
                add_eq_text(col_by_lower["channel"], v, "pattern_kanal")

    # --- column value (имя колонки как в схеме + aliases with spaces)
    for name in sorted(col_names, key=len, reverse=True):
        ln = name.lower()
        if name in skip_columns:
            continue
        for alias in _col_aliases(name):
            esc = re.escape(alias)
            # equality patterns:
            #   school type public
            #   school type is public
            #   school type = public
            m = re.search(
                rf"\b{esc}\s*(?:is|equals|=|равно|это)?\s*([a-zA-Zа-яА-ЯёЁ0-9_.-]+)\b",
                q,
                re.IGNORECASE,
            )
            if m:
                v = _norm_val(m.group(1))
                if v and v.lower() != alias.replace(" ", ""):
                    if _is_numeric_sql_type(col_types.get(name, "")):
                        try:
                            add_cmp_numeric(name, "=", float(v.replace(",", ".")), "col_alias_value_numeric")
                        except ValueError:
                            pass
                    else:
                        add_eq_text(name, v, "col_alias_value")
                break

    _STATUS_VALUES = frozenset(
        {"completed", "returned", "refunded", "cancelled", "canceled", "pending"}
    )

    # --- «только по городу X» / «only by category X» (слово «по» иначе съедалось как значение)
    for pat, col_key, reason in (
        (
            r"\b(?:только|only|лишь)\s+по\s+город\w*\s+([a-zA-Zа-яА-ЯёЁ0-9_.-]+)\b",
            "city",
            "pattern_tolko_po_gorod",
        ),
        (
            r"\b(?:только|only|лишь)\s+по\s+категори\w*\s+([a-zA-Zа-яА-ЯёЁ0-9_.-]+)\b",
            "category",
            "pattern_tolko_po_category",
        ),
        (
            r"\b(?:только|only|лишь)\s+по\s+канал\w*\s+([a-zA-Zа-яА-ЯёЁ0-9_.-]+)\b",
            "channel",
            "pattern_tolko_po_channel",
        ),
    ):
        mm = re.search(pat, q, re.IGNORECASE)
        if mm and col_key in col_by_lower:
            v = _norm_val(mm.group(1))
            if v:
                add_eq_text(col_by_lower[col_key], v, reason)

    # --- «только X» — сначала статус (иначе «только completed» ошибочно шло в category)
    m = re.search(r"\b(?:только|only)\s+([a-zA-Zа-яА-ЯёЁ0-9_.-]+)\b", q)
    if m:
        v = _norm_val(m.group(1))
        if v:
            vlow = v.lower()
            if (
                "status" in col_by_lower
                and vlow in _STATUS_VALUES
                and col_by_lower["status"] not in used_cols
                and col_by_lower["status"] not in skip_columns
            ):
                add_eq_text(col_by_lower["status"], vlow, "pattern_tolko_status")
            else:
                for pref in ("category", "city", "channel", "manager", "product", "status"):
                    if pref in col_by_lower and col_by_lower[pref] not in used_cols:
                        if pref in skip_columns:
                            continue
                        add_eq_text(col_by_lower[pref], v, "pattern_tolko")
                        break

    # --- «покажи Electronics» / show Home — значение категории (или статус), не имя колонки
    mm = re.search(
        r"\b(?:покаж\w*|show|display)\s*,?\s*([A-Za-zА-Яа-яёЁ0-9_-]{2,})\b",
        q,
        re.IGNORECASE,
    )
    if mm:
        v = _norm_val(mm.group(1))
        if v and v.lower() not in col_by_lower and v.lower() != "category":
            vlow = v.lower()
            if (
                vlow in _STATUS_VALUES
                and "status" in col_by_lower
                and col_by_lower["status"] not in used_cols
                and col_by_lower["status"] not in skip_columns
            ):
                add_eq_text(col_by_lower["status"], vlow, "pattern_pokazhi_status")
            elif "category" in col_by_lower and col_by_lower["category"] not in used_cols:
                add_eq_text(col_by_lower["category"], v, "pattern_pokazhi_value")

    # --- Сравнения для числовых колонок (RU + EN)
    for name in col_names:
        if name in skip_columns or name in used_cols:
            continue
        if not _is_numeric_sql_type(col_types.get(name, "")):
            continue
        matched = False
        for alias in _col_aliases(name):
            esc = re.escape(alias)
            for rx, op in (
                (rf"\b{esc}\s*>\s*(\d+(?:\.\d+)?)\b", ">"),
                (rf"\b{esc}\s*<\s*(\d+(?:\.\d+)?)\b", "<"),
                (rf"\b{esc}\s*>=\s*(\d+(?:\.\d+)?)\b", ">="),
                (rf"\b{esc}\s*<=\s*(\d+(?:\.\d+)?)\b", "<="),
                (rf"\b{esc}\s*=\s*(\d+(?:\.\d+)?)\b", "="),
                (rf"\b{esc}\s+больше\s+(\d+(?:\.\d+)?)\b", ">"),
                (rf"\b{esc}\s+меньше\s+(\d+(?:\.\d+)?)\b", "<"),
                (rf"\b{esc}\s+не\s+меньше\s+(\d+(?:\.\d+)?)\b", ">="),
                (rf"\b{esc}\s+не\s+больше\s+(\d+(?:\.\d+)?)\b", "<="),
                (rf"\b{esc}\s+(?:more|over|above|greater(?:\s+than)?)\s+(\d+(?:\.\d+)?)\b", ">"),
                (rf"\b{esc}\s+(?:less|under|below|lower(?:\s+than)?)\s+(\d+(?:\.\d+)?)\b", "<"),
                (rf"\b{esc}\s+(?:at\s+least|not\s+less\s+than)\s+(\d+(?:\.\d+)?)\b", ">="),
                (rf"\b{esc}\s+(?:at\s+most|not\s+more\s+than)\s+(\d+(?:\.\d+)?)\b", "<="),
            ):
                mm = re.search(rx, q, re.IGNORECASE)
                if mm:
                    add_cmp_numeric(name, op, float(mm.group(1).replace(",", ".")), "numeric_cmp_multilang")
                    matched = True
                    break
            if matched:
                break

    # --- ORDER BY
    order_by = _pick_order_by(q, col_by_lower, col_types)

    return where_fragments, params, meta, order_by


def _pick_order_by(
    q: str,
    col_by_lower: dict[str, str],
    col_types: dict[str, str],
) -> Optional[tuple[str, str]]:
    desc = bool(
        re.search(
            r"\b(?:убыван|убывающ|desc|descending|от\s+больш|max\s+first|сначала\s+больш)\w*\b",
            q,
            re.IGNORECASE,
        )
    )
    asc = bool(
        re.search(
            r"\b(?:возрастан|возрастающ|asc|ascending|от\s+меньш|min\s+first|сначала\s+меньш)\w*\b",
            q,
            re.IGNORECASE,
        )
    )
    direction = "DESC" if desc and not asc else "ASC" if asc and not desc else "ASC"
    if re.search(r"\b(?:максимум|наибольш|самые\s+больш)\w*\b", q) and not asc:
        direction = "DESC"
    if re.search(r"\b(?:минимум|наименьш|самые\s+маленьк)\w*\b", q) and not desc:
        direction = "ASC"

    col_token: Optional[str] = None
    m = re.search(r"\b(?:sort(?:ed)?\s+by|order\s+by)\s+([a-zA-Zа-яА-ЯёЁ_][\w]*)\b", q)
    if m:
        col_token = m.group(1).lower()

    alias_to_col = {
        "дате": "date",
        "дата": "date",
        "датам": "date",
        "количеству": "quantity",
        "числу": "quantity",
        "цене": "unit_price",
        "ценам": "unit_price",
        "выручке": "revenue",
        "сумме": "amount",
        "менеджеру": "manager",
        "имени": "manager",
    }
    _po_skip = frozenset(
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
    resolved: Optional[tuple[str, str]] = None
    if col_token:
        if col_token in alias_to_col:
            cand = alias_to_col[col_token]
            if cand in col_by_lower:
                resolved = (col_by_lower[cand], direction)
        elif col_token in col_by_lower:
            resolved = (col_by_lower[col_token], direction)
    if resolved:
        return resolved
    for mm in re.finditer(r"\bпо\s+([a-zA-Zа-яА-ЯёЁ_]\w*)\b", q):
        tok = mm.group(1).lower()
        if tok in _po_skip:
            continue
        if tok in alias_to_col:
            cand = alias_to_col[tok]
            if cand in col_by_lower:
                return (col_by_lower[cand], direction)
        if tok in col_by_lower:
            return (col_by_lower[tok], direction)
    return None
