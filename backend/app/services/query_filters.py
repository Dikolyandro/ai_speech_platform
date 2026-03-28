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


def _cmp_sql_left_and_op(col: str, sql_type: str, op: str) -> Optional[str]:
    """
    Левая часть числового сравнения + оператор (без плейсхолдера значения).
    Для TEXT-колонок вроде Exam_Score, ошибочно помеченных при импорте, — CAST AS REAL.
    """
    if _is_numeric_sql_type(sql_type or ""):
        return f"{_quote_col(col)} {op}"
    t = (sql_type or "").upper()
    if t != "TEXT" and t != "VARCHAR" and "CHAR" not in t:
        return None
    if not re.search(
        r"(^|_)(score|rating|grade|points|percent|pct|amount|price|qty|quantity|hours|studied|attendance|age|count|income|revenue|total|balance)(_|$)",
        col,
        re.I,
    ):
        return None
    return f"CAST({_quote_col(col)} AS REAL) {op}"


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


def _col_aliases_with_metric_synonyms(name: str) -> list[str]:
    """«exam score» ↔ «exam grade» и т.п., чтобы NL вроде *exam grade more than 70* цеплялся к Exam_Score."""
    base = _col_aliases(name)
    ln = name.lower()
    if "score" not in ln and "grade" not in ln:
        return base
    out: set[str] = set(base)
    for a in base:
        if "score" in a:
            out.add(a.replace("score", "grade"))
        if "grade" in a:
            out.add(a.replace("grade", "score"))
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

    cmp_signatures: set[tuple[str, str, str]] = set()

    def add_cmp_numeric(col: str, op: str, val: float, reason: str) -> None:
        if col in skip_columns:
            return
        # Несколько сравнений по одной колонке (score > 75 и score < 77); «=» — одно на колонку.
        if op == "=" and col in used_cols:
            return
        sig = (col, op, f"{val}")
        if sig in cmp_signatures:
            return
        left_op = _cmp_sql_left_and_op(col, col_types.get(col, ""), op)
        if not left_op:
            return
        key = next_key()
        params[key] = val
        where_fragments.append(f"{left_op} :{key}")
        meta.append({"column": col, "op": op, "value": val, "reason": reason})
        cmp_signatures.add(sig)
        if op == "=":
            used_cols.add(col)

    def add_or_eq_text_same_col(col: str, vals: list[str], reason: str) -> None:
        if col in skip_columns or col in used_cols:
            return
        vals = [v for v in vals if v]
        if not vals:
            return
        if len(vals) == 1:
            add_eq_text(col, vals[0], reason)
            return
        parts: list[str] = []
        for v in vals:
            key = next_key()
            params[key] = v
            parts.append(f"LOWER({_quote_col(col)}) = LOWER(:{key})")
        where_fragments.append("(" + " OR ".join(parts) + ")")
        meta.append({"column": col, "op": "OR_EQ", "value": vals, "reason": reason})
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

    # --- образование родителей: старшая школа / колледж / послевузовское (как в типичном CSV)
    if "parental_education_level" in col_by_lower:
        pec = col_by_lower["parental_education_level"]
        if pec not in skip_columns:
            if re.search(
                r"\b(?:магистратур\w*|аспирантур\w*|послевузовск\w*|postgraduate)\b",
                q,
                re.IGNORECASE,
            ):
                add_eq_text(pec, "Postgraduate", "pattern_ru_parent_edu")
            elif re.search(r"\bколледж\w*\b", q, re.IGNORECASE):
                add_eq_text(pec, "College", "pattern_ru_parent_edu")
            elif re.search(
                r"\b(?:старш(ая|ей|ие|их)?\s+школ\w*|high\s+school)\b",
                q,
                re.IGNORECASE,
            ):
                add_eq_text(pec, "High School", "pattern_ru_parent_edu")

    # --- пол (RU + EN, без явного «gender = …» в запросе)
    if "gender" in col_by_lower:
        gcol = col_by_lower["gender"]
        if gcol not in skip_columns:
            has_f = bool(
                re.search(
                    r"\b(female|женщин\w*|девушк\w*|женск\w*|девочк\w*)\b",
                    q,
                    re.IGNORECASE,
                )
            )
            has_m = bool(
                re.search(
                    r"\b(male|мужчин\w*|парн\w*|мужск\w*|юнош\w*|мальчик\w*)\b",
                    q,
                    re.IGNORECASE,
                )
            )
            if has_f and not has_m:
                add_eq_text(gcol, "female", "pattern_gender_word")
            elif has_m and not has_f:
                add_eq_text(gcol, "male", "pattern_gender_word")

    # --- Distance_from_Home: Near / Moderate / Far (только при контексте «расстояние / до дома»)
    if "distance_from_home" in col_by_lower:
        dcol = col_by_lower["distance_from_home"]
        if dcol not in skip_columns:
            dist_ctx = bool(
                re.search(
                    r"\b(?:расстоян\w*|distance\s+from\s+home|от\s+дома|до\s+дома|из\s+дома|"
                    r"близко\s+к\s+дому|удалённост\w*|удаленност\w*)\b",
                    q,
                    re.IGNORECASE,
                )
            )
            if dist_ctx:
                near = bool(re.search(r"\b(близк\w*|недалеко|рядом)\b", q, re.IGNORECASE))
                mod = bool(re.search(r"\b(средн\w*|умеренн\w*|moderate)\b", q, re.IGNORECASE))
                far = bool(
                    re.search(r"\b(далёк\w*|далек\w*|удалённ\w*|удаленн\w*|far)\b", q, re.IGNORECASE)
                )
                vals: list[str] = []
                if near:
                    vals.append("Near")
                if mod:
                    vals.append("Moderate")
                if far:
                    vals.append("Far")
                if len(vals) == 1:
                    add_eq_text(dcol, vals[0], "pattern_ru_distance")
                elif len(vals) > 1:
                    add_or_eq_text_same_col(dcol, vals, "pattern_ru_distance")

    # --- RU: о́ценка / балл / результат против числа (колонки *score*, без повтора имени колонки)
    # Между «оценка» и «больше» могут быть уточнения: «оценка экзамена больше 70».
    _ru_score_head = r"(?:оценк\w*|балл\w*|результат\w*)"
    _ru_score_gap = r"(?:\s+[a-zA-Zа-яА-ЯёЁ0-9_-]+){0,6}"
    score_cols = [n for n in col_names if "score" in n.lower() and n not in skip_columns]
    if score_cols:
        primary_sc = next(
            (n for n in score_cols if "exam" in n.lower()),
            score_cols[0],
        )
        for rx, op in (
            (rf"\b{_ru_score_head}{_ru_score_gap}\s+больше\s+(\d+(?:\.\d+)?)\b", ">"),
            (rf"\b{_ru_score_head}{_ru_score_gap}\s+меньше\s+(\d+(?:\.\d+)?)\b", "<"),
            (rf"\b{_ru_score_head}{_ru_score_gap}\s+(?:выше|сверх)\s+(\d+(?:\.\d+)?)\b", ">"),
            (rf"\b{_ru_score_head}{_ru_score_gap}\s+ниже\s+(\d+(?:\.\d+)?)\b", "<"),
            (rf"\b{_ru_score_head}{_ru_score_gap}\s+не\s+меньше\s+(\d+(?:\.\d+)?)\b", ">="),
            (rf"\b{_ru_score_head}{_ru_score_gap}\s+не\s+больше\s+(\d+(?:\.\d+)?)\b", "<="),
            (r"\bбольше\s+(\d+(?:\.\d+)?)\s+(?:баллов|балла|балл|очков)\b", ">"),
            (r"\bменьше\s+(\d+(?:\.\d+)?)\s+(?:баллов|балла|балл|очков)\b", "<"),
        ):
            mm = re.search(rx, q, re.IGNORECASE)
            if mm:
                add_cmp_numeric(
                    primary_sc,
                    op,
                    float(mm.group(1).replace(",", ".")),
                    "ru_score_phrase",
                )
        # Диапазон без имени колонки: «оценка больше 70 но меньше 75»
        _ru_join = r"(?:и|но|а)"
        mm_rr = re.search(
            rf"\b{_ru_score_head}{_ru_score_gap}\s+больше\s+(\d+(?:\.\d+)?)\s+{_ru_join}\s+меньше\s+(\d+(?:\.\d+)?)\b",
            q,
            re.IGNORECASE,
        )
        if mm_rr:
            add_cmp_numeric(
                primary_sc,
                ">",
                float(mm_rr.group(1).replace(",", ".")),
                "ru_score_range",
            )
            add_cmp_numeric(
                primary_sc,
                "<",
                float(mm_rr.group(2).replace(",", ".")),
                "ru_score_range",
            )
        mm_rr_rev = re.search(
            rf"\b{_ru_score_head}{_ru_score_gap}\s+меньше\s+(\d+(?:\.\d+)?)\s+{_ru_join}\s+больше\s+(\d+(?:\.\d+)?)\b",
            q,
            re.IGNORECASE,
        )
        if mm_rr_rev:
            add_cmp_numeric(
                primary_sc,
                "<",
                float(mm_rr_rev.group(1).replace(",", ".")),
                "ru_score_range_rev",
            )
            add_cmp_numeric(
                primary_sc,
                ">",
                float(mm_rr_rev.group(2).replace(",", ".")),
                "ru_score_range_rev",
            )

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

    # --- Сравнения для числовых колонок (RU + EN); несколько предикатов на одну колонку (диапазоны)
    for name in col_names:
        if name in skip_columns:
            continue
        typ = col_types.get(name, "")
        if not _is_numeric_sql_type(typ) and _cmp_sql_left_and_op(name, typ, ">") is None:
            continue
        _en_range_join = r"(?:and|but)"
        for alias in _col_aliases_with_metric_synonyms(name):
            esc = re.escape(alias)
            # «exam score more than 75 and less than 77» — второе ограничение без повтора имени колонки
            mm_range_hi_lo = re.search(
                rf"\b{esc}\s+(?:more(?:\s+than)?|over|above|greater(?:\s+than)?|higher(?:\s+than)?)\s+(\d+(?:\.\d+)?)"
                rf"\s+{_en_range_join}\s+(?:less(?:\s+than)?|fewer(?:\s+than)?|under|below|lower(?:\s+than)?)\s+(\d+(?:\.\d+)?)\b",
                q,
                re.IGNORECASE,
            )
            if mm_range_hi_lo:
                add_cmp_numeric(
                    name,
                    ">",
                    float(mm_range_hi_lo.group(1).replace(",", ".")),
                    "numeric_range_en_and",
                )
                add_cmp_numeric(
                    name,
                    "<",
                    float(mm_range_hi_lo.group(2).replace(",", ".")),
                    "numeric_range_en_and",
                )
                continue
            mm_range_lo_hi = re.search(
                rf"\b{esc}\s+(?:less(?:\s+than)?|fewer(?:\s+than)?|under|below|lower(?:\s+than)?)\s+(\d+(?:\.\d+)?)"
                rf"\s+{_en_range_join}\s+(?:more(?:\s+than)?|over|above|greater(?:\s+than)?|higher(?:\s+than)?)\s+(\d+(?:\.\d+)?)\b",
                q,
                re.IGNORECASE,
            )
            if mm_range_lo_hi:
                add_cmp_numeric(
                    name,
                    "<",
                    float(mm_range_lo_hi.group(1).replace(",", ".")),
                    "numeric_range_en_and_rev",
                )
                add_cmp_numeric(
                    name,
                    ">",
                    float(mm_range_lo_hi.group(2).replace(",", ".")),
                    "numeric_range_en_and_rev",
                )
                continue
            mm_range_ru = re.search(
                rf"\b{esc}\s+больше\s+(\d+(?:\.\d+)?)\s+(?:и|но|а)\s+меньше\s+(\d+(?:\.\d+)?)\b",
                q,
                re.IGNORECASE,
            )
            if mm_range_ru:
                add_cmp_numeric(
                    name,
                    ">",
                    float(mm_range_ru.group(1).replace(",", ".")),
                    "numeric_range_ru_i",
                )
                add_cmp_numeric(
                    name,
                    "<",
                    float(mm_range_ru.group(2).replace(",", ".")),
                    "numeric_range_ru_i",
                )
                continue
            mm_range_ru_lo_hi = re.search(
                rf"\b{esc}\s+меньше\s+(\d+(?:\.\d+)?)\s+(?:и|но|а)\s+больше\s+(\d+(?:\.\d+)?)\b",
                q,
                re.IGNORECASE,
            )
            if mm_range_ru_lo_hi:
                add_cmp_numeric(
                    name,
                    "<",
                    float(mm_range_ru_lo_hi.group(1).replace(",", ".")),
                    "numeric_range_ru_i_rev",
                )
                add_cmp_numeric(
                    name,
                    ">",
                    float(mm_range_ru_lo_hi.group(2).replace(",", ".")),
                    "numeric_range_ru_i_rev",
                )
                continue
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
                (rf"\b{esc}\s+(?:выше|сверх)\s+(\d+(?:\.\d+)?)\b", ">"),
                (rf"\b{esc}\s+ниже\s+(\d+(?:\.\d+)?)\b", "<"),
                (
                    rf"\b{esc}\s+(?:more(?:\s+than)?|over|above|greater(?:\s+than)?|higher(?:\s+than)?)\s+(\d+(?:\.\d+)?)\b",
                    ">",
                ),
                (
                    rf"\b{esc}\s+(?:less(?:\s+than)?|fewer(?:\s+than)?|under|below|lower(?:\s+than)?)\s+(\d+(?:\.\d+)?)\b",
                    "<",
                ),
                (rf"\b{esc}\s+(?:at\s+least|not\s+less\s+than)\s+(\d+(?:\.\d+)?)\b", ">="),
                (rf"\b{esc}\s+(?:at\s+most|not\s+more\s+than)\s+(\d+(?:\.\d+)?)\b", "<="),
            ):
                mm = re.search(rx, q, re.IGNORECASE)
                if mm:
                    add_cmp_numeric(
                        name, op, float(mm.group(1).replace(",", ".")), "numeric_cmp_multilang"
                    )

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
