from __future__ import annotations

import csv
import io
import re
from collections import Counter
from typing import Any, Optional

from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Dataset, DatasetTableMeta


def _sanitize_col(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^0-9a-zA-Z_а-яА-ЯёЁ]", "", name)
    if not name:
        name = "col"
    name = name.replace("ё", "e").replace("Ё", "E")
    if re.match(r"^\d", name):
        name = f"c_{name}"
    return name[:64]


def _normalize_col_name_for_search(name: str) -> str:
    """
    Normalizes arbitrary column names into a human-like phrase for matching:
      - Family_Income -> family income
      - player_name -> player name
      - customerCity -> customer city
    """
    s = (name or "").strip()
    s = re.sub(r"[_\-]+", " ", s)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _normalize_value_for_search(value: str) -> str:
    s = (value or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    # drop punctuation but keep letters/digits/spaces
    s = re.sub(r"[^0-9a-zA-Zа-яА-ЯёЁ ]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _tokenize_simple(text: str) -> list[str]:
    t = _normalize_value_for_search(text)
    toks = [w for w in t.split(" ") if len(w) >= 2]
    return toks[:32]


def _localize_token(token: str, lang: str) -> str:
    # lightweight dictionary; fallback is the source token.
    ru = {
        "income": "доход",
        "family": "семейный",
        "gender": "пол",
        "exam": "экзамен",
        "score": "балл",
        "player": "игрок",
        "club": "клуб",
        "market": "рынок",
        "value": "стоимость",
        "customer": "клиент",
        "city": "город",
        "total": "итог",
        "amount": "сумма",
        "order": "заказ",
        "date": "дата",
        "product": "товар",
        "title": "название",
        "category": "категория",
        "rating": "рейтинг",
        "name": "имя",
    }
    kk = {
        "income": "табыс",
        "family": "отбасы",
        "gender": "жыныс",
        "exam": "емтихан",
        "score": "балл",
        "player": "ойыншы",
        "club": "клуб",
        "market": "нарық",
        "value": "құн",
        "customer": "клиент",
        "city": "қала",
        "total": "жалпы",
        "amount": "сома",
        "order": "тапсырыс",
        "date": "күн",
        "product": "өнім",
        "title": "атау",
        "category": "санат",
        "rating": "рейтинг",
        "name": "аты",
    }
    if lang == "ru":
        return ru.get(token, token)
    if lang == "kk":
        return kk.get(token, token)
    return token


def _localized_label_and_description(raw_name: str, normalized_name: str) -> dict[str, dict[str, str]]:
    toks = _tokenize_simple(normalized_name)
    if not toks:
        toks = _tokenize_simple(raw_name)
    en_label = " ".join(toks).strip() or normalized_name or raw_name
    ru_label = " ".join(_localize_token(t, "ru") for t in toks).strip() or raw_name
    kk_label = " ".join(_localize_token(t, "kk") for t in toks).strip() or raw_name
    return {
        "labels": {
            "en": en_label.title(),
            "ru": ru_label[:1].upper() + ru_label[1:] if ru_label else raw_name,
            "kk": kk_label[:1].upper() + kk_label[1:] if kk_label else raw_name,
        },
        "descriptions": {
            "en": f"Column '{raw_name}' from uploaded dataset.",
            "ru": f"Колонка '{raw_name}' из загруженного датасета.",
            "kk": f"Жүктелген датасеттегі '{raw_name}' бағаны.",
        },
    }


def _infer_type(values: list[str]) -> str:
    cleaned = [v.strip() for v in values if v is not None and str(v).strip() != ""]
    if not cleaned:
        return "TEXT"
    is_int = True
    is_float = True
    for v in cleaned[:200]:
        if not re.fullmatch(r"-?\d+", v):
            is_int = False
        if not re.fullmatch(r"-?\d+(\.\d+)?", v):
            is_float = False
    if is_int:
        return "BIGINT"
    if is_float:
        return "DOUBLE"
    return "TEXT"


def _as_number(v: Optional[str]) -> Optional[float]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    # tolerate comma decimal separator
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _profile_columns(
    header: list[str],
    col_types: list[str],
    rows: list[list[Optional[str]]],
    *,
    max_distinct: int = 5000,
    top_k: int = 7,
    categorical_distinct_cap: int = 80,
    categorical_store_cap: int = 200,
) -> list[dict[str, Any]]:
    n_rows = len(rows)
    profiles: list[dict[str, Any]] = []

    for j, (name, typ) in enumerate(zip(header, col_types)):
        nulls = 0
        empties = 0
        non_null = 0
        distinct: set[str] = set()
        too_many_distinct = False

        # numeric stats
        n_count = 0
        n_sum = 0.0
        n_min: Optional[float] = None
        n_max: Optional[float] = None

        # text stats
        lens_sum = 0
        lens_count = 0
        top_counter: Counter[str] = Counter()
        categorical_values_norm: set[str] = set()
        categorical_values_raw: set[str] = set()

        for r in rows:
            v = r[j] if j < len(r) else None
            if v is None:
                nulls += 1
                continue
            s = str(v)
            if s.strip() == "":
                empties += 1
                continue

            non_null += 1
            if not too_many_distinct:
                if len(distinct) < max_distinct:
                    distinct.add(s)
                else:
                    too_many_distinct = True

            # numeric profile
            if typ in ("BIGINT", "DOUBLE"):
                num = _as_number(s)
                if num is not None:
                    n_count += 1
                    n_sum += num
                    n_min = num if n_min is None else min(n_min, num)
                    n_max = num if n_max is None else max(n_max, num)
                continue

            # text profile
            ss = s.strip()
            if ss:
                top_counter[ss] += 1
                lens_sum += len(ss)
                lens_count += 1
                # collect categorical values (dataset-agnostic value resolution)
                if not too_many_distinct and len(distinct) <= categorical_distinct_cap:
                    vn = _normalize_value_for_search(ss)
                    if vn:
                        if len(categorical_values_norm) < categorical_store_cap:
                            categorical_values_norm.add(vn)
                        if len(categorical_values_raw) < categorical_store_cap:
                            categorical_values_raw.add(ss)

        prof: dict[str, Any] = {
            "rows": n_rows,
            "non_null": non_null,
            "nulls": nulls,
            "empties": empties,
            "distinct": (None if too_many_distinct else len(distinct)),
            "distinct_truncated": bool(too_many_distinct),
        }

        if typ in ("BIGINT", "DOUBLE"):
            prof["numeric"] = {
                "count": n_count,
                "min": n_min,
                "max": n_max,
                "mean": (None if n_count == 0 else (n_sum / n_count)),
            }
        else:
            prof["text"] = {
                "avg_len": (None if lens_count == 0 else (lens_sum / lens_count)),
                "top_values": [{"value": v, "count": int(c)} for v, c in top_counter.most_common(top_k)],
            }

        normalized_name = _normalize_col_name_for_search(name)
        sem: dict[str, Any] = {
            "normalized": normalized_name,
            "tokens": _tokenize_simple(normalized_name),
        }
        sem.update(_localized_label_and_description(name, normalized_name))
        # Only store categorical values for low-cardinality TEXT columns.
        if typ == "TEXT" and (not too_many_distinct) and (prof.get("distinct") is not None) and int(prof["distinct"]) <= categorical_distinct_cap:
            sem["categorical_values_norm"] = sorted(categorical_values_norm)[:categorical_store_cap]
            # raw samples (helps debugging / UI)
            sem["categorical_values_raw"] = sorted(categorical_values_raw)[:categorical_store_cap]

        # Candidate roles: useful hints, not hardcoded to any dataset.
        sem["roles"] = {
            "is_numeric": bool(typ in ("BIGINT", "DOUBLE")),
            "is_categorical": bool(
                typ == "TEXT"
                and (not too_many_distinct)
                and (prof.get("distinct") is not None)
                and int(prof["distinct"]) <= categorical_distinct_cap
            ),
        }

        profiles.append({"name": name, "type": typ, "profile": prof, "semantic": sem})

    return profiles


async def import_csv_into_dataset(
    db: AsyncSession,
    dataset_id: int,
    csv_text: str,
    *,
    delimiter: str = ",",
    has_header: bool = True,
    max_rows: int = 5000,
    drop_existing: bool = True,
) -> dict[str, Any]:
    ds = (await db.execute(select(Dataset).where(Dataset.id == dataset_id))).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")

    table_name = f"ds_{dataset_id}_data"
    if not csv_text or not csv_text.strip():
        raise HTTPException(status_code=400, detail="csv_text is empty")
    if len(delimiter) != 1:
        raise HTTPException(status_code=400, detail="delimiter must be a single character")

    f = io.StringIO(csv_text.strip())
    reader = csv.reader(f, delimiter=delimiter)
    rows: list[list[str]] = []
    header: Optional[list[str]] = None

    for i, row in enumerate(reader):
        if i == 0 and has_header:
            header = [_sanitize_col(x) for x in row]
            continue
        if row and any(cell.strip() for cell in row):
            rows.append(row)
        if len(rows) >= max_rows:
            break

    if not rows:
        raise HTTPException(status_code=400, detail="no data rows found in CSV")

    if header is None:
        max_c = max(len(r) for r in rows)
        header = [f"col_{i+1}" for i in range(max_c)]

    max_cols = len(header)
    norm_rows = []
    for r in rows:
        r2 = (r + [""] * max_cols)[:max_cols]
        norm_rows.append(r2)

    col_samples: list[list[str]] = [[] for _ in range(max_cols)]
    for r in norm_rows[:500]:
        for j in range(max_cols):
            col_samples[j].append(r[j])

    col_types = [_infer_type(col_samples[j]) for j in range(max_cols)]

    seen = set()
    fixed_header = []
    for name in header:
        n = name
        k = 1
        while n in seen:
            k += 1
            n = f"{name}_{k}"
        seen.add(n)
        fixed_header.append(n)
    header = fixed_header

    if drop_existing:
        await db.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))

    cols_ddl = []
    for name, typ in zip(header, col_types):
        cols_ddl.append(f"`{name}` {typ} NULL")
    ddl = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(cols_ddl)})"
    await db.execute(text(ddl))

    col_list = ", ".join(f"`{c}`" for c in header)
    placeholders = ", ".join([f":v{i}" for i in range(max_cols)])
    ins_sql = text(f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders})")

    BATCH = 500
    inserted = 0
    for start in range(0, len(norm_rows), BATCH):
        batch = norm_rows[start : start + BATCH]
        params_list = []
        for r in batch:
            params_list.append({f"v{i}": (r[i] if r[i] != "" else None) for i in range(max_cols)})
        await db.execute(ins_sql, params_list)
        inserted += len(batch)

    # Store a lightweight schema profile into DatasetTableMeta.columns_json.
    # Kept as a list for backward compatibility with existing API consumers.
    prof_rows: list[list[Optional[str]]] = [
        [(c if c != "" else None) for c in r] for r in norm_rows
    ]
    columns_payload = _profile_columns(header, col_types, prof_rows)
    meta_row = (
        await db.execute(select(DatasetTableMeta).where(DatasetTableMeta.dataset_id == dataset_id))
    ).scalar_one_or_none()
    if meta_row:
        meta_row.table_name = table_name
        meta_row.columns_json = columns_payload
    else:
        db.add(
            DatasetTableMeta(
                dataset_id=dataset_id,
                table_name=table_name,
                columns_json=columns_payload,
            )
        )

    await db.commit()

    return {
        "dataset_id": dataset_id,
        "table_name": table_name,
        "columns": columns_payload,
        "rows_inserted": inserted,
        "note": "Now use POST /api/v1/query/answer to get results from this table.",
    }
