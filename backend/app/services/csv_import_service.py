from __future__ import annotations

import csv
import io
import re
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

    columns_payload = [{"name": n, "type": t} for n, t in zip(header, col_types)]
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
