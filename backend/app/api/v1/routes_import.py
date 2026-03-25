from __future__ import annotations

import csv
import io
import re
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db

router = APIRouter(prefix="/datasets", tags=["Dataset Import"])


class ImportCSVRequest(BaseModel):
    # CSV content as string (paste into Swagger)
    csv_text: str

    # options
    delimiter: str = ","
    has_header: bool = True
    max_rows: int = 5000  # safety
    drop_existing: bool = True  # recreate table


def _sanitize_col(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^0-9a-zA-Z_а-яА-ЯёЁ]", "", name)
    if not name:
        name = "col"
    # mysql identifiers: keep ascii-ish
    name = name.replace("ё", "e").replace("Ё", "E")
    # if starts with digit
    if re.match(r"^\d", name):
        name = f"c_{name}"
    return name[:64]


def _infer_type(values: list[str]) -> str:
    # MySQL basic types for MVP
    # try int -> float -> text
    cleaned = [v.strip() for v in values if v is not None and str(v).strip() != ""]
    if not cleaned:
        return "TEXT"

    is_int = True
    is_float = True

    for v in cleaned[:200]:
        # int?
        if not re.fullmatch(r"-?\d+", v):
            is_int = False
        # float?
        if not re.fullmatch(r"-?\d+(\.\d+)?", v):
            is_float = False

    if is_int:
        return "BIGINT"
    if is_float:
        return "DOUBLE"
    return "TEXT"


@router.post("/{dataset_id}/import_csv")
async def import_csv(dataset_id: int, req: ImportCSVRequest, db: AsyncSession = Depends(get_db)) -> dict[str, Any]:
    table_name = f"ds_{dataset_id}_data"

    if not req.csv_text or not req.csv_text.strip():
        raise HTTPException(status_code=400, detail="csv_text is empty")

    if len(req.delimiter) != 1:
        raise HTTPException(status_code=400, detail="delimiter must be a single character")

    # parse CSV
    f = io.StringIO(req.csv_text.strip())
    reader = csv.reader(f, delimiter=req.delimiter)

    rows: list[list[str]] = []
    header: Optional[list[str]] = None

    for i, row in enumerate(reader):
        if i == 0 and req.has_header:
            header = [_sanitize_col(x) for x in row]
            continue
        if row and any(cell.strip() for cell in row):
            rows.append(row)

        if len(rows) >= req.max_rows:
            break

    if not rows:
        raise HTTPException(status_code=400, detail="no data rows found in CSV")

    # if no header -> create generic
    if header is None:
        max_cols = max(len(r) for r in rows)
        header = [f"col_{i+1}" for i in range(max_cols)]

    # normalize row lengths
    max_cols = len(header)
    norm_rows = []
    for r in rows:
        r2 = (r + [""] * max_cols)[:max_cols]
        norm_rows.append(r2)

    # infer types per column using sample
    col_samples: list[list[str]] = [[] for _ in range(max_cols)]
    for r in norm_rows[:500]:
        for j in range(max_cols):
            col_samples[j].append(r[j])

    col_types = [_infer_type(col_samples[j]) for j in range(max_cols)]

    # (optional) prevent empty/duplicate column names
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

    # drop / create table
    if req.drop_existing:
        await db.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))

    # create DDL
    cols_ddl = []
    for name, typ in zip(header, col_types):
        # store everything nullable for MVP
        cols_ddl.append(f"`{name}` {typ} NULL")
    ddl = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(cols_ddl)})"
    await db.execute(text(ddl))

    # insert rows (batch)
    col_list = ", ".join(f"`{c}`" for c in header)
    placeholders = ", ".join([f":v{i}" for i in range(max_cols)])
    ins_sql = text(f"INSERT INTO `{table_name}` ({col_list}) VALUES ({placeholders})")

    BATCH = 500
    inserted = 0
    for start in range(0, len(norm_rows), BATCH):
        batch = norm_rows[start:start + BATCH]
        params_list = []
        for r in batch:
            params_list.append({f"v{i}": (r[i] if r[i] != "" else None) for i in range(max_cols)})
        await db.execute(ins_sql, params_list)
        inserted += len(batch)

    await db.commit()

    return {
        "dataset_id": dataset_id,
        "table_name": table_name,
        "columns": [{"name": n, "type": t} for n, t in zip(header, col_types)],
        "rows_inserted": inserted,
        "note": "Now use POST /api/v1/query/answer to get results from this table.",
    }
