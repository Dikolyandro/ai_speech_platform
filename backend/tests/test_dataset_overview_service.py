"""Tests for deterministic dataset overview and columns_json wrapping."""

from __future__ import annotations

from app.services.dataset_overview_service import (
    build_dataset_overview,
    get_dataset_summary_from_meta_json,
    unwrap_dataset_columns_json,
    wrap_columns_with_summary,
)


def _col(name: str, typ: str = "TEXT") -> dict:
    return {"name": name, "type": typ}


def test_unwrap_legacy_list():
    raw = [_col("a"), _col("b")]
    assert unwrap_dataset_columns_json(raw) == raw


def test_unwrap_wrapped_dict():
    cols = [_col("title"), _col("listed_in")]
    summary = {"domain_guess": "x"}
    wrapped = wrap_columns_with_summary(cols, summary)
    assert unwrap_dataset_columns_json(wrapped) == cols
    assert get_dataset_summary_from_meta_json(wrapped) == summary


def test_build_overview_netflix_like():
    cols = [
        _col("show_id"),
        _col("type"),
        _col("title"),
        _col("director"),
        _col("cast"),
        _col("country"),
        _col("date_added"),
        _col("release_year", "INT"),
        _col("rating"),
        _col("duration"),
        _col("listed_in"),
        _col("description"),
    ]
    o = build_dataset_overview(cols, row_count=8807)
    assert o["domain_guess"] == "entertainment/media"
    assert o["row_count"] == 8807
    assert o["column_count"] == 12
    assert "titles" in o["detected_entities"]
    assert "genres" in o["detected_entities"]
    assert "top genres on netflix" in o["suggested_questions"]


def test_build_overview_student_performance_like():
    cols = [
        _col("student_id"),
        _col("hours_studied", "INT"),
        _col("attendance", "INT"),
        _col("exam_score", "INT"),
    ]
    o = build_dataset_overview(cols, row_count=100)
    assert o["domain_guess"] == "education/performance"
    assert "average exam score by attendance" in o["suggested_questions"]


def test_build_overview_general():
    cols = [_col("sku"), _col("qty", "INT"), _col("region")]
    o = build_dataset_overview(cols, row_count=None)
    assert o["domain_guess"] == "general_tabular_data"
    assert "row_count" not in o
    assert "show top categories" in o["suggested_questions"]
