"""
Deterministic dataset overview from column metadata (no LLM, no external APIs).

``DatasetTableMeta.columns_json`` may be either:

- legacy: ``[ { "name", "type", "profile", ... }, ... ]``
- wrapped: ``{ "columns": [ ... ], "dataset_summary": { ... } }``
"""

from __future__ import annotations

from typing import Any, Optional

# ----- unwrap helpers (single source of truth) --------------------------------


def unwrap_dataset_columns_json(raw: Any) -> list[dict[str, Any]]:
    """Return the column profile list from ``columns_json`` regardless of storage shape."""
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, dict):
        inner = raw.get("columns")
        if isinstance(inner, list):
            return [x for x in inner if isinstance(x, dict)]
    return []


def get_dataset_summary_from_meta_json(raw: Any) -> Optional[dict[str, Any]]:
    """Extract embedded ``dataset_summary`` if present."""
    if isinstance(raw, dict):
        ds = raw.get("dataset_summary")
        if isinstance(ds, dict):
            return ds
    return None


def wrap_columns_with_summary(
    columns: list[dict[str, Any]],
    summary: dict[str, Any],
) -> dict[str, Any]:
    """Persistable ``columns_json`` object with column profiles + overview."""
    return {"columns": list(columns), "dataset_summary": dict(summary)}


# ----- overview generation ----------------------------------------------------


_ENTERTAINMENT_MARKERS: frozenset[str] = frozenset(
    {
        "show_id",
        "title",
        "type",
        "director",
        "cast",
        "country",
        "date_added",
        "release_year",
        "rating",
        "duration",
        "listed_in",
        "description",
    }
)

_EDUCATION_MARKERS: frozenset[str] = frozenset(
    {
        "exam_score",
        "exam_scores",
        "hours_studied",
        "hoursstudied",
        "attendance",
        "previous_scores",
        "previous_score",
        "student",
        "student_id",
        "study_hours",
    }
)


def _norm_col_names(columns_json: list[dict[str, Any]]) -> set[str]:
    out: set[str] = set()
    for c in columns_json:
        if not isinstance(c, dict):
            continue
        n = c.get("name")
        if isinstance(n, str) and n.strip():
            out.add(n.strip().lower())
    return out


def _guess_domain(names: set[str]) -> str:
    ent = sum(1 for m in _ENTERTAINMENT_MARKERS if m in names)
    edu = sum(1 for m in _EDUCATION_MARKERS if m in names)
    # Strong Netflix-style signal
    if ("listed_in" in names and "title" in names) or ent >= 4:
        return "entertainment/media"
    if edu >= 2 or ("exam_score" in names and "hours_studied" in names):
        return "education/performance"
    return "general_tabular_data"


def _classify_columns(names: set[str]) -> tuple[list[str], list[str], list[str]]:
    """Loose entity / metric / dimension buckets for display (deterministic)."""
    entities: list[str] = []
    metrics: list[str] = []
    dims: list[str] = []

    def add_unique(bucket: list[str], label: str, cond: bool) -> None:
        if cond and label not in bucket:
            bucket.append(label)

    add_unique(entities, "titles", any(x in names for x in ("title", "show_title", "movie_title", "name")))
    add_unique(entities, "genres", any(x in names for x in ("listed_in", "genre", "genres")))
    add_unique(entities, "countries", "country" in names or "nation" in names)
    add_unique(entities, "directors", "director" in names)
    add_unique(entities, "cast", "cast" in names or "actors" in names)
    add_unique(entities, "release years", "release_year" in names or "year" in names)

    add_unique(metrics, "duration", "duration" in names or "runtime" in names)
    add_unique(metrics, "release_year", "release_year" in names)
    add_unique(metrics, "exam_score", "exam_score" in names or "score" in names)
    add_unique(metrics, "hours_studied", "hours_studied" in names or "hours" in names)
    add_unique(metrics, "attendance", "attendance" in names)

    add_unique(dims, "type", "type" in names or "movie_type" in names)
    add_unique(dims, "country", "country" in names)
    add_unique(dims, "listed_in", "listed_in" in names)
    add_unique(dims, "rating", "rating" in names)
    add_unique(dims, "status", "status" in names)
    add_unique(dims, "category", "category" in names)

    return entities, metrics, dims


def build_dataset_overview(
    columns_json: list[dict[str, Any]],
    row_count: int | None = None,
) -> dict[str, Any]:
    """
    Build a small JSON-serializable overview for UI / onboarding.

    ``columns_json`` must be the list of column profile dicts (``name``, ``type``, …).
    """
    names = _norm_col_names(columns_json)
    domain = _guess_domain(names)
    entities, metrics, dims = _classify_columns(names)
    col_list = [x for x in columns_json if isinstance(x, dict)]
    col_count = len(col_list)

    suggested: list[str] = []
    if domain == "entertainment/media":
        suggested = [
            "top genres on netflix",
            "compare movies and tv shows",
            "trend of releases over time",
            "which countries produce most content",
            "longest movies on netflix",
        ]
    elif domain == "education/performance":
        suggested = [
            "average exam score by attendance",
            "top students by exam score",
            "correlation between hours studied and exam score",
            "count students below passing score",
            "show distribution of exam scores",
        ]
    else:
        suggested = [
            "show top categories",
            "count rows by status",
            "average numeric values by category",
            "show distribution by category",
        ]

    overview: dict[str, Any] = {
        "domain_guess": domain,
        "column_count": col_count or len(names),
        "detected_entities": entities,
        "detected_metrics": metrics,
        "detected_dimensions": dims,
        "suggested_questions": suggested,
    }
    if row_count is not None:
        overview["row_count"] = int(row_count)
    return overview
