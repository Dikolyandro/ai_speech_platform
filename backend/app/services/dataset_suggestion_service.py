from __future__ import annotations

import re
from typing import Any


def _column_name(column: dict[str, Any]) -> str:
    value = column.get("name")
    return value.strip() if isinstance(value, str) else ""


def _column_type(column: dict[str, Any]) -> str:
    return str(column.get("type") or column.get("data_type") or "").lower()


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def _friendly(name: str) -> str:
    return re.sub(r"[_-]+", " ", name).strip() or name


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
    if name.endswith("id") or name in {
        "vendorid",
        "ratecodeid",
        "pulocationid",
        "dolocationid",
        "paymenttype",
        "storeandfwdflag",
    }:
        return False
    return True


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
    if _is_numeric(column) or _is_date(column) or _is_text(column):
        return False
    if _is_boolean(column):
        return True
    typ = _column_type(column)
    distinct = _profile_number(column, ("distinct", "distinct_count", "unique_count", "n_unique"))
    if distinct is not None and distinct <= 80:
        return True
    return bool(re.search(r"(text|string|varchar|char|object)", typ))


def _make(
    suggestions: list[dict[str, Any]],
    *,
    title: str,
    query: str,
    required_columns: list[str],
    category: str,
    explanation: str,
) -> None:
    key = query.strip().lower()
    seen = {s["query"].lower() for s in suggestions}
    if key in seen:
        return
    suggestions.append(
        {
            "title": title.strip(),
            "query": query.strip(),
            "required_columns": list(required_columns),
            "category": category,
            "explanation": explanation.strip(),
        }
    )


def _is_supported_suggestion_query(query: str) -> bool:
    """Keep suggestions inside the safe patterns handled by /query/answer."""
    q = query.strip().lower().rstrip(".")
    safe_patterns = (
        r"^show average [a-zA-Z0-9_]+$",
        r"^show average [a-zA-Z0-9_]+ by [a-zA-Z0-9_]+$",
        r"^count (?:rows|trips|reviews) by [a-zA-Z0-9_]+$",
        r"^show top [a-zA-Z0-9_]+$",
        r"^show top (?:rows|trips|reviews) by [a-zA-Z0-9_]+$",
        r"^show (?:highest|lowest|longest) (?:rows|trips|reviews) by [a-zA-Z0-9_]+$",
        r"^show [a-zA-Z0-9_]+$",
        r"^show reviews where [a-zA-Z0-9_]+ is (?:greater|lower) than \d+(?:\.\d+)?$",
        r"^compare [a-zA-Z0-9_]+ by [a-zA-Z0-9_]+$",
    )
    return any(re.match(pattern, q, re.IGNORECASE) for pattern in safe_patterns)


def _validate_suggestions(suggestions: list[dict[str, Any]], available_columns: set[str]) -> list[dict[str, Any]]:
    """Final guard: no missing-column or unsupported-query suggestions leave the service."""
    valid: list[dict[str, Any]] = []
    seen_queries: set[str] = set()
    for suggestion in suggestions:
        required = suggestion.get("required_columns")
        query = str(suggestion.get("query") or "")
        if not isinstance(required, list) or not all(isinstance(item, str) for item in required):
            continue
        if any(_norm(column) not in available_columns for column in required):
            continue
        if not _is_supported_suggestion_query(query):
            continue
        key = query.strip().lower()
        if key in seen_queries:
            continue
        seen_queries.add(key)
        valid.append(suggestion)
    return valid


def build_dataset_suggestions(
    *,
    dataset_id: int,
    columns: list[dict[str, Any]],
    dataset_summary: dict[str, Any] | None = None,
    max_suggestions: int = 36,
) -> list[dict[str, Any]]:
    """Build chat suggestions strictly from the selected dataset's actual columns."""
    del dataset_id, dataset_summary
    real_columns = [c for c in columns if isinstance(c, dict) and _column_name(c)]
    by_norm = {_norm(_column_name(c)): c for c in real_columns}

    def has(key: str) -> bool:
        return _norm(key) in by_norm

    def actual(key: str) -> str:
        return _column_name(by_norm[_norm(key)])

    def add_avg(key: str, *, title: str, category: str, explanation: str) -> None:
        if has(key):
            col = actual(key)
            _make(
                suggestions,
                title=title,
                query=f"Show average {col}.",
                required_columns=[col],
                category=category,
                explanation=explanation,
            )

    def add_top_rows(key: str, *, title: str, category: str, explanation: str, adjective: str = "top") -> None:
        if has(key):
            col = actual(key)
            _make(
                suggestions,
                title=title,
                query=f"Show {adjective} trips by {col}.",
                required_columns=[col],
                category=category,
                explanation=explanation,
            )

    def add_count_by(key: str, *, title: str, category: str, explanation: str) -> None:
        if has(key):
            col = actual(key)
            _make(
                suggestions,
                title=title,
                query=f"Count trips by {col}.",
                required_columns=[col],
                category=category,
                explanation=explanation,
            )

    def add_top_values(key: str, *, title: str, category: str, explanation: str) -> None:
        if has(key):
            col = actual(key)
            _make(
                suggestions,
                title=title,
                query=f"Show top {col}.",
                required_columns=[col],
                category=category,
                explanation=explanation,
            )

    suggestions: list[dict[str, Any]] = []

    # NYC Yellow Taxi-like rules. All templates are gated by the actual selected sample schema.
    add_avg(
        "fare_amount",
        title="Average fare amount",
        category="Fare",
        explanation="Calculates the average fare from the selected taxi sample.",
    )
    add_top_rows(
        "fare_amount",
        title="Top trips by fare amount",
        category="Fare",
        explanation="Shows rows ordered by fare amount.",
    )
    add_top_rows(
        "fare_amount",
        title="Lowest trips by fare amount",
        category="Fare",
        explanation="Shows rows with the lowest fare amounts.",
        adjective="lowest",
    )
    add_avg(
        "total_amount",
        title="Average total amount",
        category="Fare",
        explanation="Calculates the average total charged amount.",
    )
    add_top_rows(
        "total_amount",
        title="Highest total amounts",
        category="Fare",
        explanation="Shows trips ordered by total amount.",
        adjective="highest",
    )
    if has("total_amount"):
        col = actual("total_amount")
        _make(
            suggestions,
            title="Total amount distribution",
            query=f"Count trips by {col}.",
            required_columns=[col],
            category="Fare",
            explanation="Groups trips by total amount values.",
        )
    add_avg(
        "tip_amount",
        title="Average tip amount",
        category="Fare",
        explanation="Calculates the average tip amount.",
    )
    add_top_rows(
        "tip_amount",
        title="Top trips by tip amount",
        category="Fare",
        explanation="Shows trips ordered by tip amount.",
    )
    add_count_by(
        "payment_type",
        title="Count trips by payment type",
        category="Payments",
        explanation="Counts taxi trips grouped by payment type.",
    )
    add_top_values(
        "payment_type",
        title="Top payment types",
        category="Payments",
        explanation="Shows the most common payment type values.",
    )

    add_avg(
        "trip_distance",
        title="Average trip distance",
        category="Distance",
        explanation="Calculates the average taxi trip distance.",
    )
    add_top_rows(
        "trip_distance",
        title="Longest trips",
        category="Distance",
        explanation="Shows trips ordered by distance.",
        adjective="longest",
    )
    if has("trip_distance"):
        col = actual("trip_distance")
        _make(
            suggestions,
            title="Trip distance distribution",
            query=f"Count trips by {col}.",
            required_columns=[col],
            category="Distance",
            explanation="Groups trips by distance values.",
        )

    add_count_by(
        "passenger_count",
        title="Count trips by passenger count",
        category="Passengers",
        explanation="Counts trips grouped by passenger count.",
    )
    add_avg(
        "passenger_count",
        title="Average passenger count",
        category="Passengers",
        explanation="Calculates the average passenger count.",
    )
    if has("passenger_count"):
        col = actual("passenger_count")
        _make(
            suggestions,
            title="Passenger count distribution",
            query=f"Count trips by {col}.",
            required_columns=[col],
            category="Passengers",
            explanation="Groups trips by passenger count values.",
        )

    add_top_values(
        "PULocationID",
        title="Top pickup locations",
        category="Locations",
        explanation="Shows the most common pickup location IDs.",
    )
    add_top_values(
        "DOLocationID",
        title="Top dropoff locations",
        category="Locations",
        explanation="Shows the most common dropoff location IDs.",
    )

    add_count_by(
        "tpep_pickup_datetime",
        title="Count trips by pickup date",
        category="Time",
        explanation="Counts trips grouped by pickup datetime values.",
    )
    add_top_values(
        "tpep_pickup_datetime",
        title="Busiest pickup times",
        category="Time",
        explanation="Shows the most common pickup datetime values.",
    )
    add_count_by(
        "tpep_dropoff_datetime",
        title="Count trips by dropoff date",
        category="Time",
        explanation="Counts trips grouped by dropoff datetime values.",
    )

    for fee_key, label in (
        ("extra", "extra charges"),
        ("mta_tax", "MTA tax"),
        ("tolls_amount", "tolls amount"),
        ("improvement_surcharge", "improvement surcharge"),
        ("congestion_surcharge", "congestion surcharge"),
        ("Airport_fee", "airport fee"),
        ("cbd_congestion_fee", "CBD congestion fee"),
    ):
        add_avg(
            fee_key,
            title=f"Average {label}",
            category="Fees",
            explanation=f"Calculates the average {label}.",
        )
        add_top_rows(
            fee_key,
            title=f"Top trips by {label}",
            category="Fees",
            explanation=f"Shows trips ordered by {label}.",
        )

    add_count_by(
        "VendorID",
        title="Count trips by vendor",
        category="Vendors",
        explanation="Counts taxi trips grouped by vendor.",
    )
    if has("VendorID") and has("fare_amount"):
        vendor = actual("VendorID")
        fare = actual("fare_amount")
        _make(
            suggestions,
            title="Average fare by vendor",
            query=f"Show average {fare} by {vendor}.",
            required_columns=[fare, vendor],
            category="Vendors",
            explanation="Compares average fare amount between vendors.",
        )
    if has("VendorID") and has("trip_distance"):
        vendor = actual("VendorID")
        distance = actual("trip_distance")
        _make(
            suggestions,
            title="Average trip distance by vendor",
            query=f"Show average {distance} by {vendor}.",
            required_columns=[distance, vendor],
            category="Vendors",
            explanation="Compares average trip distance between vendors.",
        )

    # Amazon review-like rules. Each suggestion is gated by the exact required columns.
    if has("overall"):
        col = actual("overall")
        _make(
            suggestions,
            title="Average rating",
            query=f"Show average {col}.",
            required_columns=[col],
            category="numeric",
            explanation="Uses the rating column in this selected dataset.",
        )
        _make(
            suggestions,
            title="Count reviews by rating",
            query=f"Count reviews by {col}.",
            required_columns=[col],
            category="categorical",
            explanation="Groups rows by the rating column.",
        )
        _make(
            suggestions,
            title="Rating distribution",
            query=f"Show distribution of {col}.",
            required_columns=[col],
            category="categorical",
            explanation="Shows how reviews are spread across rating values.",
        )
        _make(
            suggestions,
            title="Reviews with high rating",
            query=f"Show reviews where {col} is greater than 4.",
            required_columns=[col],
            category="filter",
            explanation="Filters rows using the rating column.",
        )
        _make(
            suggestions,
            title="Reviews with low rating",
            query=f"Show reviews where {col} is lower than 3.",
            required_columns=[col],
            category="filter",
            explanation="Filters rows using the rating column.",
        )

    if has("asin"):
        col = actual("asin")
        _make(
            suggestions,
            title="Top products by number of reviews",
            query=f"Count rows by {col}.",
            required_columns=[col],
            category="categorical",
            explanation="Counts rows grouped by product id.",
        )

    if has("verified"):
        col = actual("verified")
        _make(
            suggestions,
            title="Count reviews by verified status",
            query=f"Count rows by {col}.",
            required_columns=[col],
            category="boolean",
            explanation="Counts reviews by verification status.",
        )
        if has("overall"):
            overall = actual("overall")
            _make(
                suggestions,
                title="Compare verified and non-verified ratings",
                query=f"Compare {overall} by {col}.",
                required_columns=[overall, col],
                category="boolean",
                explanation="Requires both rating and verified columns.",
            )

    for date_key in ("reviewTime", "unixReviewTime"):
        if has(date_key):
            col = actual(date_key)
            _make(
                suggestions,
                title="Review trends over time",
                query=f"Count reviews by {col}.",
                required_columns=[col],
                category="date",
                explanation="Groups reviews by the available time column.",
            )
            break

    if has("reviewText"):
        col = actual("reviewText")
        _make(
            suggestions,
            title="Show sample review texts",
            query=f"Show {col}.",
            required_columns=[col],
            category="text",
            explanation="Shows sample rows containing review text.",
        )

    if has("summary"):
        col = actual("summary")
        _make(
            suggestions,
            title="Most common review summaries",
            query=f"Count rows by {col}.",
            required_columns=[col],
            category="text",
            explanation="Counts repeated summary values in this dataset.",
        )
        _make(
            suggestions,
            title="Show sample summaries",
            query=f"Show {col}.",
            required_columns=[col],
            category="text",
            explanation="Shows sample rows containing summary text.",
        )

    if has("vote"):
        col = actual("vote")
        _make(
            suggestions,
            title="Average helpful votes",
            query=f"Show average {col}.",
            required_columns=[col],
            category="numeric",
            explanation="Uses the helpful vote column.",
        )
        _make(
            suggestions,
            title="Top reviews by helpful votes",
            query=f"Show top rows by {col}.",
            required_columns=[col],
            category="numeric",
            explanation="Sorts rows by the helpful vote column.",
        )

    for reviewer_key, label in (("reviewerID", "reviewers"), ("reviewerName", "reviewer names")):
        if has(reviewer_key):
            col = actual(reviewer_key)
            _make(
                suggestions,
                title=f"Top {label} by number of reviews",
                query=f"Count rows by {col}.",
                required_columns=[col],
                category="categorical",
                explanation=f"Counts rows grouped by {label}.",
            )

    if has("style"):
        col = actual("style")
        _make(
            suggestions,
            title="Count reviews by product style",
            query=f"Count rows by {col}.",
            required_columns=[col],
            category="categorical",
            explanation="Groups rows by product style.",
        )

    if has("image"):
        col = actual("image")
        _make(
            suggestions,
            title="Count reviews with images",
            query=f"Count rows by {col}.",
            required_columns=[col],
            category="boolean",
            explanation="Groups rows by the image column present in this dataset.",
        )

    # Generic fallback, still based only on actual selected columns.
    numeric = [c for c in real_columns if _is_numeric_measure(c)]
    categorical = [c for c in real_columns if _is_categorical(c)]
    text_cols = [c for c in real_columns if _is_text(c)]
    date_cols = [c for c in real_columns if _is_date(c)]
    boolean_cols = [c for c in real_columns if _is_boolean(c)]

    for col_obj in numeric[:3]:
        col = _column_name(col_obj)
        _make(
            suggestions,
            title=f"Average {_friendly(col)}",
            query=f"Show average {col}.",
            required_columns=[col],
            category="numeric",
            explanation="Uses a numeric column from this selected dataset.",
        )

    for col_obj in categorical[:4]:
        col = _column_name(col_obj)
        _make(
            suggestions,
            title=f"Count rows by {_friendly(col)}",
            query=f"Count rows by {col}.",
            required_columns=[col],
            category="categorical",
            explanation="Groups rows by a categorical column from this selected dataset.",
        )

    for col_obj in boolean_cols[:2]:
        col = _column_name(col_obj)
        _make(
            suggestions,
            title=f"Compare rows by {_friendly(col)}",
            query=f"Count rows by {col}.",
            required_columns=[col],
            category="boolean",
            explanation="Compares rows by a boolean-like column from this selected dataset.",
        )

    for col_obj in date_cols[:2]:
        col = _column_name(col_obj)
        _make(
            suggestions,
            title=f"Trends by {_friendly(col)}",
            query=f"Count rows by {col}.",
            required_columns=[col],
            category="date",
            explanation="Groups rows by a date/time-like column from this selected dataset.",
        )

    for col_obj in text_cols[:2]:
        col = _column_name(col_obj)
        _make(
            suggestions,
            title=f"Show sample {_friendly(col)}",
            query=f"Show {col}.",
            required_columns=[col],
            category="text",
            explanation="Shows sample rows for a text column from this selected dataset.",
        )

    validated = _validate_suggestions(suggestions, set(by_norm.keys()))
    return validated[:max_suggestions]
