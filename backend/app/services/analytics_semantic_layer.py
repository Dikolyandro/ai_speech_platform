"""
Deterministic semantic understanding for analytical NL queries.

This module inspects a natural-language query and tabular column metadata to
propose likely metrics, dimensions, and date fields. It does **not** generate
SQL, call external APIs, or touch the database.

Includes extended heuristics for **entertainment / Netflix-style** catalogs
(titles, genres in ``listed_in``, ``Movie`` vs ``TV Show``, directors, cast,
countries, release years, and textual durations).

Intended to be called optionally from higher layers (e.g. future integration
with ``routes_answer``) behind feature flags, without changing existing behavior.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Final, Iterable, Literal

from app.services.dataset_overview_service import unwrap_dataset_columns_json

# ----- Public data models -------------------------------------------------

BusinessType = Literal["metric", "dimension", "date", "id", "text", "unknown"]


@dataclass(frozen=True)
class ColumnSemanticInfo:
    """Semantic view of a single dataset column (schema-level, no row stats)."""

    name: str
    type: str
    normalized_name: str
    business_type: BusinessType
    aliases: tuple[str, ...] = ()
    confidence: float = 0.0


@dataclass(frozen=True)
class SemanticMatch:
    """A column the layer believes matches part of the user's intent."""

    column: str
    score: float
    evidence: str = ""


@dataclass
class AnalyticsSemanticResult:
    """Full semantic interpretation for one user query (no SQL)."""

    original_query: str
    normalized_query: str
    detected_metric: SemanticMatch | None = None
    detected_dimension: SemanticMatch | None = None
    detected_date_column: SemanticMatch | None = None
    detected_filters: list[SemanticMatch] = field(default_factory=list)
    query_keywords: list[str] = field(default_factory=list)
    confidence: float = 0.0
    debug: dict[str, Any] = field(default_factory=dict)


# ----- Normalization & keywords -------------------------------------------

_WS_RE: Final[re.Pattern[str]] = re.compile(r"\s+")
_NON_ALNUM_RE: Final[re.Pattern[str]] = re.compile(r"[^0-9a-zA-Zа-яА-ЯёЁәғқңөұүһі\s]+", re.UNICODE)

# Stopwords by coarse language bucket (deterministic, small lists).
_STOPWORDS_EN: Final[frozenset[str]] = frozenset(
    {
        "the",
        "a",
        "an",
        "and",
        "or",
        "to",
        "of",
        "in",
        "on",
        "for",
        "with",
        "by",
        "from",
        "as",
        "at",
        "is",
        "are",
        "was",
        "were",
        "be",
        "been",
        "being",
        "it",
        "this",
        "that",
        "these",
        "those",
        "show",
        "display",
        "list",
        "give",
        "get",
        "top",
        "all",
        "each",
        "per",
    }
)
_STOPWORDS_RU: Final[frozenset[str]] = frozenset(
    {
        "и",
        "в",
        "во",
        "на",
        "по",
        "про",
        "для",
        "из",
        "к",
        "со",
        "от",
        "до",
        "как",
        "что",
        "это",
        "тот",
        "та",
        "те",
        "не",
        "ни",
        "а",
        "но",
        "или",
        "же",
        "ли",
        "бы",
        "покажи",
        "показать",
        "выведи",
        "дай",
        "дайте",
        "все",
        "всё",
        "кажд",
        "топ",
    }
)
_STOPWORDS_KK: Final[frozenset[str]] = frozenset(
    {
        "және",
        "мен",
        "үшін",
        "туралы",
        "бойынша",
        "бұл",
        "сол",
        "немесе",
        "да",
        "де",
        "көрсет",
        "бер",
        "барлық",
        "әр",
        "топ",
    }
)

# Phrase-level intent tokens (still removed from "keywords" but used elsewhere if needed).
_EXTRA_STOP: Final[frozenset[str]] = frozenset({"how", "many", "much", "сколько", "қанша"})


def normalize_text(text: str) -> str:
    """
    Lowercase, strip, collapse whitespace, and remove most punctuation for matching.

    Keeps Latin, Cyrillic, and Kazakh-specific letters; digits are preserved.
    """
    raw = (text or "").strip()
    if not raw:
        return ""
    # Unicode normalize for stable lowercasing of special letters.
    raw = unicodedata.normalize("NFKC", raw)
    raw = raw.lower()
    raw = _NON_ALNUM_RE.sub(" ", raw)
    raw = _WS_RE.sub(" ", raw).strip()
    return raw


def extract_query_keywords(query_normalized: str, user_lang: str) -> list[str]:
    """
    Tokenize a normalized query and drop language-specific stopwords.

    ``user_lang`` is a 2-letter code: ``en``, ``ru``, ``kk`` (anything else → English stopwords).
    """
    lang = (user_lang or "en").strip().lower()[:2]
    if lang == "ru":
        stops = _STOPWORDS_RU
    elif lang == "kk":
        stops = _STOPWORDS_KK
    else:
        stops = _STOPWORDS_EN

    parts = [p for p in query_normalized.split() if len(p) >= 2]
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        if p in stops or p in _EXTRA_STOP:
            continue
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


# ----- SQL type helpers ----------------------------------------------------

_NUMERIC_SQL: Final[frozenset[str]] = frozenset(
    {
        "int",
        "integer",
        "bigint",
        "smallint",
        "tinyint",
        "decimal",
        "numeric",
        "float",
        "double",
        "real",
        "money",
    }
)


def _sql_type_family(sql_type: str) -> str:
    t = (sql_type or "").lower()
    for key in ("datetime", "timestamp", "date", "time"):
        if key in t:
            return "temporal"
    for key in _NUMERIC_SQL:
        if key in t:
            return "numeric"
    if "char" in t or "text" in t or "clob" in t or t == "json":
        return "textual"
    return "other"


def infer_column_business_type(name: str, sql_type: str) -> BusinessType:
    """
    Map a physical column name and SQL type to a coarse business role.

    Heuristic only: no per-dataset statistics. Tuned for typical analytics CSVs
    and Netflix-style movie/TV catalog tables (title, listed_in, release_year, …).
    """
    n = (name or "").strip().lower()
    fam = _sql_type_family(sql_type)

    if fam == "temporal":
        return "date"

    # Name-driven date signals (e.g. year INTEGER used as time axis)
    if re.search(
        r"(^|_)(date|time|datetime|timestamp|month|year|day|week|quarter|"
        r"release_year|year_added|date_added|added_on|premiere|air_date)(_|$)?",
        n,
        re.IGNORECASE,
    ):
        if fam == "numeric" or fam == "textual":
            return "date"

    # Identifier columns
    if n == "id" or n.endswith("_id"):
        # avoid treating obvious measures as ids
        if fam == "numeric" and re.search(r"(amount|price|total|qty|quantity|payment|value|sum|count)", n):
            return "metric"
        return "id"

    # Runtime / length as a measure (numeric or textual like "90 min" — still ranked for "longest")
    if re.search(r"(^|_)(duration|runtime|length|minutes|mins)(_|$)?", n, re.IGNORECASE):
        return "metric"

    if fam == "numeric":
        if re.search(
            r"(amount|price|total|qty|quantity|payment|revenue|value|cost|fee|balance|"
            r"sales|income|earning|sum|avg|mean|score|count|seasons|episodes)",
            n,
            re.IGNORECASE,
        ):
            return "metric"
        # bare counts / measures often unnamed → still treat as metric candidate
        return "metric"

    if fam == "textual":
        # People / credits (group-by or "most common X")
        if re.search(
            r"(^|_)(director|filmmaker|producer|writer|screenwriter|creator|showrunner|"
            r"cast|starring|actors?|celebrit|crew)(_|$)?",
            n,
            re.IGNORECASE,
        ):
            return "dimension"
        # Genre / taxonomy / content bucket (Netflix listed_in, type, tags)
        if re.search(
            r"(^|_)(listed_in|genre|genres|tags|categories|classification|movie_type|"
            r"content_type|entertainment)(_|$)?",
            n,
            re.IGNORECASE,
        ):
            return "dimension"
        # Title / show identity (high-cardinality text; still matchable for "movies" queries)
        if re.search(
            r"(^|_)(title|show_title|movie_title|series_title|name|program|episode_name)(_|$)?",
            n,
            re.IGNORECASE,
        ):
            return "text"
        if re.search(
            r"(category|type|status|group|segment|channel|city|region|country|nation|"
            r"product|customer|client|user|seller|vendor|gender|state|language|studio)",
            n,
            re.IGNORECASE,
        ):
            return "dimension"
        return "text"

    return "unknown"


# ----- Alias expansion (query terms ↔ column roles) -----------------------

# Maps abstract "intent buckets" to extra tokens matched against queries and appended as aliases.
_METRIC_SYNONYMS_EN: Final[tuple[str, ...]] = (
    "revenue",
    "sales",
    "income",
    "earnings",
    "payment",
    "amount",
    "value",
    "total",
    "price",
    "quantity",
    "qty",
    "sum",
)
_METRIC_SYNONYMS_RU: Final[tuple[str, ...]] = (
    "доход",
    "продажи",
    "выручка",
    "сумма",
    "оплата",
    "платеж",
    "стоимость",
    "количество",
    "итог",
    "всего",
)
_METRIC_SYNONYMS_KK: Final[tuple[str, ...]] = (
    "табыс",
    "сату",
    "сома",
    "түсім",
    "саны",
    "баға",
    "құн",
)

_DIMENSION_SYNONYMS_EN: Final[tuple[str, ...]] = (
    "category",
    "type",
    "group",
    "status",
    "segment",
    "class",
)
_DIMENSION_SYNONYMS_RU: Final[tuple[str, ...]] = (
    "категория",
    "тип",
    "статус",
    "группа",
    "класс",
    "сегмент",
)
_DIMENSION_SYNONYMS_KK: Final[tuple[str, ...]] = (
    "санат",
    "түрі",
    "класс",
    "статус",
)

_DATE_SYNONYMS_EN: Final[tuple[str, ...]] = (
    "date",
    "day",
    "month",
    "year",
    "time",
    "week",
    "quarter",
)
_DATE_SYNONYMS_RU: Final[tuple[str, ...]] = (
    "дата",
    "день",
    "месяц",
    "год",
    "время",
    "неделя",
    "квартал",
)
_DATE_SYNONYMS_KK: Final[tuple[str, ...]] = (
    "күн",
    "ай",
    "жыл",
    "уақыт",
    "апта",
)

_CUSTOMER_SYNONYMS: Final[tuple[str, ...]] = (
    "customer",
    "client",
    "user",
    "buyer",
    "клиент",
    "покупатель",
    "пользователь",
    "тұтынушы",
)
_PRODUCT_SYNONYMS: Final[tuple[str, ...]] = (
    "product",
    "item",
    "goods",
    "sku",
    "товар",
    "продукт",
    "өнім",
    "тауар",
)
_SELLER_SYNONYMS: Final[tuple[str, ...]] = (
    "seller",
    "vendor",
    "merchant",
    "продавец",
    "поставщик",
    "сатушы",
)

# ----- Entertainment / Netflix-style catalog semantics --------------------

# Content kind (column often stores "Movie" vs "TV Show").
_MOVIE_KIND_EN: Final[tuple[str, ...]] = (
    "movie",
    "movies",
    "film",
    "films",
    "cinema",
    "cinematic",
    "picture",
    "flick",
)
_MOVIE_KIND_RU: Final[tuple[str, ...]] = ("фильм", "фильмы", "кино", "кинофильм", "картина")
_MOVIE_KIND_KK: Final[tuple[str, ...]] = ("фильм", "кино", "кинофильм")

_TV_KIND_EN: Final[tuple[str, ...]] = (
    "tv",
    "television",
    "series",
    "show",
    "shows",
    "season",
    "seasons",
    "episode",
    "episodes",
    "sitcom",
    "miniseries",
)
_TV_KIND_RU: Final[tuple[str, ...]] = ("сериал", "сериалы", "телесериал", "шоу", "тв", "телешоу")
_TV_KIND_KK: Final[tuple[str, ...]] = ("сериал", "телехикая", "шоу")

_GENRE_INTENT_EN: Final[tuple[str, ...]] = (
    "genre",
    "genres",
    "category",
    "categories",
    "listed",
    "listed in",
    "listedin",
    "taxonomy",
    "tag",
    "tags",
    "entertainment type",
    "content type",
    "content categories",
    "thematic",
)
_GENRE_INTENT_RU: Final[tuple[str, ...]] = (
    "жанр",
    "жанры",
    "категория",
    "категории",
    "рубрика",
    "подборка",
)
_GENRE_INTENT_KK: Final[tuple[str, ...]] = ("жанр", "жанрлар", "санат", "тақырып", "топтама")

_PEOPLE_INTENT_EN: Final[tuple[str, ...]] = (
    "director",
    "directors",
    "filmmaker",
    "filmmakers",
    "producer",
    "producers",
    "writer",
    "writers",
    "showrunner",
    "actor",
    "actors",
    "actress",
    "actresses",
    "cast",
    "casting",
    "starring",
    "stars",
    "celebrity",
    "celebrities",
    "crew",
)
_PEOPLE_INTENT_RU: Final[tuple[str, ...]] = (
    "режиссер",
    "режиссёр",
    "режиссеры",
    "постановщик",
    "актер",
    "актёр",
    "актеры",
    "актриса",
    "роли",
    "каст",
    "состав",
    "звезды",
)
_PEOPLE_INTENT_KK: Final[tuple[str, ...]] = (
    "режиссер",
    "режиссёр",
    "актер",
    "актёр",
    "рөл",
    "рөлдер",
    "каст",
)

_RELEASE_INTENT_EN: Final[tuple[str, ...]] = (
    "release",
    "releases",
    "released",
    "published",
    "launch",
    "launched",
    "premiere",
    "premiered",
    "debut",
    "recent",
    "newest",
    "latest",
    "oldest",
    "classic",
    "classics",
    "vintage",
    "old movies",
    "over time",
    "timeline",
    "trend",
    "trends",
    "year over year",
)
_RELEASE_INTENT_RU: Final[tuple[str, ...]] = (
    "премьера",
    "премьеры",
    "выпуск",
    "выпустили",
    "вышел",
    "вышли",
    "недавние",
    "новые",
    "новинки",
    "старые",
    "классика",
    "год выпуска",
    "по годам",
    "динамика",
    "тренд",
)
_RELEASE_INTENT_KK: Final[tuple[str, ...]] = (
    "шығару",
    "шығарылды",
    "жариялану",
    "премьера",
    "соңғы",
    "жаңа",
    "ескі",
    "классика",
    "шыққан жылы",
    "жылдар",
    "динамика",
)

_COUNTRY_INTENT_EN: Final[tuple[str, ...]] = (
    "country",
    "countries",
    "region",
    "regions",
    "nation",
    "nations",
    "international",
    "geography",
)
_COUNTRY_INTENT_RU: Final[tuple[str, ...]] = ("страна", "страны", "регион", "нация", "география")
_COUNTRY_INTENT_KK: Final[tuple[str, ...]] = ("ел", "елдер", "аудан", "аймақ", "халықаралық")

_DURATION_INTENT_EN: Final[tuple[str, ...]] = (
    "duration",
    "length",
    "lengths",
    "runtime",
    "runtimes",
    "longest",
    "shortest",
    "minutes",
    "minute",
    "mins",
    "hours",
    "hour",
    "running time",
)
_DURATION_INTENT_RU: Final[tuple[str, ...]] = (
    "длительность",
    "продолжительность",
    "длинные",
    "длинный",
    "короткие",
    "короткий",
    "минут",
    "часов",
)
_DURATION_INTENT_KK: Final[tuple[str, ...]] = (
    "ұзақтық",
    "ұзақтығы",
    "ұзақ",
    "қысқа",
    "минут",
    "сағат",
)

_TITLE_INTENT_EN: Final[tuple[str, ...]] = (
    "title",
    "titles",
    "movie name",
    "show name",
    "called",
    "named",
)
_TITLE_INTENT_RU: Final[tuple[str, ...]] = ("название", "названия", "называется", "зовут")
_TITLE_INTENT_KK: Final[tuple[str, ...]] = ("атауы", "атаулар", "атаулы")


def _column_matches_entertainment_pattern(name_lower: str, pattern: str) -> bool:
    return bool(re.search(pattern, name_lower, re.IGNORECASE))


def _entertainment_aliases_for_column(name: str) -> tuple[str, ...]:
    """Extra NL + value hints for catalog columns (Netflix-style datasets)."""
    n = (name or "").strip().lower()
    buckets: list[tuple[str, ...]] = []

    if _column_matches_entertainment_pattern(
        n,
        r"(^|_)(type|movie_type|content_type|show_type|media_type)(_|$)?",
    ):
        buckets.append(
            _MOVIE_KIND_EN
            + _MOVIE_KIND_RU
            + _MOVIE_KIND_KK
            + ("movie", "tv show")
            + _TV_KIND_EN
            + _TV_KIND_RU
            + _TV_KIND_KK
        )

    if _column_matches_entertainment_pattern(
        n,
        r"(listed_in|genre|genres|tags|categories|classification|thematic)",
    ):
        buckets.append(_GENRE_INTENT_EN + _GENRE_INTENT_RU + _GENRE_INTENT_KK)
        buckets.append(_DIMENSION_SYNONYMS_EN + _DIMENSION_SYNONYMS_RU + _DIMENSION_SYNONYMS_KK)

    if _column_matches_entertainment_pattern(
        n,
        r"(^|_)(director|filmmaker|producer|writer|screenwriter|creator|showrunner)(_|$)?",
    ):
        buckets.append(_PEOPLE_INTENT_EN + _PEOPLE_INTENT_RU + _PEOPLE_INTENT_KK)

    if _column_matches_entertainment_pattern(n, r"(^|_)(cast|starring|actors?)(_|$)?"):
        buckets.append(_PEOPLE_INTENT_EN + _PEOPLE_INTENT_RU + _PEOPLE_INTENT_KK)

    if _column_matches_entertainment_pattern(n, r"(country|nation|region|origin|filmed_in)"):
        buckets.append(_COUNTRY_INTENT_EN + _COUNTRY_INTENT_RU + _COUNTRY_INTENT_KK)

    if _column_matches_entertainment_pattern(
        n,
        r"(duration|runtime|length|minutes|mins|episode_length)",
    ):
        buckets.append(_DURATION_INTENT_EN + _DURATION_INTENT_RU + _DURATION_INTENT_KK)

    if _column_matches_entertainment_pattern(
        n,
        r"(^|_)(release_year|year_released|premiere_year|start_year)(_|$)?",
    ):
        buckets.append(_RELEASE_INTENT_EN + _RELEASE_INTENT_RU + _RELEASE_INTENT_KK)
        buckets.append(_DATE_SYNONYMS_EN + _DATE_SYNONYMS_RU + _DATE_SYNONYMS_KK)

    if _column_matches_entertainment_pattern(
        n,
        r"(^|_)(date_added|added_on|available_on|catalog_date)(_|$)?",
    ):
        buckets.append(_RELEASE_INTENT_EN + _RELEASE_INTENT_RU + _RELEASE_INTENT_KK)
        buckets.append(_DATE_SYNONYMS_EN + _DATE_SYNONYMS_RU + _DATE_SYNONYMS_KK)

    if _column_matches_entertainment_pattern(
        n,
        r"(^|_)(title|show_title|movie_title|series_title|program_name|episode_title)(_|$)?",
    ):
        buckets.append(_TITLE_INTENT_EN + _TITLE_INTENT_RU + _TITLE_INTENT_KK)
        buckets.append(_MOVIE_KIND_EN + _MOVIE_KIND_RU + _MOVIE_KIND_KK + _TV_KIND_EN + _TV_KIND_RU + _TV_KIND_KK)

    return _merge_alias_sets(*buckets) if buckets else tuple()


def _entertainment_score_boost(query_normalized: str, column_name: str) -> float:
    """Extra deterministic boost when catalog intent tokens align with column role."""
    if not query_normalized:
        return 0.0
    n = column_name.lower()
    boost = 0.0
    q = query_normalized

    def _hits_any(terms: tuple[str, ...]) -> bool:
        return any(t in q for t in terms if len(t) >= 3)

    # Genre / listed_in
    if _column_matches_entertainment_pattern(
        n,
        r"(listed_in|genre|genres|tags|categories|classification|thematic)",
    ) and (_hits_any(_GENRE_INTENT_EN + _GENRE_INTENT_RU + _GENRE_INTENT_KK) or "genre" in q):
        boost = max(boost, 0.14)

    # Movie vs TV (type column)
    if _column_matches_entertainment_pattern(
        n,
        r"(^|_)(type|movie_type|content_type|show_type|media_type)(_|$)?",
    ):
        if _hits_any(_MOVIE_KIND_EN + _MOVIE_KIND_RU + _MOVIE_KIND_KK) or _hits_any(
            _TV_KIND_EN + _TV_KIND_RU + _TV_KIND_KK
        ):
            boost = max(boost, 0.13)
        if "compare" in q and ("movie" in q or "film" in q or "series" in q or "show" in q):
            boost = max(boost, 0.11)

    # People → director / cast
    if _column_matches_entertainment_pattern(
        n,
        r"(^|_)(director|filmmaker|producer|writer|screenwriter|creator|showrunner)(_|$)?",
    ) and _hits_any(_PEOPLE_INTENT_EN + _PEOPLE_INTENT_RU + _PEOPLE_INTENT_KK):
        if "director" in q or "режисс" in q:
            boost = max(boost, 0.15)

    if _column_matches_entertainment_pattern(n, r"(^|_)(cast|starring|actors?)(_|$)?") and (
        "cast" in q or "actor" in q or "акт" in q or "star" in q
    ):
        boost = max(boost, 0.14)

    # Country / origin
    if _column_matches_entertainment_pattern(n, r"(country|nation|region|origin|filmed_in)") and _hits_any(
        _COUNTRY_INTENT_EN + _COUNTRY_INTENT_RU + _COUNTRY_INTENT_KK
    ):
        boost = max(boost, 0.14)

    # Duration / runtime (often metric column)
    if _column_matches_entertainment_pattern(
        n,
        r"(duration|runtime|length|minutes|mins|episode_length)",
    ) and _hits_any(_DURATION_INTENT_EN + _DURATION_INTENT_RU + _DURATION_INTENT_KK):
        boost = max(boost, 0.16)

    # Release / calendar axis
    if _column_matches_entertainment_pattern(
        n,
        r"(release_year|year_released|premiere_year|start_year|date_added|added_on|available_on)",
    ) and _hits_any(_RELEASE_INTENT_EN + _RELEASE_INTENT_RU + _RELEASE_INTENT_KK):
        boost = max(boost, 0.13)
    if _column_matches_entertainment_pattern(
        n,
        r"(release_year|year_released|premiere_year|start_year)",
    ) and (
        "trend" in q
        or "time" in q
        or "over" in q
        or "динамика" in q
        or "тренд" in q
        or "release" in q
        or "releases" in q
        or "timeline" in q
    ):
        boost = max(boost, 0.14)

    # Title column
    if _column_matches_entertainment_pattern(
        n,
        r"(^|_)(title|show_title|movie_title|series_title|program_name|episode_title)(_|$)?",
    ) and _hits_any(_TITLE_INTENT_EN + _TITLE_INTENT_RU + _TITLE_INTENT_KK):
        boost = max(boost, 0.1)

    return min(0.2, boost)


def _duration_metric_deprioritize_for_people_query(query_normalized: str, column_name: str) -> bool:
    """Avoid picking runtime/duration as the metric when the query is clearly about people/credits."""
    if not re.search(
        r"(^|_)(duration|runtime|length|episode_length|minutes)(_|$)?",
        column_name.lower(),
    ):
        return False
    q = query_normalized
    people = any(
        x in q
        for x in (
            "director",
            "cast",
            "actor",
            "actress",
            "producer",
            "writer",
            "режисс",
            "акт",
            "сценар",
            "каст",
        )
    )
    duration_intent = any(
        x in q
        for x in (
            "duration",
            "longest",
            "shortest",
            "runtime",
            "length",
            "minute",
            "длительност",
            "продолжит",
            "ұзақтығы",
            "ұзақтық",
            "mins",
            "hours",
        )
    )
    return people and not duration_intent


def _catalog_ingest_date_deprioritize(query_normalized: str, column_name: str) -> bool:
    """When the query is about theatrical/catalog release years, not platform ingest dates."""
    if not re.search(r"(date_added|added_on|available_on|catalog_date)", column_name.lower()):
        return False
    q = query_normalized
    release_axis = any(
        x in q
        for x in (
            "release",
            "releases",
            "released",
            "premiere",
            "year over",
            "trend",
            "timeline",
            "динамика",
            "тренд",
            "годам",
            "по годам",
            "выпуск",
            "премьер",
            "шыққан жылы",
            "жылдар",
        )
    )
    ingest_axis = any(
        x in q
        for x in ("date added", "added to", "catalog", "available on", "добавлен", "поступил")
    )
    return release_axis and not ingest_axis


def _name_tokens(name: str) -> list[str]:
    parts = re.split(r"[^a-zA-Zа-яА-ЯёЁәғқңөұүһі0-9]+", (name or "").lower())
    return [p for p in parts if len(p) >= 2]


def _merge_alias_sets(*groups: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for g in groups:
        for s in g:
            s2 = s.strip().lower()
            if len(s2) < 2 or s2 in seen:
                continue
            seen.add(s2)
            out.append(s2)
    return tuple(out)


def _aliases_for_column(name: str, business_type: BusinessType) -> tuple[str, ...]:
    """Build search aliases: normalized tokens + role synonyms if the name fits."""
    tokens = _name_tokens(name)
    base: list[str] = list(tokens)
    nlow = name.lower()

    extra: list[str] = []
    if business_type == "metric":
        extra.extend(_METRIC_SYNONYMS_EN + _METRIC_SYNONYMS_RU + _METRIC_SYNONYMS_KK)
    elif business_type == "dimension":
        extra.extend(_DIMENSION_SYNONYMS_EN + _DIMENSION_SYNONYMS_RU + _DIMENSION_SYNONYMS_KK)
        if re.search(r"(customer|client|user|buyer|клиент|покупатель|тұтынушы)", nlow):
            extra.extend(_CUSTOMER_SYNONYMS)
        if re.search(r"(product|item|goods|sku|товар|продукт|өнім|тауар)", nlow):
            extra.extend(_PRODUCT_SYNONYMS)
        if re.search(r"(seller|vendor|merchant|продавец|сатушы)", nlow):
            extra.extend(_SELLER_SYNONYMS)
    elif business_type == "date":
        extra.extend(_DATE_SYNONYMS_EN + _DATE_SYNONYMS_RU + _DATE_SYNONYMS_KK)

    return _merge_alias_sets(base, extra, _entertainment_aliases_for_column(name))


def build_column_semantic_info(
    name: str,
    sql_type: str,
    *,
    columns_json_row: dict[str, Any] | None = None,
) -> ColumnSemanticInfo:
    """
    Construct :class:`ColumnSemanticInfo` from a column name/type and optional profile row.

    If ``columns_json_row`` contains ``semantic.is_categorical`` / profile hints,
    textual columns may be promoted to ``dimension`` with slightly higher confidence.
    """
    clean_name = (name or "").strip()
    typ = sql_type or ""
    norm = normalize_text(clean_name.replace("_", " "))
    bt: BusinessType = infer_column_business_type(clean_name, typ)
    conf = 0.55

    sem: dict[str, Any] = {}
    if isinstance(columns_json_row, dict):
        raw_sem = columns_json_row.get("semantic")
        if isinstance(raw_sem, dict):
            sem = raw_sem

    if bt == "text" and sem.get("is_categorical") is True:
        bt = "dimension"
        conf = 0.62

    if bt == "metric":
        conf = 0.68
    elif bt == "date":
        conf = 0.7
    elif bt == "dimension":
        conf = 0.64
    elif bt == "id":
        conf = 0.72

    aliases = _aliases_for_column(clean_name, bt)
    return ColumnSemanticInfo(
        name=clean_name,
        type=typ,
        normalized_name=norm,
        business_type=bt,
        aliases=aliases,
        confidence=float(conf),
    )


def score_column_match(query_normalized: str, column: ColumnSemanticInfo) -> float:
    """
    Deterministic score in ``[0, 1]`` for how well the query refers to this column.

    Uses substring checks for column name, normalized name tokens, and alias overlaps.
    """
    if not query_normalized:
        return 0.0
    q = f" {query_normalized} "
    score = 0.0

    # Direct name / underscored name mention
    nl = column.name.lower()
    if len(nl) >= 3 and nl in query_normalized:
        score = max(score, 0.85)

    # Token overlap with column name pieces
    q_tokens = set(query_normalized.split())
    name_tokens = set(_name_tokens(column.name))
    overlap = len(q_tokens & name_tokens)
    if overlap:
        score = max(score, min(0.45 + 0.12 * overlap, 0.9))

    # Alias hits (stronger if multi-word alias appears)
    for al in column.aliases:
        if len(al) < 2:
            continue
        if " " in al:
            if al in query_normalized:
                score = max(score, 0.88)
        else:
            if al in q_tokens:
                score = max(score, 0.75)
            elif len(al) >= 4 and al in query_normalized:
                score = max(score, 0.7)
            elif len(al) >= 4:
                # Inflected forms (e.g. RU «категориям» vs stem «категория»)
                for qt in q_tokens:
                    if len(qt) >= 4 and (al in qt or qt in al or al[:4] == qt[:4]):
                        score = max(score, 0.66)
                        break

    # Business-type generic tokens in query (e.g. "revenue" boosts metric columns)
    if column.business_type == "metric":
        for t in _METRIC_SYNONYMS_EN + _METRIC_SYNONYMS_RU + _METRIC_SYNONYMS_KK:
            if t in q_tokens or t in query_normalized:
                score = max(score, 0.55)
                break
    elif column.business_type == "dimension":
        for t in _DIMENSION_SYNONYMS_EN + _DIMENSION_SYNONYMS_RU + _DIMENSION_SYNONYMS_KK:
            if t in q_tokens or t in query_normalized:
                score = max(score, 0.52)
                break
    elif column.business_type == "date":
        for t in _DATE_SYNONYMS_EN + _DATE_SYNONYMS_RU + _DATE_SYNONYMS_KK:
            if t in q_tokens or t in query_normalized:
                score = max(score, 0.58)
                break

    score = min(1.0, score + _entertainment_score_boost(query_normalized, column.name))
    if _duration_metric_deprioritize_for_people_query(query_normalized, column.name):
        score = min(score, 0.24)
    if _catalog_ingest_date_deprioritize(query_normalized, column.name):
        score = min(score, 0.34)
    return score


def _best_match(
    candidates: list[ColumnSemanticInfo],
    query_normalized: str,
    *,
    allowed_types: set[BusinessType],
    keyword_boost: list[str],
    min_score: float = 0.12,
) -> tuple[SemanticMatch | None, dict[str, Any]]:
    """Pick highest-scoring column among candidates with optional keyword boosts."""
    dbg_rows: list[dict[str, Any]] = []
    best: SemanticMatch | None = None
    kw_set = set(keyword_boost)

    for col in candidates:
        if col.business_type not in allowed_types:
            continue
        s = score_column_match(query_normalized, col)
        # Light boost if extracted keywords hit column name tokens
        for kt in kw_set:
            if kt in _name_tokens(col.name) or any(kt == a for a in col.aliases if len(kt) >= 3):
                s = min(1.0, s + 0.08)
        dbg_rows.append({"column": col.name, "business_type": col.business_type, "score": round(s, 4)})
        if s < min_score:
            continue
        if best is None or s > best.score:
            best = SemanticMatch(column=col.name, score=float(s), evidence="score_column_match")

    dbg_rows.sort(key=lambda r: r["score"], reverse=True)
    return best, {"ranked": dbg_rows[:12]}


def _overall_confidence(
    metric: SemanticMatch | None,
    dimension: SemanticMatch | None,
    date_c: SemanticMatch | None,
) -> float:
    parts: list[float] = []
    if metric:
        parts.append(metric.score)
    if dimension:
        parts.append(dimension.score)
    if date_c:
        parts.append(date_c.score)
    if not parts:
        return 0.15
    base = sum(parts) / len(parts)
    # Penalize ambiguity: two strong metrics not handled here — keep simple
    return max(0.1, min(0.95, base))


def analyze_query_semantics(
    query: str,
    columns: list[tuple[str, str]],
    columns_json: list[dict] | None = None,
    user_lang: str = "en",
) -> AnalyticsSemanticResult:
    """
    Analyze a user query against dataset columns (deterministic, no I/O).

    Parameters
    ----------
    query:
        Raw user question.
    columns:
        List of ``(column_name, sql_type)`` as used elsewhere in the backend.
    columns_json:
        Optional ``DatasetTableMeta.columns_json`` list for profile hints
        (e.g. ``semantic.is_categorical``).
    user_lang:
        ``en`` | ``ru`` | ``kk`` — steers stopword removal for keyword extraction.

    Returns
    -------
    AnalyticsSemanticResult
        Structured guesses + debug payloads. Safe to ignore until wired in.
    """
    original = query or ""
    normalized = normalize_text(original)
    keywords = extract_query_keywords(normalized, user_lang)

    json_by_name: dict[str, dict[str, Any]] = {}
    cols_json_list = unwrap_dataset_columns_json(columns_json) if columns_json is not None else []
    for row in cols_json_list:
        if isinstance(row, dict):
            nm = row.get("name")
            if isinstance(nm, str) and nm.strip():
                json_by_name[nm.strip()] = row

    col_infos: list[ColumnSemanticInfo] = []
    for name, typ in columns:
        row = json_by_name.get(name.strip())
        col_infos.append(build_column_semantic_info(name, typ, columns_json_row=row))

    metrics_pool = [c for c in col_infos if c.business_type == "metric"]
    dims_pool = [c for c in col_infos if c.business_type in ("dimension", "text")]
    dates_pool = [c for c in col_infos if c.business_type == "date"]

    # If no explicit date-typed columns, allow name-like date fields typed as numeric/text
    if not dates_pool:
        seen_date_names: set[str] = set()
        for c in col_infos:
            if c.business_type in ("dimension", "text", "metric", "unknown"):
                if re.search(
                    r"(date|time|month|year|day|week|release|added|premiere|available)",
                    c.name.lower(),
                ):
                    if c.name in seen_date_names:
                        continue
                    seen_date_names.add(c.name)
                    dates_pool.append(
                        ColumnSemanticInfo(
                            name=c.name,
                            type=c.type,
                            normalized_name=c.normalized_name,
                            business_type="date",
                            aliases=tuple(dict.fromkeys(c.aliases + _DATE_SYNONYMS_EN + _DATE_SYNONYMS_RU + _DATE_SYNONYMS_KK)),
                            confidence=0.5,
                        )
                    )

    m_match, m_dbg = _best_match(
        col_infos,
        normalized,
        allowed_types={"metric"},
        keyword_boost=keywords,
    )
    # If strict metric pool empty, fall back to unknown numeric-ish via col_infos with metric scores
    if m_match is None or m_match.score < 0.25:
        m2, m2_dbg = _best_match(
            col_infos,
            normalized,
            allowed_types={"metric", "unknown"},
            keyword_boost=keywords,
        )
        if m2 and (m_match is None or m2.score > (m_match.score if m_match else 0)):
            m_match, m_dbg = m2, m2_dbg

    d_match, d_dbg = _best_match(
        dims_pool if dims_pool else col_infos,
        normalized,
        allowed_types={"dimension", "text"},
        keyword_boost=keywords,
    )

    dt_match, dt_dbg = _best_match(
        dates_pool,
        normalized,
        allowed_types={"date"},
        keyword_boost=keywords,
    )

    # Filters: secondary strong dimension hits (not chosen as primary dimension)
    filters: list[SemanticMatch] = []
    if d_match:
        for col in dims_pool:
            if col.name == d_match.column:
                continue
            s = score_column_match(normalized, col)
            if s >= 0.62:
                filters.append(SemanticMatch(column=col.name, score=s, evidence="secondary_dimension"))

    conf = _overall_confidence(m_match, d_match, dt_match)
    if not normalized:
        conf = 0.05

    debug: dict[str, Any] = {
        "metric_ranking": m_dbg,
        "dimension_ranking": d_dbg,
        "date_ranking": dt_dbg,
        "columns_classified": [
            {
                "name": c.name,
                "business_type": c.business_type,
                "confidence": c.confidence,
            }
            for c in col_infos
        ],
    }

    return AnalyticsSemanticResult(
        original_query=original,
        normalized_query=normalized,
        detected_metric=m_match,
        detected_dimension=d_match,
        detected_date_column=dt_match,
        detected_filters=filters,
        query_keywords=keywords,
        confidence=float(conf),
        debug=debug,
    )
