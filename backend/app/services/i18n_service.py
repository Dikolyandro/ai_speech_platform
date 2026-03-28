from __future__ import annotations

import re
from typing import Literal

PreferredLanguage = Literal["ru", "en", "kk"]
SUPPORTED_LANGUAGES: tuple[str, ...] = ("ru", "en", "kk")
DEFAULT_LANGUAGE: PreferredLanguage = "ru"


def normalize_preferred_language(value: str | None) -> PreferredLanguage:
    v = (value or "").strip().lower()
    if v in SUPPORTED_LANGUAGES:
        return v  # type: ignore[return-value]
    return DEFAULT_LANGUAGE


_RE_CYRILLIC = re.compile(r"[А-Яа-яЁёӘәҒғҚқҢңӨөҰұҮүҺһІі]")
_RE_LATIN = re.compile(r"[A-Za-z]")
_RE_KK_ONLY = re.compile(r"[ӘәҒғҚқҢңӨөҰұҮүҺһІі]")
_KK_HINT_WORDS = (
    "және",
    "бойынша",
    "көрсет",
    "көрсету",
    "сұрыптау",
    "лимит",
    "барлығы",
    "орташа",
    "ең",
)


def validate_query_language(text: str, preferred_language: str) -> tuple[bool, str]:
    """
    Validate user query language against the account language.
    Returns (ok, reason_code).
    reason_code: ok | empty | need_en | need_ru | need_kk
    """
    q = (text or "").strip()
    if not q:
        return (False, "empty")

    has_cyr = bool(_RE_CYRILLIC.search(q))
    has_lat = bool(_RE_LATIN.search(q))
    low = q.lower()
    lang = normalize_preferred_language(preferred_language)

    if lang == "en":
        # Strict mode: no Cyrillic in EN account.
        if has_cyr:
            return (False, "need_en")
        return (True, "ok")

    if lang == "ru":
        # Strict mode: RU expects Cyrillic and no Latin words.
        if not has_cyr or has_lat:
            return (False, "need_ru")
        # If clearly Kazakh-specific letters dominate, ask to switch account language.
        if _RE_KK_ONLY.search(q):
            return (False, "need_ru")
        return (True, "ok")

    # lang == "kk"
    if has_lat:
        return (False, "need_kk")
    if not has_cyr:
        return (False, "need_kk")
    # Need at least one Kazakh marker (letter or frequent word),
    # otherwise RU and KK become indistinguishable in Cyrillic.
    if _RE_KK_ONLY.search(q):
        return (True, "ok")
    if any(w in low for w in _KK_HINT_WORDS):
        return (True, "ok")
    return (False, "need_kk")

