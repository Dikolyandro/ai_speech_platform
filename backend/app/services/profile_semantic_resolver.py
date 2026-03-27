from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


def _norm_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^0-9a-zA-Zа-яА-ЯёЁ ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _tokenize(s: str) -> List[str]:
    s = _norm_text(s)
    toks = [t for t in s.split(" ") if len(t) >= 2]
    return toks[:64]


def _extract_ngrams(tokens: List[str], max_n: int = 3) -> List[str]:
    out: List[str] = []
    for n in range(1, max_n + 1):
        for i in range(0, max(0, len(tokens) - n + 1)):
            out.append(" ".join(tokens[i : i + n]))
    # prefer longer first
    out.sort(key=lambda x: (-len(x.split(" ")), x))
    return out


@dataclass(frozen=True)
class ProfileFilter:
    column: str
    op: str  # currently only "=" (safe, conservative)
    value: str
    score: float
    evidence: str


def _columns_from_profile(columns_json: Any) -> List[Dict[str, Any]]:
    if not isinstance(columns_json, list):
        return []
    out: List[Dict[str, Any]] = []
    for it in columns_json:
        if not isinstance(it, dict):
            continue
        name = it.get("name")
        typ = it.get("type")
        sem = it.get("semantic") if isinstance(it.get("semantic"), dict) else {}
        if isinstance(name, str) and name.strip():
            out.append({"name": name, "type": str(typ or ""), "semantic": sem})
    return out


def resolve_profile_value_filters(
    *,
    query_text: str,
    columns_json: Any,
    max_filters: int = 6,
) -> Tuple[List[ProfileFilter], Dict[str, Any]]:
    """
    Dataset-agnostic value resolution.
    Finds query fragments that match categorical values from the stored dataset profile.

    Conservative strategy:
      - only uses low-cardinality TEXT columns (semantic.categorical_values_norm present)
      - adds only "=" filters
      - ranks by uniqueness across columns and ngram length
    """
    cols = _columns_from_profile(columns_json)
    tokens = _tokenize(query_text)
    ngrams = _extract_ngrams(tokens, max_n=3)

    # build value -> columns index
    val_to_cols: Dict[str, List[str]] = {}
    col_to_vals: Dict[str, set[str]] = {}
    for c in cols:
        sem = c.get("semantic") or {}
        vals = sem.get("categorical_values_norm")
        if not isinstance(vals, list) or not vals:
            continue
        vset = {str(v) for v in vals if isinstance(v, str) and v.strip()}
        if not vset:
            continue
        col_to_vals[c["name"]] = vset
        for v in vset:
            val_to_cols.setdefault(v, []).append(c["name"])

    matches: List[ProfileFilter] = []
    matched_spans: List[str] = []
    used_cols: set[str] = set()

    for ng in ngrams:
        if len(matches) >= max_filters:
            break
        if ng in matched_spans:
            continue
        cols_for_val = val_to_cols.get(ng)
        if not cols_for_val:
            continue

        # choose a column: prefer unique value->column mapping
        if len(cols_for_val) == 1:
            chosen = cols_for_val[0]
            base = 0.92
        else:
            # ambiguous: pick the column where this value is most "prominent".
            # We don't store full frequencies, so use heuristic: prefer columns with smaller cardinality.
            cand_scores: List[Tuple[str, float]] = []
            for col in cols_for_val:
                card = len(col_to_vals.get(col, set()))
                s = 0.65 + (0.20 if card <= 12 else 0.10 if card <= 30 else 0.0)
                cand_scores.append((col, s))
            cand_scores.sort(key=lambda x: x[1], reverse=True)
            chosen, base = cand_scores[0]
            base = min(base, 0.79)  # cap because ambiguous

        if chosen in used_cols:
            continue

        # longer phrases are higher confidence than single tokens
        n_tok = len(ng.split(" "))
        length_boost = 0.06 if n_tok >= 2 else 0.0
        score = min(0.99, base + length_boost)

        matches.append(
            ProfileFilter(
                column=chosen,
                op="=",
                value=ng,
                score=score,
                evidence=f"profile_value_match:{ng}",
            )
        )
        used_cols.add(chosen)
        matched_spans.append(ng)

    debug: Dict[str, Any] = {
        "tokens": tokens,
        "matched_values": [
            {"value": m.value, "column": m.column, "score": round(m.score, 4), "evidence": m.evidence}
            for m in matches
        ],
        "unmatched_ngrams_sample": ngrams[:12],
        "value_index_size": len(val_to_cols),
    }
    return matches, debug


def compile_profile_filters_to_sql(
    filters: List[ProfileFilter],
    *,
    param_prefix: str = "pf",
) -> Tuple[List[str], Dict[str, Any], List[Dict[str, Any]], set[str]]:
    """
    Converts ProfileFilter list into SQL fragments + params compatible with routes_answer.py.
    Safe: identifiers are quoted at caller level (routes_answer already checks col_set).
    """
    frags: List[str] = []
    params: Dict[str, Any] = {}
    meta: List[Dict[str, Any]] = []
    used_cols: set[str] = set()
    idx = 0
    for f in filters:
        key = f"{param_prefix}{idx}"
        idx += 1
        params[key] = f.value
        frags.append(f"LOWER(`{f.column}`) = LOWER(:{key})")
        meta.append(
            {
                "column": f.column,
                "op": f.op,
                "value": f.value,
                "reason": "profile_value",
                "score": f.score,
                "evidence": f.evidence,
            }
        )
        used_cols.add(f.column)
    return frags, params, meta, used_cols

