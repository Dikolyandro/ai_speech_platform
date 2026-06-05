#!/usr/bin/env python3
"""
Manual evaluation: POST /query/semantic-debug with fixed multilingual queries.

=== Start backend ===
From the backend project root:
  uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

=== Login / get JWT ===
Use your client or curl against the auth endpoint (e.g. POST /api/v1/auth/login
with form body username + password). Copy ``access_token`` from the JSON response.

=== Run (PowerShell) ===
  $env:API_TOKEN="eyJhbGciOi..."
  python scripts/test_semantic_debug.py --dataset-id 1

=== Run (bash) ===
  export API_TOKEN="eyJhbGciOi..."
  python scripts/test_semantic_debug.py --base-url http://localhost:8000/api/v1 --dataset-id 1

Requires: ``requests`` (``pip install requests``). Token is read from env ``API_TOKEN``.

``API_TOKEN`` must be a real JWT (ASCII). HTTP headers are Latin-1 only — a Cyrillic
placeholder like ``твой_token`` will crash ``requests``; paste the ``access_token`` from login.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any


def _try_import_requests():
    try:
        import requests  # type: ignore

        return requests
    except ImportError:
        print(
            "Missing dependency: requests\n"
            "Install with:  pip install requests\n"
            "Then re-run this script.",
            file=sys.stderr,
        )
        sys.exit(1)


def _fmt_semantic_match(obj: Any) -> str:
    if not obj or not isinstance(obj, dict):
        return "(none)"
    col = obj.get("column")
    score = obj.get("score")
    ev = obj.get("evidence") or ""
    tail = f" score={score}" if score is not None else ""
    if ev:
        tail += f" evidence={ev!r}"
    return f"{col}{tail}"


def _heuristic_verdict(payload: dict[str, Any]) -> str:
    """No ground truth — only signals for manual review."""
    leg = payload.get("legacy_semantics") or {}
    sl_err = payload.get("semantic_layer_error")
    sl = payload.get("semantic_layer_v1")
    cmp_ = payload.get("comparison") or {}

    if leg.get("error"):
        return "legacy resolver raised an error (see legacy_semantics.error)"
    if sl_err:
        return "semantic layer failed (see semantic_layer_error); cannot compare fairly"
    if sl is None:
        return "no semantic_layer_v1 in response"
    if cmp_.get("metric_same") and cmp_.get("group_same"):
        return "both paths agree on metric and group (tie)"

    bits: list[str] = []
    if not cmp_.get("metric_same"):
        bits.append(
            f"metric: legacy={leg.get('resolved_metric')!r} vs semantic={_fmt_semantic_match(sl.get('detected_metric'))}"
        )
    if not cmp_.get("group_same"):
        bits.append(
            f"group: legacy={leg.get('resolved_group_by')!r} vs semantic={_fmt_semantic_match(sl.get('detected_dimension'))}"
        )
    if bits:
        return "disagreement — " + "; ".join(bits) + " → pick winner manually"
    return "see printed fields (unexpected comparison state)"


TEST_QUERIES: list[tuple[str, str]] = [
    ("en", "show top revenue by category"),
    ("en", "which products make the most money"),
    ("en", "average payment by month"),
    ("en", "count orders by status"),
    ("en", "show customers with highest spending"),
    ("ru", "покажи продажи по категориям"),
    ("ru", "какие товары приносят больше всего денег"),
    ("ru", "средний платеж по месяцам"),
    ("ru", "посчитай заказы по статусу"),
    ("ru", "покажи клиентов с самыми большими покупками"),
    ("kk", "санат бойынша табысты көрсет"),
    ("kk", "ең көп ақша әкелетін өнімдер"),
    ("kk", "ай бойынша орташа төлем"),
    ("kk", "мәртебе бойынша тапсырыстарды сана"),
    ("kk", "ең көп сатып алған клиенттерді көрсет"),
]


def main() -> None:
    requests = _try_import_requests()

    p = argparse.ArgumentParser(description="Call semantic-debug for fixed multilingual queries.")
    p.add_argument(
        "--base-url",
        default="http://localhost:8000/api/v1",
        help="API prefix including /api/v1 (default: %(default)s)",
    )
    p.add_argument("--dataset-id", type=int, default=1, help="Dataset id (default: %(default)s)")
    args = p.parse_args()

    token = os.environ.get("API_TOKEN", "").strip()
    if not token:
        print("Set environment variable API_TOKEN to your JWT (Bearer token).", file=sys.stderr)
        sys.exit(1)
    try:
        token.encode("iso-8859-1")
    except UnicodeEncodeError:
        print(
            "API_TOKEN must be encodable in HTTP headers (Latin-1). "
            "Use the real access_token from POST /api/v1/auth/login - it is ASCII.\n"
            "Cyrillic or other Unicode in the token value will fail before the request is sent.",
            file=sys.stderr,
        )
        sys.exit(1)

    base = args.base_url.rstrip("/")
    url = f"{base}/query/semantic-debug"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    print(f"POST {url}")
    print(f"dataset_id={args.dataset_id}")
    print("=" * 72)

    for lang, text in TEST_QUERIES:
        body = {"dataset_id": args.dataset_id, "text": text}
        try:
            r = requests.post(url, headers=headers, json=body, timeout=60)
        except UnicodeEncodeError as e:
            print(f"\n[{lang}] query: {text!r}")
            print(f"  REQUEST ERROR (header encoding): {e}")
            continue
        except requests.RequestException as e:
            print(f"\n[{lang}] query: {text!r}")
            print(f"  REQUEST ERROR: {e}")
            continue

        print(f"\n[{lang}] query: {text!r}")
        print(f"  HTTP {r.status_code}")

        if r.status_code != 200:
            print(f"  body: {r.text[:500]!r}")
            continue

        try:
            data: dict[str, Any] = r.json()
        except json.JSONDecodeError:
            print(f"  INVALID JSON: {r.text[:300]!r}")
            continue

        leg = data.get("legacy_semantics") or {}
        sl = data.get("semantic_layer_v1")
        cmp_ = data.get("comparison") or {}

        print(f"  normalized query: {data.get('query')!r}")
        print(f"  legacy resolved_metric: {leg.get('resolved_metric')!r}")
        print(f"  legacy resolved_group_by: {leg.get('resolved_group_by')!r}")
        if leg.get("error"):
            print(f"  legacy error: {leg.get('error')!r}")

        if sl:
            print(f"  semantic detected_metric: {_fmt_semantic_match(sl.get('detected_metric'))}")
            print(f"  semantic detected_dimension: {_fmt_semantic_match(sl.get('detected_dimension'))}")
            print(f"  semantic detected_date_column: {_fmt_semantic_match(sl.get('detected_date_column'))}")
            conf = sl.get("confidence")
            print(f"  semantic confidence: {conf!r}")
        else:
            print("  semantic detected_metric: (no semantic_layer_v1)")
            print("  semantic detected_dimension: (no semantic_layer_v1)")
            print("  semantic detected_date_column: (no semantic_layer_v1)")
            print("  semantic confidence: (n/a)")

        if data.get("semantic_layer_error"):
            print(f"  semantic_layer_error: {data['semantic_layer_error']!r}")

        print(
            "  comparison: "
            f"metric_same={cmp_.get('metric_same')} group_same={cmp_.get('group_same')} "
            f"better_metric_candidate={cmp_.get('semantic_metric_better_candidate')!r} "
            f"better_group_candidate={cmp_.get('semantic_group_better_candidate')!r}"
        )
        print(f"  verdict (heuristic): {_heuristic_verdict(data)}")

    print("\n" + "=" * 72)
    print("Done.")


if __name__ == "__main__":
    main()
