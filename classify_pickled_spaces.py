#!/usr/bin/env python3
"""
Batch classify Confluence pages stored as pickles (per space) using OpenCode + Copilot models.

Outputs JSONL with:
  - page_id, title, url, space (if present)
  - model classification JSON (api_doc/dataset_doc/etc.)
  - evidence snippets
  - raw_model_text (if parsing fails)

Usage:
  python classify_pickled_spaces.py \
    --pickle_dir /path/to/pickles \
    --out results.jsonl \
    --model "<provider/model>" \
    --max_chars 12000 \
    --workers 4

Notes:
  - Requires `opencode` on PATH and authenticated to GitHub Copilot in OpenCode.
  - Recommend running `opencode serve` once and using --attach for speed.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import pickle
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple


# --------------------------
# 1) Adjust this to match your pickle schema
# --------------------------

@dataclass(frozen=True)
class Page:
    page_id: str
    title: str
    url: str
    space: str
    text: str


def iter_pages_from_pickle(obj: Any, default_space: str = "") -> Iterator[Page]:
    """
    Convert your pickled object into an iterator of Page objects.

    You MUST adapt this to your repo's pickle format.

    Common patterns this supports:
      A) obj is a dict: {"space": "ABC", "pages": [ {...}, {...} ]}
      B) obj is a list of page dicts
      C) obj is a dict of page_id -> page_dict
    """
    # Helper to normalize one page dict
    def mk_page(d: Dict[str, Any], space_fallback: str) -> Optional[Page]:
        page_id = str(d.get("id") or d.get("page_id") or d.get("content_id") or "")
        title = str(d.get("title") or "")
        url = str(d.get("url") or d.get("link") or d.get("_links", {}).get("webui") or "")
        space = str(d.get("space") or d.get("space_key") or space_fallback or default_space or "")

        # text fields commonly used in confluence exports
        text = (
            d.get("text")
            or d.get("body_text")
            or d.get("body")
            or d.get("content")
            or d.get("storage")
            or ""
        )
        if isinstance(text, dict):
            # sometimes body is nested like {"storage": {"value": "..."}}
            text = text.get("value") or text.get("storage", {}).get("value") or ""

        text = str(text or "")

        if not (page_id or title or url or text):
            return None
        if not page_id:
            # stable synthetic id based on url+title
            page_id = hashlib.sha1((url + "||" + title).encode("utf-8", "ignore")).hexdigest()[:16]

        return Page(page_id=page_id, title=title, url=url, space=space, text=text)

    # Pattern A: {"space": "...", "pages": [...]}
    if isinstance(obj, dict) and "pages" in obj and isinstance(obj["pages"], list):
        space = str(obj.get("space") or obj.get("space_key") or default_space or "")
        for d in obj["pages"]:
            if isinstance(d, dict):
                p = mk_page(d, space)
                if p:
                    yield p
        return

    # Pattern B: list of dicts
    if isinstance(obj, list):
        for d in obj:
            if isinstance(d, dict):
                p = mk_page(d, default_space)
                if p:
                    yield p
        return

    # Pattern C: dict of page_id -> dict
    if isinstance(obj, dict):
        # maybe it's already a mapping of pages
        # heuristically detect page-like dicts
        values = list(obj.values())
        if values and all(isinstance(v, dict) for v in values[:10]):
            for k, d in obj.items():
                if isinstance(d, dict):
                    d = dict(d)
                    d.setdefault("id", k)
                    p = mk_page(d, default_space)
                    if p:
                        yield p
            return

    raise ValueError(f"Unrecognized pickle structure: {type(obj)}")


# --------------------------
# 2) Prompt + text prep
# --------------------------

CLASSIFY_PROMPT = """You are classifying an internal documentation page.

Return ONLY valid JSON with this schema:
{
  "api_doc": boolean,
  "dataset_doc": boolean,
  "has_real_endpoints": boolean,
  "has_real_dataset_refs": boolean,
  "endpoints": string[],
  "dataset_refs": string[],
  "auth_methods": string[],
  "evidence_snippets": string[],
  "confidence": number
}

Rules:
- api_doc=true if it documents callable APIs (endpoints, request/response, auth, SDK usage).
- dataset_doc=true if it describes real datasets (tables/files/buckets/DB schemas) or gives schema/sample.
- has_real_endpoints=true only if concrete paths/hosts appear (e.g. /v1/foo, api.example.com).
- has_real_dataset_refs=true only if concrete dataset locations/identifiers appear (e.g. S3://..., db.schema.table).
- confidence is 0..1.
- evidence_snippets: 2-5 short quotes from the page content that justify decisions.
"""


def strip_markup(text: str) -> str:
    # Keep it simple: collapse whitespace, remove some common Confluence artefacts
    text = re.sub(r"\r\n", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    # Remove very long runs of whitespace
    text = re.sub(r"\n{4,}", "\n\n", text)
    return text.strip()


def build_llm_input(page: Page, max_chars: int) -> str:
    header = f"TITLE: {page.title}\nSPACE: {page.space}\nURL: {page.url}\nPAGE_ID: {page.page_id}\n\n"
    body = strip_markup(page.text)

    # Cheap "keep the good bits" heuristics:
    # If the page is huge, prioritize code-ish and table-ish lines.
    if len(body) > max_chars:
        lines = body.splitlines()
        keep: List[str] = []
        for ln in lines:
            l = ln.strip()
            if not l:
                continue
            if (
                "http" in l
                or l.startswith(("GET ", "POST ", "PUT ", "DELETE "))
                or "/v" in l
                or "curl " in l
                or "SELECT " in l.upper()
                or "CREATE TABLE" in l.upper()
                or "s3://" in l.lower()
                or "gs://" in l.lower()
                or "oauth" in l.lower()
                or "bearer" in l.lower()
                or "x-api-key" in l.lower()
            ):
                keep.append(ln)
            if len("\n".join(keep)) > max_chars:
                break
        if len("\n".join(keep)) < max_chars // 3:
            # fallback: take the first chunk
            body = body[:max_chars]
        else:
            body = "\n".join(keep)[:max_chars]
    else:
        body = body[:max_chars]

    return header + body


# --------------------------
# 3) OpenCode invocation
# --------------------------

def run_opencode(
    model: str,
    prompt: str,
    content: str,
    attach: Optional[str] = None,
    timeout_s: int = 120,
) -> str:
    """
    Calls: opencode run ... and returns stdout.
    """
    cmd = ["opencode", "run", prompt, "--format", "json"]
    if model:
        cmd += ["--model", model]
    if attach:
        cmd += ["--attach", attach]

    # Provide content via stdin to avoid temp files
    # (OpenCode accepts stdin as message content in many setups; if yours needs --file, swap this.)
    p = subprocess.run(
        cmd,
        input=content.encode("utf-8", "ignore"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout_s,
    )
    if p.returncode != 0:
        raise RuntimeError(
            f"opencode failed rc={p.returncode}\nSTDERR:\n{p.stderr.decode('utf-8', 'ignore')}"
        )
    return p.stdout.decode("utf-8", "ignore")


def extract_final_json(opencode_stdout: str) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    OpenCode --format json often streams events (JSON objects per line).
    We attempt to find the last assistant message text and parse it as JSON.
    Returns (parsed_json_or_none, raw_text).
    """
    last_text = ""
    for line in opencode_stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            evt = json.loads(line)
        except Exception:
            continue

        # Heuristic: different versions emit different shapes.
        # Look for any text payload fields.
        for path in [
            ("message", "content"),
            ("content",),
            ("data", "content"),
            ("delta", "content"),
            ("output",),
            ("text",),
        ]:
            cur = evt
            ok = True
            for k in path:
                if isinstance(cur, dict) and k in cur:
                    cur = cur[k]
                else:
                    ok = False
                    break
            if ok and isinstance(cur, str) and cur.strip():
                last_text = cur.strip()

    raw = last_text or opencode_stdout.strip()

    # Try to parse JSON from raw text
    try:
        return json.loads(raw), raw
    except Exception:
        # Maybe the model wrapped it; try to extract first JSON object
        m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0)), raw
            except Exception:
                pass
        return None, raw


# --------------------------
# 4) Batch orchestration
# --------------------------

def load_pickle(path: Path) -> Any:
    with path.open("rb") as f:
        return pickle.load(f)


def classify_page(page: Page, model: str, attach: Optional[str], max_chars: int) -> Dict[str, Any]:
    llm_input = build_llm_input(page, max_chars=max_chars)
    out = run_opencode(model=model, prompt=CLASSIFY_PROMPT, content=llm_input, attach=attach)
    parsed, raw = extract_final_json(out)

    result: Dict[str, Any] = {
        "page_id": page.page_id,
        "title": page.title,
        "url": page.url,
        "space": page.space,
        "ts_utc": time.time(),
        "model": model,
    }
    if parsed is not None:
        result["classification"] = parsed
    else:
        result["classification"] = None
        result["raw_model_text"] = raw[:20000]
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pickle_dir", required=True, help="Directory containing per-space pickle files")
    ap.add_argument("--out", required=True, help="Output JSONL path")
    ap.add_argument("--model", default="", help="OpenCode model name, e.g. 'copilot/<something>'")
    ap.add_argument("--attach", default=None, help="If using opencode serve: http://localhost:4096")
    ap.add_argument("--max_chars", type=int, default=12000, help="Max chars of page content sent to model")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--limit_pages", type=int, default=0, help="0 = no limit")
    args = ap.parse_args()

    pickle_dir = Path(args.pickle_dir)
    out_path = Path(args.out)

    pickle_files = sorted([p for p in pickle_dir.glob("*.pkl")] + [p for p in pickle_dir.glob("*.pickle")])
    if not pickle_files:
        print(f"No pickle files found in {pickle_dir}", file=sys.stderr)
        return 2

    # Load all pages
    pages: List[Page] = []
    for pf in pickle_files:
        try:
            obj = load_pickle(pf)
        except Exception as e:
            print(f"[WARN] Failed to load {pf.name}: {e}", file=sys.stderr)
            continue

        default_space = pf.stem
        try:
            for page in iter_pages_from_pickle(obj, default_space=default_space):
                pages.append(page)
        except Exception as e:
            print(f"[WARN] Unrecognized structure in {pf.name}: {e}", file=sys.stderr)
            continue

    if args.limit_pages and len(pages) > args.limit_pages:
        pages = pages[: args.limit_pages]

    print(f"Loaded {len(pages)} pages from {len(pickle_files)} pickle files.")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Write JSONL incrementally
    with out_path.open("a", encoding="utf-8") as out_f:
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(classify_page, p, args.model, args.attach, args.max_chars) for p in pages]
            for i, fut in enumerate(cf.as_completed(futs), 1):
                try:
                    rec = fut.result()
                    out_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    out_f.flush()
                except Exception as e:
                    print(f"[ERR] classification failed: {e}", file=sys.stderr)

                if i % 100 == 0:
                    print(f"Processed {i}/{len(pages)}")

    print(f"Done. Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
