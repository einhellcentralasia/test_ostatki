#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Read-only SharePoint folder sync:
- Lists all .xlsx files in a SharePoint folder (optionally recursive)
- For each file, loads Excel Table (SP_TABLE_NAME, default "Table1") fully in memory
- Extracts only SKU and Qty columns (robust header matching)
- Aggregates Qty by SKU across all files
- Saves outputs to repo ./data/:
    - sold_to_clients.parquet
    - sold_to_clients.csv
    - sold_to_clients.json
- Prints runtime + processing stats

Safety (poka-yoke):
- NO write calls to SharePoint (only GET)
- Skips temp files like "~$..."
- Handles missing table / missing columns per-file (logs + continues)
- Handles Graph pagination and throttling retries
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from openpyxl import load_workbook


# -------------------------
# Error handling at the top
# -------------------------

def die(msg: str, code: int = 2) -> None:
    print(f"❌ {msg}", file=sys.stderr)
    raise SystemExit(code)

def env(name: str, default: Optional[str] = None, required: bool = True) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        die(f"Missing required env var: {name}")
    return str(val)

def now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


# -------------------------
# Microsoft Graph helpers
# -------------------------

GRAPH = "https://graph.microsoft.com/v1.0"

@dataclass
class GraphCtx:
    token: str
    session: requests.Session

def new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    return s

def request_json(ctx: GraphCtx, method: str, url: str, *, params=None, stream=False, expected=(200,)) -> dict:
    """JSON request with retry/backoff for 429/5xx."""
    max_tries = 8
    backoff = 1.0

    headers = {"Authorization": f"Bearer {ctx.token}"}
    for attempt in range(1, max_tries + 1):
        resp = ctx.session.request(method, url, headers=headers, params=params, stream=stream, timeout=60)

        if resp.status_code in expected:
            return resp.json()

        if resp.status_code in (429, 500, 502, 503, 504):
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    sleep_s = float(retry_after)
                except ValueError:
                    sleep_s = backoff
            else:
                sleep_s = backoff

            print(f"⚠️ {now_ts()} Graph {resp.status_code} on {url} (attempt {attempt}/{max_tries}), sleep {sleep_s:.1f}s")
            time.sleep(sleep_s)
            backoff = min(backoff * 1.8, 20.0)
            continue

        # Non-retryable
        try:
            body = resp.text[:2000]
        except Exception:
            body = "<unreadable>"
        die(f"Graph error {resp.status_code} on {url}\nResponse: {body}")

    die(f"Graph failed after retries on {url}")
    return {}

def request_bytes(ctx: GraphCtx, url: str) -> bytes:
    """Download bytes (GET) with retry/backoff."""
    max_tries = 8
    backoff = 1.0
    headers = {"Authorization": f"Bearer {ctx.token}"}

    for attempt in range(1, max_tries + 1):
        resp = ctx.session.get(url, headers=headers, stream=True, timeout=120)
        if resp.status_code == 200:
            return resp.content

        if resp.status_code in (429, 500, 502, 503, 504):
            retry_after = resp.headers.get("Retry-After")
            sleep_s = float(retry_after) if retry_after and retry_after.isdigit() else backoff
            print(f"⚠️ {now_ts()} Download {resp.status_code} (attempt {attempt}/{max_tries}), sleep {sleep_s:.1f}s")
            time.sleep(sleep_s)
            backoff = min(backoff * 1.8, 20.0)
            continue

        die(f"Download error {resp.status_code} for {url}: {resp.text[:1000]}")

    die(f"Download failed after retries: {url}")
    return b""

def get_app_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "https://graph.microsoft.com/.default",
    }
    resp = requests.post(token_url, data=data, timeout=60)
    if resp.status_code != 200:
        die(f"Token request failed {resp.status_code}: {resp.text[:2000]}")
    js = resp.json()
    tok = js.get("access_token")
    if not tok:
        die("Token response missing access_token")
    return tok

def graph_get_site_id(ctx: GraphCtx, hostname: str, site_path: str) -> str:
    # GET /sites/{hostname}:{site-path}
    url = f"{GRAPH}/sites/{hostname}:{site_path}"
    js = request_json(ctx, "GET", url, expected=(200,))
    site_id = js.get("id")
    if not site_id:
        die("Could not resolve site id (missing id in response). Check SP_SITE_HOSTNAME/SP_SITE_PATH.")
    return site_id

def graph_get_drive_id(ctx: GraphCtx, site_id: str) -> str:
    url = f"{GRAPH}/sites/{site_id}/drive"
    js = request_json(ctx, "GET", url, expected=(200,))
    drive_id = js.get("id")
    if not drive_id:
        die("Could not resolve drive id for the site.")
    return drive_id

def graph_get_item_id_by_path(ctx: GraphCtx, drive_id: str, path: str) -> str:
    # /drives/{drive-id}/root:/{path}
    path_clean = path.strip("/")
    url = f"{GRAPH}/drives/{drive_id}/root:/{path_clean}"
    js = request_json(ctx, "GET", url, expected=(200,))
    item_id = js.get("id")
    if not item_id:
        die("Could not resolve folder item id. Check SP_XLSX_PATH.")
    return item_id

def graph_list_children(ctx: GraphCtx, drive_id: str, folder_item_id: str) -> Iterable[dict]:
    url = f"{GRAPH}/drives/{drive_id}/items/{folder_item_id}/children"
    params = {"$top": "200"}
    while True:
        js = request_json(ctx, "GET", url, params=params, expected=(200,))
        for it in js.get("value", []):
            yield it
        next_link = js.get("@odata.nextLink")
        if not next_link:
            break
        url = next_link
        params = None  # nextLink already includes params

def graph_walk_files(ctx: GraphCtx, drive_id: str, folder_item_id: str, recursive: bool) -> List[dict]:
    out = []
    stack = [folder_item_id]
    while stack:
        fid = stack.pop()
        for it in graph_list_children(ctx, drive_id, fid):
            name = it.get("name", "")
            # Skip temp/hidden-ish Office files
            if name.startswith("~$"):
                continue

            is_folder = "folder" in it
            is_file = "file" in it

            if is_folder and recursive:
                child_id = it.get("id")
                if child_id:
                    stack.append(child_id)
                continue

            if is_file:
                out.append(it)
    return out


# -------------------------
# Excel parsing helpers
# -------------------------

def norm(s: str) -> str:
    return "".join(str(s).strip().lower().split())

SKU_HEADERS = {"sku", "артикул", "арт", "item", "code"}
QTY_HEADERS = {"qty", "qt", "quantity", "кол-во", "количество", "count", "pcs"}

def find_col_indices(headers: List[str]) -> Tuple[Optional[int], Optional[int]]:
    sku_idx = None
    qty_idx = None
    for i, h in enumerate(headers):
        h2 = norm(h)
        if sku_idx is None and h2 in SKU_HEADERS:
            sku_idx = i
        if qty_idx is None and h2 in QTY_HEADERS:
            qty_idx = i
    return sku_idx, qty_idx

def read_table_from_xlsx_bytes(xlsx_bytes: bytes, table_name: str) -> Optional[pd.DataFrame]:
    wb = load_workbook(filename=io.BytesIO(xlsx_bytes), data_only=True, read_only=True)
    # Search all sheets for the table name
    for ws in wb.worksheets:
        tables = getattr(ws, "tables", None)
        if not tables:
            continue
        if table_name in tables:
            tbl = tables[table_name]
            ref = tbl.ref  # e.g., "A1:G10"
            cells = ws[ref]
            rows = [[c.value for c in row] for row in cells]
            if not rows or len(rows) < 2:
                return pd.DataFrame(columns=["SKU", "Qty"])
            headers = [str(x) if x is not None else "" for x in rows[0]]
            sku_idx, qty_idx = find_col_indices(headers)
            if sku_idx is None or qty_idx is None:
                # If the table exists but headers don't match, still fail for this file.
                return None
            data_rows = rows[1:]
            skus = []
            qtys = []
            for r in data_rows:
                # Guard against short rows
                if sku_idx >= len(r) or qty_idx >= len(r):
                    continue
                sku = r[sku_idx]
                qty = r[qty_idx]
                if sku is None or str(sku).strip() == "":
                    continue
                # Coerce qty to number
                try:
                    q = float(qty) if qty is not None and str(qty).strip() != "" else 0.0
                except Exception:
                    q = 0.0
                skus.append(str(sku).strip())
                qtys.append(q)

            df = pd.DataFrame({"SKU": skus, "Qty": qtys})
            return df

    return None


# -------------------------
# Main
# -------------------------

def main() -> None:
    t0 = time.perf_counter()

    tenant_id = env("TENANT_ID")
    client_id = env("CLIENT_ID")
    client_secret = env("CLIENT_SECRET")

    sp_site_hostname = env("SP_SITE_HOSTNAME")
    sp_site_path = env("SP_SITE_PATH")
    sp_xlsx_path = env("SP_XLSX_PATH")
    sp_table_name = env("SP_TABLE_NAME", default="Table1", required=False)

    recursive = env("SP_RECURSIVE", default="false", required=False).strip().lower() in ("1", "true", "yes", "y")

    # Output config
    out_dir = "data"
    base_name = "sold_to_clients"

    print(f"🟢 {now_ts()} Start. Site={sp_site_hostname}{sp_site_path} Folder='{sp_xlsx_path}' Table='{sp_table_name}' Recursive={recursive}")

    token = get_app_token(tenant_id, client_id, client_secret)
    ctx = GraphCtx(token=token, session=new_session())

    site_id = graph_get_site_id(ctx, sp_site_hostname, sp_site_path)
    drive_id = graph_get_drive_id(ctx, site_id)
    folder_item_id = graph_get_item_id_by_path(ctx, drive_id, sp_xlsx_path)

    items = graph_walk_files(ctx, drive_id, folder_item_id, recursive=recursive)

    # Filter xlsx files only
    xlsx_items = [it for it in items if it.get("name", "").lower().endswith(".xlsx")]
    print(f"📦 Found files: total={len(items)}, xlsx={len(xlsx_items)}")

    agg: Dict[str, float] = {}
    processed = 0
    skipped = 0
    skipped_no_table = 0
    skipped_no_cols = 0

    for it in xlsx_items:
        name = it.get("name", "")
        item_id = it.get("id")
        if not item_id:
            skipped += 1
            continue

        # Download in memory
        content_url = f"{GRAPH}/drives/{drive_id}/items/{item_id}/content"
        try:
            b = request_bytes(ctx, content_url)
        except Exception as e:
            print(f"⚠️ Skip '{name}': download failed: {e}")
            skipped += 1
            continue

        # Parse table
        df = None
        try:
            df = read_table_from_xlsx_bytes(b, sp_table_name)
        except Exception as e:
            print(f"⚠️ Skip '{name}': parse failed: {e}")
            skipped += 1
            continue

        if df is None:
            # Could be missing table or header mismatch
            # We can quickly distinguish: if table exists but cols mismatch, df returns None anyway.
            # We'll log a generic warning.
            print(f"⚠️ Skip '{name}': table '{sp_table_name}' not found or required columns missing")
            skipped_no_table += 1
            continue

        if df.empty:
            processed += 1
            continue

        # Aggregate
        for sku, qty in zip(df["SKU"].tolist(), df["Qty"].tolist()):
            agg[sku] = agg.get(sku, 0.0) + float(qty)

        processed += 1

    # Build result dataframe
    result = pd.DataFrame(
        [{"SKU": sku, "Qty": int(round(qty)) if float(qty).is_integer() else float(qty)} for sku, qty in agg.items()]
    ).sort_values(["SKU"], kind="stable")

    os.makedirs(out_dir, exist_ok=True)

    parquet_path = os.path.join(out_dir, f"{base_name}.parquet")
    csv_path = os.path.join(out_dir, f"{base_name}.csv")
    json_path = os.path.join(out_dir, f"{base_name}.json")

    # Save outputs
    result.to_parquet(parquet_path, index=False)
    result.to_csv(csv_path, index=False, encoding="utf-8")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(result.to_dict(orient="records"), f, ensure_ascii=False, indent=2)

    t1 = time.perf_counter()
    dur = t1 - t0

    print("✅ Done.")
    print(f"🧾 Processed xlsx: {processed} | Skipped: {skipped + skipped_no_table + skipped_no_cols}")
    print(f"🧮 Unique SKUs: {len(result)}")
    print(f"⏱️ Runtime: {dur:.2f} seconds")
    print(f"📁 Outputs: {parquet_path}, {csv_path}, {json_path}")

if __name__ == "__main__":
    main()
