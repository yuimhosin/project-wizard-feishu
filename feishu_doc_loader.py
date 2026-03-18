# -*- coding: utf-8 -*-
"""飞书文档加载与清洗：支持 wiki->bitable / wiki->sheet。"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.parse
import urllib.request
import urllib.error
from typing import Any, Optional
from pathlib import Path

try:
    import tomllib  # py3.11+
except Exception:  # pragma: no cover
    tomllib = None

import pandas as pd

from data_loader import (
    PARK_TOKENS,
    TIMELINE_COLS,
    _is_progress_sheet,
    _normalize_loaded_df,
    _parse_header_from_rows,
)


FEISHU_API_BASE = "https://open.feishu.cn/open-apis"
_token_cache = {"token": None, "expires_at": 0.0}
_LAST_ERROR = ""


def _set_last_error(msg: str):
    global _LAST_ERROR
    _LAST_ERROR = str(msg or "").strip()


def get_last_error() -> str:
    return _LAST_ERROR


def _get_tenant_access_token() -> Optional[str]:
    """获取 tenant_access_token（缓存）。"""
    _set_last_error("")
    app_id = os.getenv("FEISHU_APP_ID", "")
    app_secret = os.getenv("FEISHU_APP_SECRET", "")
    if not app_id or not app_secret:
        # 避免直接访问 st.secrets 触发 "No secrets files found" 报错
        if tomllib is not None:
            candidate_files = [
                Path.home() / ".streamlit" / "secrets.toml",
                Path(__file__).resolve().parent / ".streamlit" / "secrets.toml",
            ]
            for fp in candidate_files:
                if not fp.exists():
                    continue
                try:
                    data = tomllib.loads(fp.read_text(encoding="utf-8"))
                    app_id = str(data.get("FEISHU_APP_ID", "")).strip()
                    app_secret = str(data.get("FEISHU_APP_SECRET", "")).strip()
                    if app_id and app_secret:
                        break
                except Exception:
                    continue
    if not app_id or not app_secret:
        _set_last_error("未配置 FEISHU_APP_ID/FEISHU_APP_SECRET。")
        return None

    now = time.time()
    if _token_cache["token"] and _token_cache["expires_at"] > now + 300:
        return _token_cache["token"]

    url = f"{FEISHU_API_BASE}/auth/v3/tenant_access_token/internal"
    body = json.dumps({"app_id": app_id, "app_secret": app_secret}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        if data.get("code") == 0:
            token = data.get("tenant_access_token")
            expire = int(data.get("expire", 7200))
            _token_cache["token"] = token
            _token_cache["expires_at"] = now + expire
            return token
    except Exception:
        _set_last_error("获取 tenant_access_token 失败，请检查飞书应用凭据。")
        return None
    return None


def _api_get(url: str, token: str) -> dict:
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode(errors="ignore")
            data = json.loads(body) if body else {}
        except Exception:
            data = {}
        if isinstance(data, dict):
            data.setdefault("code", -1)
            data.setdefault("msg", f"HTTP {e.code}")
            data["http_status"] = e.code
            return data
        return {"code": -1, "msg": f"HTTP {e.code}", "http_status": e.code, "raw": body}
    except Exception as e:
        return {"code": -1, "msg": f"请求失败：{e}"}


def _parse_url_tokens(url_or_id: str) -> dict:
    """解析 URL 中 token。"""
    s = (url_or_id or "").strip()
    out = {
        "raw": s,
        "wiki_token": "",
        "sheet_param": "",
        "table_param": "",
        "view_param": "",
        "base_token": "",
    }
    if not s:
        return out
    m_wiki = re.search(r"/wiki/([A-Za-z0-9]+)", s)
    if m_wiki:
        out["wiki_token"] = m_wiki.group(1)
    m_base = re.search(r"/base/([A-Za-z0-9]+)", s)
    if m_base:
        out["base_token"] = m_base.group(1)
    m_sheet = re.search(r"[?&]sheet=([A-Za-z0-9]+)", s)
    if m_sheet:
        out["sheet_param"] = m_sheet.group(1)
    m_table = re.search(r"[?&]table=([A-Za-z0-9]+)", s)
    if m_table:
        out["table_param"] = m_table.group(1)
    m_view = re.search(r"[?&]view=([A-Za-z0-9]+)", s)
    if m_view:
        out["view_param"] = m_view.group(1)
    return out


def _get_wiki_node_info(node_token: str, token: str) -> dict:
    url = f"{FEISHU_API_BASE}/wiki/v2/spaces/get_node?token={node_token}"
    data = _api_get(url, token)
    if data.get("code") != 0:
        _set_last_error(f"读取 Wiki 节点失败：{data.get('msg', '未知错误')}（code={data.get('code')}）")
        return {}
    return data.get("data", {}).get("node", {}) or {}


def _flatten_field_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (int, float, bool)):
        return str(v)
    if isinstance(v, list):
        parts = []
        for item in v:
            if isinstance(item, dict):
                parts.append(item.get("name") or item.get("text") or item.get("title") or str(item))
            else:
                parts.append(str(item))
        return "; ".join([x for x in parts if x]).strip()
    if isinstance(v, dict):
        return str(v.get("text") or v.get("name") or v.get("title") or v)
    return str(v)


def _merge_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """合并重名列，优先保留非空值。"""
    if df.empty or df.columns.is_unique:
        return df
    out = pd.DataFrame(index=df.index)
    for col in dict.fromkeys(df.columns):
        same = df.loc[:, [c == col for c in df.columns]]
        if same.shape[1] == 1:
            out[col] = same.iloc[:, 0]
        else:
            merged = same.iloc[:, 0].copy()
            for j in range(1, same.shape[1]):
                merged = merged.where(merged.astype(str).str.strip() != "", same.iloc[:, j])
            out[col] = merged
    return out


def _clean_to_project_schema(df: pd.DataFrame, source_name: str = "") -> pd.DataFrame:
    """将飞书拉取数据清洗为项目可用格式。"""
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out.columns = [str(c).strip().strip("\ufeff") for c in out.columns]

    rename_map = {
        "编号": "序号",
        "社区": "园区",
        "所属社区": "园区",
        "所在城市": "城市",
        "拟定承建组": "拟定承建组织",
        "专业细分": "专业分包",
        "验收(社区需求完成交付)": "验收",
        "验收(社区结算)": "验收",
    }
    out = out.rename(columns=rename_map)

    # 兼容列名前缀（如“预计节点（月份）.需求立项”）
    fuzzy_rename = {}
    for col in out.columns:
        s = str(col).strip()
        if s in TIMELINE_COLS:
            continue
        if "验收" in s and "验收" not in fuzzy_rename:
            fuzzy_rename[col] = "验收"
            continue
        for tcol in TIMELINE_COLS:
            if tcol in s:
                fuzzy_rename[col] = tcol
                break
    if fuzzy_rename:
        out = out.rename(columns=fuzzy_rename)
        out = _merge_duplicate_columns(out)

    # 日期列统一 YYYY-MM-DD（保留空值）
    for c in TIMELINE_COLS:
        if c in out.columns:
            dt = pd.to_datetime(out[c], errors="coerce", format="mixed")
            out[c] = dt.dt.strftime("%Y-%m-%d").fillna("")

    out = _normalize_loaded_df(out, 园区名=None, default_园区_from=source_name)
    return out


def _fetch_bitable_records(app_token: str, table_id: str, token: str) -> list[dict]:
    all_records: list[dict] = []
    page_token = ""
    while True:
        params = {"page_size": "500"}
        if page_token:
            params["page_token"] = page_token
        q = urllib.parse.urlencode(params)
        url = f"{FEISHU_API_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records?{q}"
        data = _api_get(url, token)
        if data.get("code") != 0:
            break
        d = data.get("data", {}) or {}
        items = d.get("items", []) or []
        for rec in items:
            fields = rec.get("fields", {}) or {}
            flat = {k: _flatten_field_value(v) for k, v in fields.items()}
            all_records.append(flat)
        page_token = d.get("page_token") or ""
        if not d.get("has_more") or not page_token:
            break
    return all_records


def _load_from_bitable(app_token: str, table_id: str, token: str) -> pd.DataFrame:
    url = f"{FEISHU_API_BASE}/bitable/v1/apps/{app_token}/tables"
    data = _api_get(url, token)
    if data.get("code") != 0:
        _set_last_error(f"读取多维表格失败：{data.get('msg', '未知错误')}（code={data.get('code')}）")
        return pd.DataFrame()
    tables = data.get("data", {}).get("items", []) or []
    if table_id:
        tables = [t for t in tables if t.get("table_id") == table_id]
    frames = []
    for t in tables:
        tid = t.get("table_id", "")
        tname = t.get("name", "")
        if not tid:
            continue
        records = _fetch_bitable_records(app_token, tid, token)
        if not records:
            continue
        df = pd.DataFrame(records)
        df = _clean_to_project_schema(df, source_name=tname)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _pick_sheet_token(node: dict, parsed: dict) -> str:
    # URL 优先
    if parsed.get("sheet_param"):
        return parsed["sheet_param"]
    # 节点 token 也可能就是目标 sheet
    if parsed.get("wiki_token"):
        return parsed["wiki_token"]
    return ""


def _fetch_sheet_values(spreadsheet_token: str, sheet_title: str, token: str) -> list[list[Any]]:
    """优先用 values_batch_get；失败再尝试 values 接口。"""
    rng = f"{sheet_title}!A1:AZ3000"
    ranges = urllib.parse.quote(rng, safe="")

    # v2 batch_get
    url1 = f"{FEISHU_API_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_get?ranges={ranges}"
    try:
        data1 = _api_get(url1, token)
        if data1.get("code") == 0:
            vrs = data1.get("data", {}).get("valueRanges", []) or []
            if vrs:
                return vrs[0].get("values", []) or []
    except Exception:
        pass

    # v2 values
    url2 = f"{FEISHU_API_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values/{ranges}"
    try:
        data2 = _api_get(url2, token)
        if data2.get("code") == 0:
            vr = data2.get("data", {}).get("valueRange", {}) or {}
            return vr.get("values", []) or []
    except Exception:
        pass
    return []


def _sheet_rows_to_df(rows: list[list[Any]], sheet_name: str) -> pd.DataFrame:
    if not rows or len(rows) < 2:
        return pd.DataFrame()
    row0 = rows[0]
    row1 = rows[1] if len(rows) >= 2 else []
    names = _parse_header_from_rows(row0, row1, n_time=len(TIMELINE_COLS))
    data_start = 2
    # 兼容单行表头
    if not _is_progress_sheet(names):
        first_names = [str(x).strip() for x in row0]
        if _is_progress_sheet(first_names):
            names = first_names
            data_start = 1
        else:
            return pd.DataFrame()
    data_rows = rows[data_start:]
    if not data_rows:
        return pd.DataFrame()
    width = len(names)
    normalized_rows = []
    for r in data_rows:
        rr = list(r[:width])
        if len(rr) < width:
            rr.extend([""] * (width - len(rr)))
        normalized_rows.append(rr)
    df = pd.DataFrame(normalized_rows, columns=names)
    return _clean_to_project_schema(df, source_name=sheet_name)


def _load_from_sheet(spreadsheet_token: str, preferred_sheet_token: str, token: str) -> pd.DataFrame:
    url = f"{FEISHU_API_BASE}/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
    data = _api_get(url, token)
    if data.get("code") != 0:
        _set_last_error(f"读取电子表格失败：{data.get('msg', '未知错误')}（code={data.get('code')}）")
        return pd.DataFrame()

    sheets = data.get("data", {}).get("sheets", []) or []
    if preferred_sheet_token:
        selected = [s for s in sheets if s.get("sheet_id") == preferred_sheet_token]
        sheets = selected or sheets

    frames = []
    for s in sheets:
        title = s.get("title", "")
        if not title:
            continue
        rows = _fetch_sheet_values(spreadsheet_token, title, token)
        df = _sheet_rows_to_df(rows, title)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _extract_park_name(text: str) -> str:
    s = str(text or "")
    for token in PARK_TOKENS:
        if token in s:
            return token
    return ""


def load_from_feishu_doc(url_or_id: str) -> pd.DataFrame:
    """
    从飞书文档链接加载项目数据并清洗为项目向导可用格式。
    支持：
    - wiki->bitable（多数据表合并）
    - wiki->sheet（多 sheet 合并）
    """
    token = _get_tenant_access_token()
    if not token:
        return pd.DataFrame()

    parsed = _parse_url_tokens(url_or_id)
    wiki_token = parsed.get("wiki_token", "")
    sheet_param = parsed.get("sheet_param", "")
    table_param = parsed.get("table_param", "")
    base_token = parsed.get("base_token", "")

    if base_token:
        # 直接 bitable 链接
        return _load_from_bitable(base_token, table_param, token)

    if not wiki_token:
        _set_last_error("未识别到有效的飞书链接 token。")
        return pd.DataFrame()

    # 优先兼容：wiki 链接里带 sheet 参数时，直接按 sheet API 读取
    # 这样可绕开 wiki get_node 的权限要求（当链接本身可直接访问电子表格时）。
    if sheet_param:
        direct_df = _load_from_sheet(wiki_token, sheet_param, token)
        if not direct_df.empty:
            return direct_df

    node = _get_wiki_node_info(wiki_token, token)
    obj_type = str(node.get("obj_type", "")).strip().lower()
    obj_token = str(node.get("obj_token", "")).strip()
    title = str(node.get("title", "")).strip()

    if obj_type == "bitable" and obj_token:
        return _load_from_bitable(obj_token, table_param, token)

    if obj_type == "sheet" and obj_token:
        preferred_sheet = _pick_sheet_token(node, parsed)
        df = _load_from_sheet(obj_token, preferred_sheet, token)
        if df.empty:
            return df
        # 再兜底补园区：若标题可识别园区且数据园区缺失/未知，则回填
        park = _extract_park_name(title)
        if park and "园区" in df.columns:
            mask = df["园区"].astype(str).str.strip().isin(["", "未知园区", "nan"])
            if mask.any():
                df.loc[mask, "园区"] = park
        return df

    _set_last_error("未识别为可读取的飞书对象（仅支持 wiki->sheet 或 wiki/base->bitable）。")
    return pd.DataFrame()
