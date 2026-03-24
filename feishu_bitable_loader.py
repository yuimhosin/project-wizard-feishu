# -*- coding: utf-8 -*-
"""飞书多维表格数据加载：从飞书 Bitable 读取养老社区进度表数据。"""
import os
import re
import json
import time
import urllib.request
import urllib.error
from typing import Optional

import pandas as pd

FEISHU_API_BASE = "https://open.feishu.cn/open-apis"
_token_cache = {"token": None, "expires_at": 0}

# 本地与数据库中保留的飞书记录 ID，用于更新/删除；勿在飞书「字段」中重复建同名列
FEISHU_RECORD_ID_COL = "__feishu_record_id"
_BATCH_SIZE = 500


def _get_tenant_access_token() -> Optional[str]:
    """获取 tenant_access_token，带缓存。"""
    app_id = os.getenv("FEISHU_APP_ID", "")
    app_secret = os.getenv("FEISHU_APP_SECRET", "")
    if not app_id or not app_secret:
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
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") == 0:
                token = data.get("tenant_access_token")
                expire = data.get("expire", 7200)
                _token_cache["token"] = token
                _token_cache["expires_at"] = now + expire
                return token
    except Exception:
        pass
    return None


def _parse_bitable_url(url_or_id: str) -> tuple[str, str, bool]:
    """
    从飞书多维表格 URL 解析 app_token 和 table_id。
    支持：
    - base 格式：https://xxx.feishu.cn/base/AppToken 或 ?table=TableId
    - wiki 格式：https://xxx.feishu.cn/wiki/NodeToken?table=TableId
    返回 (app_token, table_id, is_wiki)，table_id 可能为空；is_wiki 表示需通过 wiki API 解析 app_token。
    """
    s = (url_or_id or "").strip()
    table_m = re.search(r"[?&]table=([A-Za-z0-9]+)", s)
    table_id = table_m.group(1) if table_m else ""

    m_base = re.search(r"base/([A-Za-z0-9]+)", s)
    if m_base:
        return m_base.group(1), table_id, False

    m_wiki = re.search(r"wiki/([A-Za-z0-9]+)", s)
    if m_wiki and table_id:
        return m_wiki.group(1), table_id, True

    return "", "", False


def _get_app_token_from_wiki_node(node_token: str) -> Optional[str]:
    """通过 wiki get_node API 获取 bitable 的 app_token。"""
    token = _get_tenant_access_token()
    if not token:
        return None
    url = f"{FEISHU_API_BASE}/wiki/v2/spaces/get_node?token={node_token}"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") != 0:
                return None
            d = data.get("data", {}).get("node", {})
            if d.get("obj_type") == "bitable":
                return d.get("obj_token")
    except Exception:
        pass
    return None


def _get_first_table_id(app_token: str) -> Optional[str]:
    """获取多维表格的第一个数据表 ID。"""
    token = _get_tenant_access_token()
    if not token:
        return None
    url = f"{FEISHU_API_BASE}/bitable/v1/apps/{app_token}/tables"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") != 0:
                return None
            items = data.get("data", {}).get("items", [])
            if items:
                return items[0].get("table_id")
    except Exception:
        pass
    return None


def _flatten_field_value(v) -> str:
    """将飞书字段值转为字符串。"""
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, list):
        parts = []
        for item in v:
            if isinstance(item, dict):
                parts.append(item.get("name") or item.get("text") or str(item))
            else:
                parts.append(str(item))
        return "; ".join(parts) if parts else ""
    if isinstance(v, dict):
        return v.get("text") or v.get("name") or str(v)
    return str(v)


def load_from_bitable(url_or_id: str) -> pd.DataFrame:
    """
    从飞书多维表格加载数据为 DataFrame。
    url_or_id: 飞书多维表格链接，支持：
    - base 格式：https://xxx.feishu.cn/base/AppToken 或含 ?table=TableId
    - wiki 格式：https://xxx.feishu.cn/wiki/NodeToken?table=TableId
    需配置环境变量 FEISHU_APP_ID、FEISHU_APP_SECRET。
    """
    token = _get_tenant_access_token()
    if not token:
        return pd.DataFrame()

    parsed = _parse_bitable_url(url_or_id)
    app_token, table_id, is_wiki = parsed[0], parsed[1], parsed[2]
    if is_wiki:
        app_token = _get_app_token_from_wiki_node(app_token)
        if not app_token:
            return pd.DataFrame()
    elif not app_token:
        return pd.DataFrame()

    if not table_id:
        table_id = _get_first_table_id(app_token)
        if not table_id:
            return pd.DataFrame()

    all_records = []
    page_token = None

    while True:
        params = ["page_size=500"]
        if page_token:
            params.append(f"page_token={page_token}")
        url = f"{FEISHU_API_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records?{'&'.join(params)}"

        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
                if data.get("code") != 0:
                    return pd.DataFrame()
                d = data.get("data", {})
                items = d.get("items", [])
                for rec in items:
                    rid = rec.get("record_id") or ""
                    fields = rec.get("fields", {})
                    flat = {FEISHU_RECORD_ID_COL: str(rid).strip() if rid else ""}
                    for k, v in fields.items():
                        if k in ("file_token", "tmp_url", "avatar_url"):
                            continue
                        flat[k] = _flatten_field_value(v)
                    all_records.append(flat)
                page_token = d.get("page_token")
                if not d.get("has_more", False) or not page_token:
                    break
        except Exception:
            return pd.DataFrame()

    if not all_records:
        return pd.DataFrame()

    return pd.DataFrame(all_records)


def resolve_bitable_app_table(url_or_id: str) -> tuple[str, str] | None:
    """解析多维表格链接为 (app_token, table_id)，失败返回 None。"""
    parsed = _parse_bitable_url(url_or_id)
    app_token, table_id, is_wiki = parsed[0], parsed[1], parsed[2]
    if is_wiki:
        app_token = _get_app_token_from_wiki_node(app_token)
        if not app_token:
            return None
    elif not app_token:
        return None
    if not table_id:
        table_id = _get_first_table_id(app_token)
        if not table_id:
            return None
    return app_token, table_id


def _post_bitable(
    app_token: str,
    table_id: str,
    path_suffix: str,
    body: dict,
    token: str,
) -> tuple[dict | None, str]:
    """POST bitable records API，返回 (json dict, 错误信息)。"""
    url = f"{FEISHU_API_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records/{path_suffix}"
    body_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body_bytes,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            data = json.loads(resp.read().decode())
        if data.get("code") != 0:
            return None, f"code={data.get('code')} msg={data.get('msg')}"
        return data, ""
    except urllib.error.HTTPError as e:
        try:
            raw = e.read().decode("utf-8", errors="ignore")
        except Exception:
            raw = ""
        return None, f"HTTP {e.code} {raw}"
    except Exception as e:
        return None, str(e)


def _row_to_feishu_fields(row: pd.Series) -> dict:
    """DataFrame 行 → 飞书 fields 字典（不含 record_id）。"""
    out: dict = {}
    for k, v in row.items():
        ks = str(k).strip()
        if not ks or ks == FEISHU_RECORD_ID_COL or ks.startswith("__"):
            continue
        if pd.isna(v) or v is None:
            out[ks] = ""
            continue
        if isinstance(v, bool):
            out[ks] = "是" if v else "否"
            continue
        if isinstance(v, (int, float)):
            if ks == "实际预计金额" or ks.endswith("金额"):
                try:
                    out[ks] = float(v)
                except Exception:
                    out[ks] = str(v).strip()
            else:
                out[ks] = str(v).strip()
            continue
        out[ks] = str(v).strip()
    return out


def _rows_equal_for_sync(a: pd.Series, b: pd.Series) -> bool:
    return _row_to_feishu_fields(a) == _row_to_feishu_fields(b)


def sync_bitable_df_diff(
    url: str,
    df_old: pd.DataFrame,
    df_new: pd.DataFrame,
) -> tuple[bool, str, pd.DataFrame | None]:
    """
    将本地变更同步到飞书多维表格：删除、批量新增、批量更新。
    仅支持「多维表格」链接（与 load_from_bitable 相同）；新建行会回填 __feishu_record_id。
    返回 (成功, 说明文案, 若需更新本地行则返回带 record_id 的 DataFrame，否则 None)。
    """
    resolved = resolve_bitable_app_table(url)
    if not resolved:
        return False, "无法解析多维表格链接或无权访问。", None
    app_token, table_id = resolved

    token = _get_tenant_access_token()
    if not token:
        return False, "无法获取飞书 tenant_access_token。", None

    id_col = FEISHU_RECORD_ID_COL
    msgs: list[str] = []
    df_patch = df_new.copy()

    def _chunk(lst: list, n: int = _BATCH_SIZE):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    # —— 删除：旧有 record_id 在新表中不存在
    old_ids: set[str] = set()
    if df_old is not None and not df_old.empty and id_col in df_old.columns:
        old_ids = {
            str(x).strip()
            for x in df_old[id_col].tolist()
            if str(x).strip() and str(x).strip().lower() != "nan"
        }
    new_ids: set[str] = set()
    if df_new is not None and not df_new.empty and id_col in df_new.columns:
        new_ids = {
            str(x).strip()
            for x in df_new[id_col].tolist()
            if str(x).strip() and str(x).strip().lower() != "nan"
        }
    to_delete = list(old_ids - new_ids)
    for chunk in _chunk(to_delete):
        body = {"records": chunk}
        data, err = _post_bitable(app_token, table_id, "batch_delete", body, token)
        if err:
            return False, f"飞书删除记录失败：{err}", None
        msgs.append(f"删除 {len(chunk)} 条")

    old_by_id: dict[str, pd.Series] = {}
    if df_old is not None and not df_old.empty and id_col in df_old.columns:
        for _, r in df_old.iterrows():
            rid = str(r.get(id_col, "") or "").strip()
            if rid and rid.lower() != "nan":
                old_by_id[rid] = r

    # —— 更新
    updates: list[dict] = []
    if df_new is not None and not df_new.empty:
        for _, row in df_new.iterrows():
            rid = str(row.get(id_col, "") or "").strip()
            if not rid or rid.lower() == "nan":
                continue
            if rid not in old_ids:
                continue
            old_row = old_by_id.get(rid)
            if old_row is None:
                continue
            if _rows_equal_for_sync(row, old_row):
                continue
            updates.append({"record_id": rid, "fields": _row_to_feishu_fields(row)})

    for chunk in _chunk(updates):
        body = {"records": chunk}
        data, err = _post_bitable(app_token, table_id, "batch_update", body, token)
        if err:
            return False, f"飞书更新记录失败：{err}", None
        msgs.append(f"更新 {len(chunk)} 条")

    # —— 新增（无 record_id）
    new_rows: list[dict] = []
    new_idx: list = []
    if df_new is not None and not df_new.empty:
        for idx, row in df_new.iterrows():
            rid = str(row.get(id_col, "") or "").strip()
            if rid and rid.lower() != "nan":
                continue
            new_rows.append(_row_to_feishu_fields(row))
            new_idx.append(idx)

    any_new_fill = False
    for i in range(0, len(new_rows), _BATCH_SIZE):
        chunk_fields = new_rows[i : i + _BATCH_SIZE]
        chunk_idx = new_idx[i : i + _BATCH_SIZE]
        if not chunk_fields:
            break
        body = {"records": [{"fields": f} for f in chunk_fields]}
        data, err = _post_bitable(app_token, table_id, "batch_create", body, token)
        if err:
            return False, f"飞书新增记录失败：{err}", None
        items = (data or {}).get("data", {}).get("records", []) or []
        for j, rec in enumerate(items):
            rid = str(rec.get("record_id") or "").strip()
            if j < len(chunk_idx) and rid:
                df_patch.at[chunk_idx[j], id_col] = rid
                any_new_fill = True
        msgs.append(f"新增 {len(chunk_fields)} 条")

    if not msgs:
        return True, "飞书侧无变更（与上次一致）。", None
    return True, "；".join(msgs), df_patch if any_new_fill else None
