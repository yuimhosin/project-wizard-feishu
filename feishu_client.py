# -*- coding: utf-8 -*-
"""飞书自定义机器人 Webhook 推送接口。"""
from __future__ import annotations

import json
import os
import urllib.request

import pandas as pd
import streamlit as st


def get_feishu_webhook_url() -> str | None:
    """获取飞书 Webhook URL：页内输入 > Streamlit Secrets > 环境变量 FEISHU_WEBHOOK_URL。"""
    url = (st.session_state.get("feishu_webhook_url") or "").strip()
    if url and url.startswith("https://"):
        return url
    try:
        if hasattr(st, "secrets") and st.secrets:
            for key in ("FEISHU_WEBHOOK_URL", "feishu_webhook_url"):
                try:
                    v = st.secrets[key]
                    if v and str(v).strip().startswith("https://"):
                        return str(v).strip()
                except (KeyError, AttributeError, TypeError):
                    continue
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return os.getenv("FEISHU_WEBHOOK_URL") or None


def _to_json_value(v):
    """转为可 JSON 序列化的值。"""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        if hasattr(v, "item"):
            v = v.item()
    except Exception:
        return str(v)
    if isinstance(v, float):
        if v != v or v == float("inf") or v == float("-inf"):
            return None
        return int(v) if v == int(v) else v
    if isinstance(v, (int, str, bool)):
        return v
    return str(v)


def row_to_dict(row: pd.Series) -> dict:
    """将一行转为可 JSON 序列化的字典。"""
    out = {}
    for k, v in row.items():
        out[str(k)] = _to_json_value(v)
    return out


def format_cell(v) -> str:
    """用于变更详情展示。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def compute_df_diff(old_df: pd.DataFrame, new_df: pd.DataFrame) -> dict:
    """
    按「序号」对比新旧表，返回删除、新增、修改的明细及修改详情。
    返回：{"deleted": [], "added": [], "modified": [], "modified_details": [{序号, 变更项: ["列名: 旧→新"]}]}
    """
    out = {"deleted": [], "added": [], "modified": [], "modified_details": []}
    if old_df.empty and new_df.empty:
        return out
    key_col = "序号"
    if key_col not in old_df.columns or key_col not in new_df.columns:
        return out
    try:
        old_df = old_df.dropna(subset=[key_col])
        new_df = new_df.dropna(subset=[key_col])
        old_df = old_df.astype({key_col: "float64"})
        new_df = new_df.astype({key_col: "float64"})
    except Exception:
        return out
    old_ids = set(old_df[key_col].astype(int).tolist())
    new_ids = set(new_df[key_col].astype(int).tolist())
    deleted_ids = old_ids - new_ids
    added_ids = new_ids - old_ids
    common_ids = old_ids & new_ids
    for sid in deleted_ids:
        row = old_df[old_df[key_col].astype(int) == sid].iloc[0]
        out["deleted"].append(row_to_dict(row))
    for sid in added_ids:
        row = new_df[new_df[key_col].astype(int) == sid].iloc[0]
        out["added"].append(row_to_dict(row))
    for sid in common_ids:
        old_row = old_df[old_df[key_col].astype(int) == sid].iloc[0]
        new_row = new_df[new_df[key_col].astype(int) == sid].iloc[0]
        if not old_row.equals(new_row):
            out["modified"].append(row_to_dict(new_row))
            changes = []
            for col in old_row.index:
                if col not in new_row.index:
                    continue
                ov = format_cell(old_row[col])
                nv = format_cell(new_row[col])
                if ov != nv:
                    changes.append(f"{col}：{ov or '（空）'} → {nv or '（空）'}")
            out["modified_details"].append({"序号": int(sid), "变更项": changes})
    return out


def build_feishu_payload_from_diff(diff: dict, total_after: int, source: str = "看板编辑") -> dict:
    """根据 diff 构建飞书 Webhook 的 JSON 负载。"""
    n_del = int(len(diff["deleted"]))
    n_add = int(len(diff["added"]))
    n_mod = int(len(diff["modified"]))
    modified_details = diff.get("modified_details") or []
    total_after = int(total_after)
    parts = []
    if n_del:
        parts.append(f"删除 {n_del} 条")
    if n_mod:
        parts.append(f"修改 {n_mod} 条")
    if n_add:
        parts.append(f"新增 {n_add} 条")
    summary_text = "、".join(parts) if parts else "无结构变更"
    text = f"【养老社区进度表】{source}：{summary_text}，当前共 {total_after} 条记录。"
    if modified_details:
        detail_lines = []
        for item in modified_details:
            seq = item.get("序号", "")
            changes = item.get("变更项") or []
            if changes:
                detail_lines.append(f"序号 {seq}：" + "；".join(changes))
        if detail_lines:
            text += "\n修改详情：\n" + "\n".join(detail_lines)
    change_type = (
        "mixed"
        if (n_del and n_add) or (n_del and n_mod) or (n_add and n_mod)
        else ("delete" if n_del and not n_add and not n_mod else ("add" if n_add and not n_del and not n_mod else "modify"))
    )
    payload = {
        "message_type": "text",
        "text": text,
        "change_type": change_type,
        "deleted_count": n_del,
        "added_count": n_add,
        "modified_count": n_mod,
        "total_after": total_after,
        "changes": {
            "deleted": diff["deleted"],
            "added": diff["added"],
            "modified": diff["modified"],
        },
    }
    if modified_details:
        payload["modified_details"] = modified_details
    return payload


def _ensure_native_json(obj):
    """递归将 dict/list 中的值转为可 JSON 序列化的原生类型。"""
    if obj is None or isinstance(obj, (bool, str)):
        return obj
    if isinstance(obj, (int, float)):
        if isinstance(obj, float) and (obj != obj or abs(obj) == float("inf")):
            return None
        return int(obj) if isinstance(obj, float) and obj == int(obj) else obj
    if hasattr(obj, "item"):
        try:
            return _ensure_native_json(obj.item())
        except Exception:
            return str(obj)
    if isinstance(obj, dict):
        return {str(k): _ensure_native_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_ensure_native_json(x) for x in obj]
    return str(obj)


def push_to_feishu(text: str | None = None, payload: dict | None = None) -> bool:
    """向飞书 Webhook 推送。"""
    url = get_feishu_webhook_url()
    if not url:
        return False
    if payload is not None:
        body_dict = _ensure_native_json(payload)
        if os.getenv("FEISHU_PAYLOAD_SIMPLE", "").strip() in ("1", "true", "True"):
            _modified_details = body_dict.get("modified_details")
            body_dict = {
                "message_type": "text",
                "text": body_dict.get("text") or "",
                "change_type": body_dict.get("change_type", ""),
                "deleted_count": body_dict.get("deleted_count", 0),
                "added_count": body_dict.get("added_count", 0),
                "modified_count": body_dict.get("modified_count", 0),
                "total_after": body_dict.get("total_after", 0),
                "changes_json": json.dumps(body_dict.get("changes") or {}, ensure_ascii=False, default=str),
            }
            if _modified_details:
                body_dict["modified_details"] = _modified_details
        elif body_dict.get("message_type") is None and body_dict.get("msg_type") is not None:
            body_dict["message_type"] = "text"
    elif text and str(text).strip():
        body_dict = {"message_type": "text", "text": text.strip()}
    else:
        return False
    _msg = body_dict.get("text") or ""
    if _msg and os.getenv("FEISHU_DEBUG_TEXT"):
        print("[飞书推送] text:", _msg[:200] + ("..." if len(_msg) > 200 else ""))
    try:
        body = json.dumps(body_dict, ensure_ascii=False, default=str).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "application/json; charset=utf-8"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode())
                return data.get("StatusCode") == 0 or data.get("code") == 0
    except Exception:
        pass
    return False
# -*- coding: utf-8 -*-
"""飞书自定义机器人 Webhook 推送接口。"""
import os
import json
import urllib.request
import streamlit as st
import pandas as pd


def get_feishu_webhook_url() -> str | None:
    """获取飞书 Webhook URL：页内输入 > Streamlit Secrets > 环境变量 FEISHU_WEBHOOK_URL。"""
    url = (st.session_state.get("feishu_webhook_url") or "").strip()
    if url and url.startswith("https://"):
        return url
    try:
        if hasattr(st, "secrets") and st.secrets:
            for key in ("FEISHU_WEBHOOK_URL", "feishu_webhook_url"):
                try:
                    v = st.secrets[key]
                    if v and str(v).strip().startswith("https://"):
                        return str(v).strip()
                except (KeyError, AttributeError, TypeError):
                    continue
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return os.getenv("FEISHU_WEBHOOK_URL") or None


def _to_json_value(v):
    """转为可 JSON 序列化的值。"""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        if hasattr(v, "item"):
            v = v.item()
    except Exception:
        return str(v)
    if isinstance(v, float):
        if v != v or v == float("inf") or v == float("-inf"):
            return None
        return int(v) if v == int(v) else v
    if isinstance(v, (int, str, bool)):
        return v
    return str(v)


def row_to_dict(row: pd.Series) -> dict:
    """将一行转为可 JSON 序列化的字典。"""
    out = {}
    for k, v in row.items():
        out[str(k)] = _to_json_value(v)
    return out


def format_cell(v) -> str:
    """用于变更详情展示。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def compute_df_diff(old_df: pd.DataFrame, new_df: pd.DataFrame) -> dict:
    """
    按「序号」对比新旧表，返回删除、新增、修改的明细及修改详情。
    返回：{"deleted": [], "added": [], "modified": [], "modified_details": [{序号, 变更项: ["列名: 旧→新"]}]}
    """
    out = {"deleted": [], "added": [], "modified": [], "modified_details": []}
    if old_df.empty and new_df.empty:
        return out
    key_col = "序号"
    if key_col not in old_df.columns or key_col not in new_df.columns:
        return out
    try:
        old_df = old_df.dropna(subset=[key_col])
        new_df = new_df.dropna(subset=[key_col])
        old_df = old_df.astype({key_col: "float64"})
        new_df = new_df.astype({key_col: "float64"})
    except Exception:
        return out
    old_ids = set(old_df[key_col].astype(int).tolist())
    new_ids = set(new_df[key_col].astype(int).tolist())
    deleted_ids = old_ids - new_ids
    added_ids = new_ids - old_ids
    common_ids = old_ids & new_ids
    for sid in deleted_ids:
        row = old_df[old_df[key_col].astype(int) == sid].iloc[0]
        out["deleted"].append(row_to_dict(row))
    for sid in added_ids:
        row = new_df[new_df[key_col].astype(int) == sid].iloc[0]
        out["added"].append(row_to_dict(row))
    for sid in common_ids:
        old_row = old_df[old_df[key_col].astype(int) == sid].iloc[0]
        new_row = new_df[new_df[key_col].astype(int) == sid].iloc[0]
        if not old_row.equals(new_row):
            out["modified"].append(row_to_dict(new_row))
            changes = []
            for col in old_row.index:
                if col not in new_row.index:
                    continue
                ov = format_cell(old_row[col])
                nv = format_cell(new_row[col])
                if ov != nv:
                    changes.append(f"{col}：{ov or '（空）'} → {nv or '（空）'}")
            out["modified_details"].append({"序号": int(sid), "变更项": changes})
    return out


def build_feishu_payload_from_diff(diff: dict, total_after: int, source: str = "看板编辑") -> dict:
    """根据 diff 构建飞书 Webhook 的 JSON 负载。"""
    n_del = int(len(diff["deleted"]))
    n_add = int(len(diff["added"]))
    n_mod = int(len(diff["modified"]))
    modified_details = diff.get("modified_details") or []
    total_after = int(total_after)
    parts = []
    if n_del:
        parts.append(f"删除 {n_del} 条")
    if n_mod:
        parts.append(f"修改 {n_mod} 条")
    if n_add:
        parts.append(f"新增 {n_add} 条")
    summary_text = "、".join(parts) if parts else "无结构变更"
    text = f"【养老社区进度表】{source}：{summary_text}，当前共 {total_after} 条记录。"
    if modified_details:
        detail_lines = []
        for item in modified_details:
            seq = item.get("序号", "")
            changes = item.get("变更项") or []
            if changes:
                detail_lines.append(f"序号 {seq}：" + "；".join(changes))
        if detail_lines:
            text += "\n修改详情：\n" + "\n".join(detail_lines)
    change_type = "mixed" if (n_del and n_add) or (n_del and n_mod) or (n_add and n_mod) else ("delete" if n_del and not n_add and not n_mod else ("add" if n_add and not n_del and not n_mod else "modify"))
    payload = {
        "message_type": "text",
        "text": text,
        "change_type": change_type,
        "deleted_count": n_del,
        "added_count": n_add,
        "modified_count": n_mod,
        "total_after": total_after,
        "changes": {
            "deleted": diff["deleted"],
            "added": diff["added"],
            "modified": diff["modified"],
        },
    }
    if modified_details:
        payload["modified_details"] = modified_details
    return payload


def _ensure_native_json(obj):
    """递归将 dict/list 中的值转为可 JSON 序列化的原生类型。"""
    if obj is None or isinstance(obj, (bool, str)):
        return obj
    if isinstance(obj, (int, float)):
        if isinstance(obj, float) and (obj != obj or abs(obj) == float("inf")):
            return None
        return int(obj) if isinstance(obj, float) and obj == int(obj) else obj
    if hasattr(obj, "item"):
        try:
            return _ensure_native_json(obj.item())
        except Exception:
            return str(obj)
    if isinstance(obj, dict):
        return {str(k): _ensure_native_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_ensure_native_json(x) for x in obj]
    return str(obj)


def push_to_feishu(text: str | None = None, payload: dict | None = None) -> bool:
    """向飞书 Webhook 推送。"""
    url = get_feishu_webhook_url()
    if not url:
        return False
    if payload is not None:
        body_dict = _ensure_native_json(payload)
        if os.getenv("FEISHU_PAYLOAD_SIMPLE", "").strip() in ("1", "true", "True"):
            _modified_details = body_dict.get("modified_details")
            body_dict = {
                "message_type": "text",
                "text": body_dict.get("text") or "",
                "change_type": body_dict.get("change_type", ""),
                "deleted_count": body_dict.get("deleted_count", 0),
                "added_count": body_dict.get("added_count", 0),
                "modified_count": body_dict.get("modified_count", 0),
                "total_after": body_dict.get("total_after", 0),
                "changes_json": json.dumps(body_dict.get("changes") or {}, ensure_ascii=False, default=str),
            }
            if _modified_details:
                body_dict["modified_details"] = _modified_details
        elif body_dict.get("message_type") is None and body_dict.get("msg_type") is not None:
            body_dict["message_type"] = "text"
    elif text and str(text).strip():
        body_dict = {"message_type": "text", "text": text.strip()}
    else:
        return False
    _msg = body_dict.get("text") or ""
    if _msg and os.getenv("FEISHU_DEBUG_TEXT"):
        print("[飞书推送] text:", _msg[:200] + ("..." if len(_msg) > 200 else ""))
    try:
        body = json.dumps(body_dict, ensure_ascii=False, default=str).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST", headers={"Content-Type": "application/json; charset=utf-8"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode())
                return data.get("StatusCode") == 0 or data.get("code") == 0
    except Exception:
        pass
    return False
