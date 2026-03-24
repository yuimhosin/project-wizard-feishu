# -*- coding: utf-8 -*-
"""飞书多维表格数据加载：从飞书 Bitable 读取养老社区进度表数据。"""
import os
import re
import json
import time
import urllib.request
import urllib.error
import urllib.parse
from typing import Optional

import pandas as pd

FEISHU_API_BASE = "https://open.feishu.cn/open-apis"
_token_cache = {"token": None, "expires_at": 0}
_last_error = ""


def _set_last_error(msg: str):
    global _last_error
    _last_error = str(msg or "").strip()


def get_last_error() -> str:
    return _last_error


def _format_http_error(e: Exception) -> str:
    """提取 urllib HTTPError 的状态码与响应体，便于定位飞书权限问题。"""
    try:
        if isinstance(e, urllib.error.HTTPError):
            body = ""
            try:
                raw = e.read()
                if raw:
                    body = raw.decode("utf-8", errors="ignore")
            except Exception:
                body = ""
            return f"http={e.code} reason={getattr(e, 'reason', '')} body={body}".strip()
    except Exception:
        pass
    return repr(e)


def _get_tenant_access_token() -> Optional[str]:
    """获取 tenant_access_token，带缓存。"""
    app_id = os.getenv("FEISHU_APP_ID", "")
    app_secret = os.getenv("FEISHU_APP_SECRET", "")
    if not app_id or not app_secret:
        _set_last_error("缺少 FEISHU_APP_ID 或 FEISHU_APP_SECRET。")
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
                _set_last_error("")
                return token
            _set_last_error(f"获取 tenant_access_token 失败：code={data.get('code')} msg={data.get('msg')}")
    except Exception as e:
        _set_last_error(f"获取 tenant_access_token 异常：{_format_http_error(e)}")
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
    if m_wiki:
        return m_wiki.group(1), table_id, True

    return "", "", False


def _parse_sheets_url(url_or_id: str) -> tuple[str, str]:
    """
    解析飞书电子表格（sheets）链接，返回 (spreadsheet_token, sheet_id)。
    支持：
    - https://xxx.feishu.cn/sheets/SpreadsheetToken?sheet=SheetId
    - https://xxx.feishu.cn/sheets/SpreadsheetToken（无 sheet 参数时后续自动取首个）
    """
    s = (url_or_id or "").strip()
    m_sheet = re.search(r"[?&]sheet=([A-Za-z0-9_]+)", s)
    sheet_id = m_sheet.group(1) if m_sheet else ""
    m_spread = re.search(r"sheets/([A-Za-z0-9]+)", s)
    if m_spread:
        return m_spread.group(1), sheet_id
    return "", ""


def list_sheets_from_sheets_url(url_or_id: str) -> list[dict]:
    """
    读取 sheets 电子表格下所有 sheet 元信息（sheet_id + sheet_name）。
    这是“表结构”读取，用于快速生成下拉选项，不会拉取 sheet 数据。
    """
    spreadsheet_token, _ = _parse_sheets_url(url_or_id)
    if not spreadsheet_token:
        return []

    token = _get_tenant_access_token()
    if not token:
        return []

    url = f"{FEISHU_API_BASE}/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        if data.get("code") != 0:
            _set_last_error(
                f"获取 sheets 列表失败：code={data.get('code')} msg={data.get('msg')}"
            )
            return []

        sheets = data.get("data", {}).get("sheets", []) or []
        out = []
        seen = set()
        for s in sheets:
            sid = str(s.get("sheet_id") or "").strip()
            if not sid or sid in seen:
                continue
            seen.add(sid)
            name = (
                s.get("sheet_name")
                or s.get("sheetTitle")
                or s.get("sheet_title")
                or s.get("title")
                or s.get("name")
                or ""
            )
            name = str(name).strip() if name is not None else ""
            out.append({"sheet_id": sid, "sheet_name": name})

        _set_last_error("")
        return out
    except Exception as e:
        _set_last_error(f"读取 sheets 列表异常：{_format_http_error(e)}")
        return []


def _get_first_sheet_id(spreadsheet_token: str, token: str) -> Optional[str]:
    """读取电子表格元信息，获取第一个 sheet_id。"""
    url = f"{FEISHU_API_BASE}/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") != 0:
                _set_last_error(f"获取 sheets 信息失败：code={data.get('code')} msg={data.get('msg')}")
                return None
            sheets = data.get("data", {}).get("sheets", [])
            if sheets:
                sid = sheets[0].get("sheet_id")
                if sid:
                    _set_last_error("")
                    return sid
            _set_last_error("未找到任何 sheet（sheets 为空）。")
    except Exception as e:
        _set_last_error(f"读取 sheets 元信息异常：{_format_http_error(e)}")
    return None


def _get_sheet_name_from_metadata(spreadsheet_token: str, target_sheet_id: str, token: str) -> str:
    """
    从 sheets/query 元信息中取 sheet 名。
    为兼容不同版本字段名，尽量多尝试几个可能 key。
    """
    if not target_sheet_id:
        return ""
    url = f"{FEISHU_API_BASE}/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        if data.get("code") != 0:
            return ""
        sheets = data.get("data", {}).get("sheets", [])
        for s in sheets:
            if str(s.get("sheet_id") or "").strip() != str(target_sheet_id).strip():
                continue
            for k in ("sheet_name", "sheetTitle", "sheet_title", "title", "name"):
                v = s.get(k)
                if v and str(v).strip():
                    return str(v).strip()
            # 兜底：如果没取到名字，就返回空
            return ""
    except Exception:
        return ""
    return ""


def _get_first_sheet_id_and_name(spreadsheet_token: str, token: str) -> tuple[str, str]:
    """获取第一个 sheet_id + sheet_name。"""
    url = f"{FEISHU_API_BASE}/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        if data.get("code") != 0:
            return "", ""
        sheets = data.get("data", {}).get("sheets", [])
        if not sheets:
            return "", ""
        s0 = sheets[0] or {}
        sid = str(s0.get("sheet_id") or "").strip()
        name = ""
        for k in ("sheet_name", "sheetTitle", "sheet_title", "title", "name"):
            v = s0.get(k)
            if v and str(v).strip():
                name = str(v).strip()
                break
        return sid, name
    except Exception:
        return "", ""


def _load_from_sheets(spreadsheet_token: str, sheet_id: str, token: str) -> pd.DataFrame:
    """通过 Sheets API 读取整个工作表，第一行作为列名。"""
    sheet_name = ""
    if not sheet_id:
        sheet_id, sheet_name = _get_first_sheet_id_and_name(spreadsheet_token, token)
        sheet_id = sheet_id or ""
        if not sheet_id:
            return pd.DataFrame()
    if not sheet_name:
        sheet_name = _get_sheet_name_from_metadata(spreadsheet_token, sheet_id, token)
    try:
        # 1) 先读表头一行，避免一次性读取整个范围导致 10MB 限制
        header_range = f"{sheet_id}!A1:ZZ1"
        encoded_header = urllib.parse.quote(header_range, safe="")
        header_url = f"{FEISHU_API_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values/{encoded_header}"

        header_req = urllib.request.Request(
            header_url,
            method="GET",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(header_req, timeout=30) as resp:
            header_data = json.loads(resp.read().decode())

        if header_data.get("code") != 0:
            _set_last_error(
                f"读取 sheets 表头失败：code={header_data.get('code')} msg={header_data.get('msg')}"
            )
            return pd.DataFrame()

        header_values = header_data.get("data", {}).get("valueRange", {}).get("values", [])
        if not header_values:
            _set_last_error("电子表格为空（表头 values 为空）。")
            return pd.DataFrame()

        header = [str(v).strip() if v is not None else "" for v in header_values[0]]
        if not any(header):
            _set_last_error("首行未检测到表头，请确认第一行为字段名。")
            return pd.DataFrame()

        # 处理重复/空表头，避免 DataFrame 列名冲突
        used = {}
        cols = []
        for i, h in enumerate(header):
            name = h or f"列{i+1}"
            cnt = used.get(name, 0)
            used[name] = cnt + 1
            cols.append(name if cnt == 0 else f"{name}_{cnt+1}")

        # 2) 把列数映射为列字母，避免每次都请求到 ZZ
        def _col_to_letter(n: int) -> str:
            # n: 1-based
            s = ""
            while n > 0:
                n, r = divmod(n - 1, 26)
                s = chr(ord("A") + r) + s
            return s or "A"

        col_end = _col_to_letter(len(cols))  # 最后一列字母

        def _row_is_empty(row_list) -> bool:
            for x in row_list:
                if str(x).strip():
                    return False
            return True

        # 3) 分块读取数据：每次读取 chunk_rows 行
        rows = []
        chunk_rows = 500
        max_end = 50000  # 兜底上限：避免无限循环

        start = 2
        while start <= max_end:
            end = min(max_end, start + chunk_rows - 1)
            range_expr = f"{sheet_id}!A{start}:{col_end}{end}"
            encoded_range = urllib.parse.quote(range_expr, safe="")
            url = f"{FEISHU_API_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values/{encoded_range}"
            req = urllib.request.Request(
                url,
                method="GET",
                headers={"Authorization": f"Bearer {token}"},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())

            if data.get("code") != 0:
                _set_last_error(
                    f"读取 sheets 分块失败：{start}-{end} code={data.get('code')} msg={data.get('msg')}"
                )
                return pd.DataFrame()

            values = data.get("data", {}).get("valueRange", {}).get("values", [])
            if not values:
                break

            chunk_out = []
            for row in values:
                if not row:
                    continue
                row2 = ["" if v is None else str(v) for v in row]
                if len(row2) < len(cols):
                    row2.extend([""] * (len(cols) - len(row2)))
                elif len(row2) > len(cols):
                    row2 = row2[:len(cols)]
                if _row_is_empty(row2):
                    continue
                chunk_out.append(row2)

            if not chunk_out:
                # 连续空块：认为到尾部了
                break

            rows.extend(chunk_out)
            start = end + 1

        if not rows:
            _set_last_error("读取到表头，但无有效数据行。")
            return pd.DataFrame(columns=cols)

        _set_last_error("")
        df = _normalize_sheets_df(pd.DataFrame(rows, columns=cols))
        # 业务适配：用户要求「园区=sheet名」
        if sheet_name:
            df["园区"] = sheet_name
        return df
    except Exception as e:
        _set_last_error(f"读取 sheets 异常：{_format_http_error(e)}")
        return pd.DataFrame()


def _get_wiki_node_info(node_token: str) -> Optional[dict]:
    """通过 wiki get_node API 获取节点信息（obj_type + obj_token）。"""
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
                _set_last_error(f"wiki 节点解析失败：code={data.get('code')} msg={data.get('msg')}")
                return None
            d = data.get("data", {}).get("node", {})
            obj_type = str(d.get("obj_type") or "").strip().lower()
            obj_token = str(d.get("obj_token") or "").strip()
            if obj_type and obj_token:
                _set_last_error("")
                return {
                    "obj_type": obj_type,
                    "obj_token": obj_token,
                    "origin_url": str(d.get("origin_url") or ""),
                    "title": str(d.get("title") or ""),
                }
            _set_last_error("wiki 节点缺少 obj_type 或 obj_token。")
    except Exception as e:
        _set_last_error(f"调用 wiki get_node 异常：{_format_http_error(e)}")
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
                _set_last_error(f"获取 table_id 失败：code={data.get('code')} msg={data.get('msg')}")
                return None
            items = data.get("data", {}).get("items", [])
            if items:
                _set_last_error("")
                return items[0].get("table_id")
            _set_last_error("未找到任何数据表（items 为空）。")
    except Exception as e:
        _set_last_error(f"获取数据表列表异常：{_format_http_error(e)}")
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


def _normalize_sheets_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    适配飞书 sheets 常见结构：
    - 第一行主表头、第二行时间节点（列名常是「列11、列12...」）
    - 右侧大量空列
    - 缺少「园区/社区」列时补默认园区
    """
    if df is None or df.empty:
        return df
    out = df.copy()

    # 第二行补头：把「列xx」改成该行实际节点名
    row0 = out.iloc[0].astype(str).tolist()
    timeline_keywords = ("需求立项", "需求审核", "规划设计方案", "成本核算", "项目决策", "招采", "实施", "验收", "结算")
    if any(any(k in str(v) for k in timeline_keywords) for v in row0):
        rename_map = {}
        for idx, c in enumerate(out.columns):
            v = str(row0[idx]).strip() if idx < len(row0) else ""
            if str(c).startswith("列") and v:
                rename_map[c] = v
        if rename_map:
            out = out.rename(columns=rename_map)
        out = out.iloc[1:].reset_index(drop=True)

    # 删除全空列
    keep_cols = []
    for c in out.columns:
        s = out[c].astype(str).str.strip()
        if (s != "").any() and (~s.str.lower().eq("nan")).any():
            keep_cols.append(c)
    if keep_cols:
        out = out[keep_cols].copy()

    # 删除全空行
    row_non_empty = out.astype(str).apply(lambda col: col.str.strip()).apply(
        lambda col: col.ne("") & col.str.lower().ne("nan")
    ).any(axis=1)
    out = out[row_non_empty].reset_index(drop=True)

    # 过滤“合计/预算系统合计/差额”等汇总行（这些行通常不是真实项目，不应进入筛选与编辑）
    summary_markers = ("现合计", "预算系统合计", "差额")
    try:
        out_str = out.astype(str)
        keep_mask = ~out_str.apply(lambda row: any(m in str(x) for m in summary_markers for x in row), axis=1)
        out = out[keep_mask].reset_index(drop=True)
    except Exception:
        # 保底：如果过滤失败，不阻塞整体读取
        pass

    # 兜底园区列
    if "园区" not in out.columns and "社区" in out.columns:
        out["园区"] = out["社区"]
    elif "园区" not in out.columns and "社区" not in out.columns:
        out["园区"] = "未知园区"

    return out


def load_from_bitable(url_or_id: str, load_all_sheets: bool = False) -> pd.DataFrame:
    """
    从飞书多维表格加载数据为 DataFrame。
    url_or_id: 飞书多维表格链接，支持：
    - base 格式：https://xxx.feishu.cn/base/AppToken 或含 ?table=TableId
    - wiki 格式：https://xxx.feishu.cn/wiki/NodeToken?table=TableId
    需配置环境变量 FEISHU_APP_ID、FEISHU_APP_SECRET。
    """
    _set_last_error("")
    token = _get_tenant_access_token()
    if not token:
        return pd.DataFrame()

    # 先尝试 sheets 链接
    spreadsheet_token, sheet_id = _parse_sheets_url(url_or_id)
    if spreadsheet_token:
        # 默认只读指定 sheet（更快）；若 load_all_sheets=True，则遍历读取全部 sheet（更慢）。
        if not load_all_sheets:
            return _load_from_sheets(spreadsheet_token, sheet_id, token)

        all_df = []
        url = f"{FEISHU_API_BASE}/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                meta = json.loads(resp.read().decode())
            if meta.get("code") != 0:
                _set_last_error(
                    f"获取 sheets 列表失败：code={meta.get('code')} msg={meta.get('msg')}"
                )
                return pd.DataFrame()
            sheets = meta.get("data", {}).get("sheets", []) or []
            for s in sheets:
                sid = str(s.get("sheet_id") or "").strip()
                if not sid:
                    continue
                df_one = _load_from_sheets(spreadsheet_token, sid, token)
                if df_one is not None and not df_one.empty:
                    all_df.append(df_one)
            if not all_df:
                return pd.DataFrame()
            out = pd.concat(all_df, ignore_index=True)
            _set_last_error("")
            return out
        except Exception as e:
            _set_last_error(f"读取所有 sheet 异常：{_format_http_error(e)}")
            return pd.DataFrame()

    parsed = _parse_bitable_url(url_or_id)
    app_token, table_id, is_wiki = parsed[0], parsed[1], parsed[2]
    if is_wiki:
        info = _get_wiki_node_info(app_token)
        if not info:
            return pd.DataFrame()
        obj_type = info.get("obj_type", "")
        obj_token = info.get("obj_token", "")
        # wiki 指向电子表格：直接走 Sheets API
        if obj_type in ("sheet", "sheets", "spreadsheet"):
            return _load_from_sheets(obj_token, sheet_id="", token=token)
        # wiki 指向多维表：沿用 Bitable API
        if obj_type == "bitable":
            app_token = obj_token
        else:
            _set_last_error(f"wiki 节点类型为 {obj_type}，当前仅支持 sheet/bitable。")
            return pd.DataFrame()
    elif not app_token:
        _set_last_error("无法从链接解析 app_token。请使用 base 链接，或 wiki 链接中包含 ?table=xxx。")
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
                    _set_last_error(f"读取 records 失败：code={data.get('code')} msg={data.get('msg')}")
                    return pd.DataFrame()
                d = data.get("data", {})
                items = d.get("items", [])
                for rec in items:
                    fields = rec.get("fields", {})
                    flat = {}
                    for k, v in fields.items():
                        if k in ("file_token", "tmp_url", "avatar_url"):
                            continue
                        flat[k] = _flatten_field_value(v)
                    all_records.append(flat)
                page_token = d.get("page_token")
                if not d.get("has_more", False) or not page_token:
                    break
        except Exception as e:
            _set_last_error(f"读取 records 异常：{_format_http_error(e)}")
            return pd.DataFrame()

    if not all_records:
        _set_last_error("接口返回成功但无记录（records 为空）。")
        return pd.DataFrame()

    _set_last_error("")
    return pd.DataFrame(all_records)
