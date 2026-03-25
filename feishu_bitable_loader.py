# -*- coding: utf-8 -*-
"""飞书多维表格数据加载：从飞书 Bitable 读取养老社区进度表数据。"""
import os
import re
import json
import time
import urllib.request
import urllib.error
import urllib.parse
import math
from typing import Optional
from datetime import datetime

import pandas as pd

FEISHU_API_BASE = "https://open.feishu.cn/open-apis"
_token_cache = {"token": None, "expires_at": 0}
_last_error = ""
FEISHU_RECORD_ID_COL = "__feishu_record_id"
EXCLUDED_SHEET_NAMES = {"汇总分析", "填写备注", "百万级项目明细"}
# 开放平台「向单个范围写入数据」：单次不得超过 5000 行、100 列，超出会报 90202 validate RangeVal fail
FEISHU_VALUES_MAX_COLS = 100
FEISHU_VALUES_MAX_ROWS = 5000

# 与 _load_from_sheets 双行表头合并规则一致，用于写回时列对齐
_TIMELINE_HEADER_TOKENS = frozenset(
    {
        "需求立项",
        "需求审核",
        "规划设计方案",
        "成本核算",
        "项目决策",
        "招采",
        "实施",
        "验收(社区需求完成交付)",
        "验收",
        "结算",
        "文字说明及构思",
        "形成方案",
        "运保总部审核",
        "上联席会",
        "立项呈批",
        "预算动支发起",
    }
)


def _norm_sheet_header_paren(s: str) -> str:
    return str(s or "").strip().replace("（", "(").replace("）", ")")


def _merge_sheet_header_rows(row1: list, row2: list) -> list[str]:
    """合并第 1、2 行表头（与读取逻辑一致），返回与物理列数相同的逻辑列名列表（去重前）。"""
    merged_header: list[str] = []
    for i, h1 in enumerate(row1):
        h2 = row2[i] if i < len(row2) else ""
        h1s = str(h1).strip() if h1 is not None else ""
        h2s = str(h2).strip() if h2 is not None else ""
        if re.fullmatch(r"\d+", h1s or "") and h2s:
            merged_header.append(h2s)
            continue
        if not h1s and h2s in _TIMELINE_HEADER_TOKENS:
            merged_header.append(h2s)
            continue
        if _norm_sheet_header_paren(h1s) == "预计节点(月份)" and h2s:
            merged_header.append(h2s)
            continue
        merged_header.append(h1s)
    return merged_header


def _dedupe_sheet_column_names(header: list) -> list[str]:
    """与读取 DataFrame 列名规则一致，保证写回列序与读入一致。"""
    used: dict[str, int] = {}
    cols: list[str] = []
    for i, h in enumerate(header):
        name = (str(h).strip() if h is not None else "") or f"列{i + 1}"
        cnt = used.get(name, 0)
        used[name] = cnt + 1
        cols.append(name if cnt == 0 else f"{name}_{cnt + 1}")
    return cols


def _set_last_error(msg: str):
    global _last_error
    _last_error = str(msg or "").strip()


def get_last_error() -> str:
    return _last_error


def _is_excluded_sheet_name(name: str) -> bool:
    return str(name or "").strip() in EXCLUDED_SHEET_NAMES


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
            if _is_excluded_sheet_name(name):
                continue
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

        # 再读第二行，适配“燕园 0~7 + 第二行真实节点名”以及“预计节点（月）+ 第二行节点名”结构
        header2 = []
        try:
            h2_range = f"{sheet_id}!A2:ZZ2"
            h2_encoded = urllib.parse.quote(h2_range, safe="")
            h2_url = f"{FEISHU_API_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values/{h2_encoded}"
            h2_req = urllib.request.Request(
                h2_url,
                method="GET",
                headers={"Authorization": f"Bearer {token}"},
            )
            with urllib.request.urlopen(h2_req, timeout=30) as resp:
                h2_data = json.loads(resp.read().decode())
            if h2_data.get("code") == 0:
                h2_values = (h2_data.get("data") or {}).get("valueRange", {}).get("values", []) or []
                if h2_values:
                    header2 = [str(v).strip() if v is not None else "" for v in h2_values[0]]
        except Exception:
            header2 = []

        merged_header = _merge_sheet_header_rows(header, header2)
        cols = _dedupe_sheet_column_names(merged_header)

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

        # 3) 分块读取数据：从探测到的首条数据行开始（兼容多行表头）
        rows = []
        chunk_rows = 500
        max_end = 50000  # 兜底上限：避免无限循环

        data_start_row = _detect_sheet_data_start_row(spreadsheet_token, sheet_id, token)
        start = data_start_row
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
        # 行号标识（用于后续写回时定位）
        df[FEISHU_RECORD_ID_COL] = [str(i) for i in range(data_start_row, data_start_row + len(df))]
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
    keep_idx = []
    for idx, c in enumerate(out.columns):
        # 用 iloc 按位置取列，避免重复列名时 out[c] 变成 DataFrame
        s = out.iloc[:, idx].astype(str).str.strip()
        if (s != "").any() and (~s.str.lower().eq("nan")).any():
            keep_idx.append(idx)
    if keep_idx:
        out = out.iloc[:, keep_idx].copy()

    # 删除占位列（列11/列12...）：结构不统一时常出现，且大多为空
    try:
        drop_placeholder_idx = []
        for idx, c in enumerate(out.columns):
            name = str(c).strip()
            if not re.fullmatch(r"列\d+", name):
                continue
            # 用 iloc 按位置取列，避免重复列名冲突
            s = out.iloc[:, idx].astype(str).str.strip().str.lower()
            non_empty = (~s.isin(["", "nan", "none", "null"])).sum()
            ratio = float(non_empty) / max(len(out), 1)
            # 占位列只有极少量有效值时直接丢弃，避免污染编辑表单
            if ratio <= 0.1:
                drop_placeholder_idx.append(idx)
        if drop_placeholder_idx:
            keep_after = [i for i in range(out.shape[1]) if i not in set(drop_placeholder_idx)]
            out = out.iloc[:, keep_after].copy()
    except Exception:
        pass

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
                    rid = rec.get("record_id") or ""
                    flat = {FEISHU_RECORD_ID_COL: str(rid).strip() if rid else ""}
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


# 写回时绝不能把「日期」列里的数字当普通数传给飞书，否则易显示成 J3+7 等异常文本；金额/序号列保持数字
_FEISHU_AMOUNT_OR_ID_COLS = frozenset(
    {
        "序号",
        "实际预计金额",
        "上报预算金额",
        "拟定金额",
    }
)


def _column_is_timeline_like_for_write(col_name: str) -> bool:
    """是否应按「日期」语义写字符串（含 Excel 序列号转 YYYY-MM-DD）。避免「项目名称」等含「立项」子串被误判。"""
    s = str(col_name or "").strip()
    if not s or s in _FEISHU_AMOUNT_OR_ID_COLS:
        return False
    if s in _TIMELINE_HEADER_TOKENS:
        return True
    if s.startswith("验收") or s.startswith("结算"):
        return True
    for k in (
        "需求审核",
        "需求立项",
        "规划设计",
        "成本核算",
        "项目决策",
        "文字说明",
        "形成方案",
        "运保总部",
        "上联席会",
        "立项呈批",
        "预算动支",
        "招采",
        "实施",
    ):
        if k in s:
            return True
    return False


def _excel_serial_to_date_str(v) -> str | None:
    """将 Excel 日期序列号转为 YYYY-MM-DD；失败则返回 None。"""
    try:
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return None
        if abs(x - round(x)) > 1e-5:
            return None
        iv = int(round(x))
        # 约 1995–2050 的常见序列号区间；避免把金额 50000 当日期（见下方列名判断）
        if iv < 33000 or iv > 55000:
            return None
        dt = pd.to_datetime(iv, unit="D", origin="1899-12-30", errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def _normalize_cell_for_feishu(v, column_name: str = "") -> object:
    """飞书单元格：NaN/非法标量清理；日期列禁止传裸序列号（否则飞书可能显示为 J3+7 等）。"""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    if hasattr(v, "item") and not isinstance(v, (bytes, str, dict, list)):
        try:
            v = v.item()
        except Exception:
            pass
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return ""
    if isinstance(v, pd.Timestamp):
        try:
            if pd.isna(v):
                return ""
            return v.strftime("%Y-%m-%d")
        except Exception:
            return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")

    is_date_col = _column_is_timeline_like_for_write(column_name)

    if isinstance(v, (int, float)) and not isinstance(v, bool):
        if is_date_col:
            ds = _excel_serial_to_date_str(v)
            if ds:
                return ds
            # 小整数可能是误标；仍输出为字符串避免飞书误解析
            if isinstance(v, float) and v == int(v) and -1e9 < v < 1e9:
                return str(int(v))
            return str(v)
        return v

    if isinstance(v, bool):
        return v

    s = str(v)
    # 日期列若已是合法日期串，保持
    if is_date_col:
        st = s.strip()
        # 疑似错误展示的「列字母+行+偏移」不原样写回
        if re.fullmatch(r"[A-Z]{1,3}\d+\+\d+", st):
            return ""
        return st

    return s


def _col_idx_to_letter(idx: int) -> str:
    n = idx + 1
    letters = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(65 + rem))
    return "".join(reversed(letters)) or "A"


def _sheet_join_range(sheet_id: str, a1: str) -> str:
    return f"{sheet_id}!{a1}"


def _get_sheet_grid_column_count(
    spreadsheet_token: str, sheet_id: str, token: str
) -> int | None:
    """从 sheets/query 读取工作表 grid 列数（与 insert_dimension endIndex 上限一致）。"""
    try:
        url = f"{FEISHU_API_BASE}/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        if data.get("code") != 0:
            return None
        for s in (data.get("data") or {}).get("sheets") or []:
            if str(s.get("sheet_id") or "").strip() != str(sheet_id).strip():
                continue
            gp = s.get("grid_properties") or {}
            n = gp.get("column_count")
            if n is not None:
                try:
                    return int(n)
                except Exception:
                    return None
        return None
    except Exception:
        return None


def _post_add_dimension_columns(
    spreadsheet_token: str, sheet_id: str, length: int, token: str
) -> tuple[bool, str]:
    """在工作表列末尾增加空白列（官方「增加行列」接口，避免 insert_dimension 的 endIndex 越界 90202）。"""
    if length <= 0 or length >= 5000:
        return False, "length 须在 (0,5000) 内。"
    body = {
        "dimension": {
            "sheetId": sheet_id,
            "majorDimension": "COLUMNS",
            "length": length,
        }
    }
    body_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
    url = f"{FEISHU_API_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/dimension_range"
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
        if data.get("code") == 0:
            return True, ""
        return False, f"code={data.get('code')} msg={data.get('msg')}"
    except Exception as e:
        return False, _format_http_error(e)


def _ensure_sheet_column_actual_amount(
    spreadsheet_token: str,
    sheet_id: str,
    token: str,
    source_cols: list[str],
) -> tuple[bool, str]:
    """
    若合并表头中尚无法定位「实际预计金额」列，则在表尾插入一列并写入表头，便于后续单格或整表写回。
    """
    target_cols, ferr = _fetch_merged_target_cols_for_write(spreadsheet_token, sheet_id, token)
    if ferr:
        return False, ferr
    if not target_cols:
        return False, "目标表头为空，无法新增金额列。"

    idx = _find_sheet_column_index_for_df_column(
        target_cols, source_cols, "实际预计金额"
    )
    if idx is not None:
        return True, ""

    cc_before = _get_sheet_grid_column_count(spreadsheet_token, sheet_id, token)
    if cc_before is None or cc_before < 1:
        cc_before = len(target_cols)

    ok, err = _post_add_dimension_columns(spreadsheet_token, sheet_id, 1, token)
    if not ok:
        return False, f"增加「实际预计金额」列失败：{err}"

    new_idx = cc_before
    letter = _col_idx_to_letter(new_idx)
    hdr = [["实际预计金额"]]
    ok2, err2 = _put_sheets_range(
        spreadsheet_token, sheet_id, f"{letter}1:{letter}1", hdr, token
    )
    if not ok2:
        return False, f"写入金额列表头失败：{err2}"

    data_start = _detect_sheet_data_start_row(spreadsheet_token, sheet_id, token)
    if data_start >= 3:
        ok3, err3 = _put_sheets_range(
            spreadsheet_token, sheet_id, f"{letter}2:{letter}2", [[""]], token
        )
        if not ok3:
            return False, f"写入表头第二行失败：{err3}"

    return True, ""


def _put_sheets_range(spreadsheet_token: str, sheet_id: str, a1: str, values: list, token: str) -> tuple[bool, str]:
    body = {
        "valueRange": {
            "range": _sheet_join_range(sheet_id, a1),
            "values": values,
        }
    }
    body_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
    url = f"{FEISHU_API_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values"
    req = urllib.request.Request(
        url,
        data=body_bytes,
        method="PUT",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            data = json.loads(resp.read().decode())
        if data.get("code") == 0:
            return True, ""
        return False, f"code={data.get('code')} msg={data.get('msg')}"
    except Exception as e:
        return False, _format_http_error(e)


def _post_values_batch_update(
    spreadsheet_token: str, token: str, value_ranges: list
) -> tuple[bool, str]:
    """一次请求写入多个不重叠范围，避免多次 PUT 导致飞书端展示异常。"""
    if not value_ranges:
        return True, ""
    body = {"valueRanges": value_ranges}
    body_bytes = json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
    url = f"{FEISHU_API_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_update"
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
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode())
        if data.get("code") == 0:
            return True, ""
        return False, f"code={data.get('code')} msg={data.get('msg')}"
    except Exception as e:
        return False, _format_http_error(e)


def _resolve_df_column_for_sheet_header(target_col: str, source_cols: list[str]) -> str | None:
    """表头列名 → DataFrame 中对应列名（与 sync 写回逻辑一致）。"""
    if target_col in source_cols:
        return target_col
    if target_col == "拟定金额" and "实际预计金额" in source_cols:
        return "实际预计金额"
    tc_base = re.sub(r"[（(].*?[)）]", "", str(target_col)).strip()
    for sc in source_cols:
        scs = str(sc).strip()
        sc_base = re.sub(r"[（(].*?[)）]", "", scs).strip()
        if scs == target_col or sc_base == tc_base:
            return sc
    return None


def _find_sheet_column_index_for_df_column(
    target_cols: list[str], source_cols: list[str], df_column_name: str
) -> int | None:
    """在合并后的表头列序列中，找到与 df 列名对应的那一列索引（0-based）。"""
    dfn = str(df_column_name).strip()
    for i, tc in enumerate(target_cols):
        if str(tc).strip() == dfn:
            return i
    for i, tc in enumerate(target_cols):
        src = _resolve_df_column_for_sheet_header(tc, source_cols)
        if src == dfn:
            return i
    dfn_base = re.sub(r"[（(].*?[)）]", "", dfn).strip()
    for i, tc in enumerate(target_cols):
        tcb = re.sub(r"[（(].*?[)）]", "", str(tc)).strip()
        if tcb and dfn_base and tcb == dfn_base:
            return i
    return None


def _fetch_merged_target_cols_for_write(
    spreadsheet_token: str, sheet_id: str, token: str
) -> tuple[list[str], str | None]:
    """
    读取并合并第 1、2 行表头，返回 (target_cols, None)；失败返回 ([], 错误信息)。
    """
    try:
        header_range = f"{sheet_id}!A1:ZZ1"
        encoded_header = urllib.parse.quote(header_range, safe="")
        header_url = f"{FEISHU_API_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values/{encoded_header}"
        req_header = urllib.request.Request(
            header_url,
            method="GET",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req_header, timeout=30) as resp:
            header_data = json.loads(resp.read().decode())
        if header_data.get("code") != 0:
            return [], f"读取原表头失败：code={header_data.get('code')} msg={header_data.get('msg')}"
        header_values = header_data.get("data", {}).get("valueRange", {}).get("values", []) or []
        row1 = [str(x).strip() if x is not None else "" for x in (header_values[0] if header_values else [])]
        row2: list[str] = []
        try:
            h2_range = f"{sheet_id}!A2:ZZ2"
            h2_enc = urllib.parse.quote(h2_range, safe="")
            h2_url = f"{FEISHU_API_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values/{h2_enc}"
            h2_req = urllib.request.Request(h2_url, method="GET", headers={"Authorization": f"Bearer {token}"})
            with urllib.request.urlopen(h2_req, timeout=30) as resp:
                h2_data = json.loads(resp.read().decode())
            if h2_data.get("code") == 0:
                h2_vals = (h2_data.get("data") or {}).get("valueRange", {}).get("values", []) or []
                if h2_vals:
                    row2 = [str(x).strip() if x is not None else "" for x in h2_vals[0]]
        except Exception:
            row2 = []
        merged = _merge_sheet_header_rows(row1, row2)
        target_cols = _dedupe_sheet_column_names(merged)
        return target_cols, None
    except Exception as e:
        return [], f"读取原表头异常：{_format_http_error(e)}"


def sync_sheets_update_single_cell(
    url_or_id: str,
    df_for_resolve: pd.DataFrame,
    sheet_row_1based: int,
    df_column_name: str,
    raw_value,
) -> tuple[bool, str]:
    """
    仅更新飞书表格中一个单元格（按合并表头定位列、按行号定位数据行）。
    不整表覆盖，避免误伤其它列。
    """
    spreadsheet_token, sheet_id = _parse_sheets_url(url_or_id)
    if not spreadsheet_token:
        return False, "链接不是 sheets 地址，未执行电子表格写回。"
    token = _get_tenant_access_token()
    if not token:
        return False, get_last_error() or "无法获取 tenant_access_token。"
    if not sheet_id:
        sheet_id = _get_first_sheet_id(spreadsheet_token, token) or ""
    if not sheet_id:
        return False, "无法解析 sheet_id。"
    if sheet_row_1based < 1:
        return False, f"无效的数据行号：{sheet_row_1based}"

    source_cols = [
        c for c in df_for_resolve.columns if str(c).strip() and not str(c).startswith("__")
    ]
    target_cols, ferr = _fetch_merged_target_cols_for_write(spreadsheet_token, sheet_id, token)
    if ferr:
        return False, ferr
    if not target_cols:
        return False, "目标表头为空。"

    idx = _find_sheet_column_index_for_df_column(target_cols, source_cols, df_column_name)
    if idx is None and str(df_column_name).strip() == "实际预计金额":
        ok_ins, e = _ensure_sheet_column_actual_amount(
            spreadsheet_token, sheet_id, token, source_cols
        )
        if not ok_ins:
            return False, e or "无法新增金额列"
        target_cols, ferr = _fetch_merged_target_cols_for_write(
            spreadsheet_token, sheet_id, token
        )
        if ferr:
            return False, ferr
        if not target_cols:
            return False, "目标表头为空。"
        idx = _find_sheet_column_index_for_df_column(
            target_cols, source_cols, df_column_name
        )
    if idx is None:
        return (
            False,
            f"无法在飞书表头中定位与「{df_column_name}」对应的列，请改用整表同步或检查列名。",
        )

    letter = _col_idx_to_letter(idx)
    cell = _normalize_cell_for_feishu(raw_value, df_column_name)
    a1 = f"{letter}{sheet_row_1based}:{letter}{sheet_row_1based}"
    ok, err = _put_sheets_range(spreadsheet_token, sheet_id, a1, [[cell]], token)
    if not ok:
        return False, err or "单格写入失败"
    _set_last_error("")
    return True, f"已更新飞书单元格 {sheet_id}!{a1}"


def sync_sheets_update_cells_batch(
    url_or_id: str,
    df_for_resolve: pd.DataFrame,
    cells: list[tuple[int, str, object]],
) -> tuple[bool, str]:
    """
    批量更新多个单元格（同一工作表），不整表覆盖。
    cells: (sheet_row_1based, df_column_name, raw_value)
    """
    if not cells:
        return True, "无单元格需更新。"
    spreadsheet_token, sheet_id = _parse_sheets_url(url_or_id)
    if not spreadsheet_token:
        return False, "链接不是 sheets 地址，未执行电子表格写回。"
    token = _get_tenant_access_token()
    if not token:
        return False, get_last_error() or "无法获取 tenant_access_token。"
    if not sheet_id:
        sheet_id = _get_first_sheet_id(spreadsheet_token, token) or ""
    if not sheet_id:
        return False, "无法解析 sheet_id。"

    source_cols = [
        c for c in df_for_resolve.columns if str(c).strip() and not str(c).startswith("__")
    ]
    target_cols, ferr = _fetch_merged_target_cols_for_write(spreadsheet_token, sheet_id, token)
    if ferr:
        return False, ferr
    if not target_cols:
        return False, "目标表头为空。"

    if any(str(c[1]).strip() == "实际预计金额" for c in cells):
        idx_amt = _find_sheet_column_index_for_df_column(
            target_cols, source_cols, "实际预计金额"
        )
        if idx_amt is None:
            ok_ins, e = _ensure_sheet_column_actual_amount(
                spreadsheet_token, sheet_id, token, source_cols
            )
            if not ok_ins:
                return False, e or "无法新增金额列"
            target_cols, ferr = _fetch_merged_target_cols_for_write(
                spreadsheet_token, sheet_id, token
            )
            if ferr:
                return False, ferr
            if not target_cols:
                return False, "目标表头为空。"

    value_ranges: list[dict] = []
    for sheet_row_1based, df_column_name, raw_value in cells:
        if sheet_row_1based < 1:
            return False, f"无效的数据行号：{sheet_row_1based}"
        idx = _find_sheet_column_index_for_df_column(
            target_cols, source_cols, df_column_name
        )
        if idx is None:
            return (
                False,
                f"无法在飞书表头中定位列「{df_column_name}」。",
            )
        letter = _col_idx_to_letter(idx)
        cell = _normalize_cell_for_feishu(raw_value, df_column_name)
        a1 = f"{letter}{sheet_row_1based}:{letter}{sheet_row_1based}"
        value_ranges.append(
            {
                "range": _sheet_join_range(sheet_id, a1),
                "values": [[cell]],
            }
        )

    batch_sz = 80
    for i in range(0, len(value_ranges), batch_sz):
        chunk = value_ranges[i : i + batch_sz]
        ok, err = _post_values_batch_update(spreadsheet_token, token, chunk)
        if not ok:
            return False, err or "批量写入失败"
    _set_last_error("")
    return True, f"已更新飞书 {len(cells)} 个单元格。"


def _detect_sheet_data_start_row(spreadsheet_token: str, sheet_id: str, token: str) -> int:
    """
    探测分表首条数据行（默认 2）。
    规则：扫描 A1:A80，找到首个“正整数序号”所在行作为数据起始行。
    """
    try:
        scan_range = f"{sheet_id}!A1:A80"
        encoded = urllib.parse.quote(scan_range, safe="")
        url = f"{FEISHU_API_BASE}/sheets/v2/spreadsheets/{spreadsheet_token}/values/{encoded}"
        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": f"Bearer {token}"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        if data.get("code") != 0:
            return 2
        values = (data.get("data") or {}).get("valueRange", {}).get("values", []) or []
        for i, row in enumerate(values, start=1):
            if not row:
                continue
            first = str(row[0]).strip() if row[0] is not None else ""
            if not first:
                continue
            # 序号一般是正整数；用它定位首条数据行，兼容多行表头
            if re.fullmatch(r"\d+", first):
                n = int(first)
                if n >= 1:
                    return i
        return 2
    except Exception:
        return 2


def sync_sheets_full_replace(url_or_id: str, df_new: pd.DataFrame) -> tuple[bool, str]:
    """
    将 DataFrame 全量覆盖写回飞书电子表格（Sheets）。
    - 不覆盖表头区：从探测到的数据起始行开始写入
    - 列名与读取一致：合并第 1、2 行表头（与 _load_from_sheets 相同），避免「0/1/2」或「预计节点」占位导致进度列写空
    - 按 __feishu_record_id 排序后再写，避免本地库行序与表格不一致
    """
    spreadsheet_token, sheet_id = _parse_sheets_url(url_or_id)
    if not spreadsheet_token:
        return False, "链接不是 sheets 地址，未执行电子表格写回。"
    token = _get_tenant_access_token()
    if not token:
        return False, get_last_error() or "无法获取 tenant_access_token。"
    if not sheet_id:
        sheet_id = _get_first_sheet_id(spreadsheet_token, token) or ""
    if not sheet_id:
        return False, "无法解析 sheet_id。"

    if df_new is None:
        return False, "待写回数据为空。"

    # 保留业务列，跳过内部列
    source_cols = [c for c in df_new.columns if str(c).strip() and not str(c).startswith("__")]
    if not source_cols:
        return False, "无可写回列。"

    # 读取第 1、2 行并合并表头（与 _load_from_sheets 一致），否则双行表头分表的进度列无法对齐
    target_cols, hdr_err = _fetch_merged_target_cols_for_write(spreadsheet_token, sheet_id, token)
    if hdr_err:
        return False, hdr_err

    if not target_cols:
        target_cols = [str(c).strip() for c in source_cols if str(c).strip()]
    if not target_cols:
        return False, "目标表头为空，无法写回。"

    if "实际预计金额" in source_cols:
        idx_amt = _find_sheet_column_index_for_df_column(
            target_cols, source_cols, "实际预计金额"
        )
        if idx_amt is None:
            ok_amt, err_amt = _ensure_sheet_column_actual_amount(
                spreadsheet_token, sheet_id, token, source_cols
            )
            if not ok_amt:
                return False, err_amt
            target_cols, hdr_err = _fetch_merged_target_cols_for_write(
                spreadsheet_token, sheet_id, token
            )
            if hdr_err:
                return False, hdr_err
            if not target_cols:
                return False, "新增金额列后重读表头失败。"

    # 探测原始分表的数据起始行（有些分表是多行表头，不一定从第2行开始）
    data_start_row = _detect_sheet_data_start_row(spreadsheet_token, sheet_id, token)

    # 旧行数（不含表头），用于清空尾部
    old_df = _load_from_sheets(spreadsheet_token, sheet_id, token)
    old_rows = 0 if old_df is None else len(old_df)

    # 1) 写数据（分块：行 + 列均受飞书单次上限约束）
    ncols = len(target_cols)

    df_write = df_new.copy()
    if FEISHU_RECORD_ID_COL in df_write.columns:
        try:
            df_write["_feishu_sheet_row"] = pd.to_numeric(
                df_write[FEISHU_RECORD_ID_COL], errors="coerce"
            )
            df_write = df_write.sort_values(
                "_feishu_sheet_row", kind="mergesort", na_position="last"
            ).drop(columns=["_feishu_sheet_row"])
        except Exception:
            df_write = df_new
    elif "序号" in df_write.columns:
        # 历史数据若没有行号列，至少按序号排序，避免乱序覆盖整表
        try:
            df_write = df_write.copy()
            df_write["_feishu_sort_seq"] = pd.to_numeric(df_write["序号"], errors="coerce")
            df_write = df_write.sort_values(
                "_feishu_sort_seq", kind="mergesort", na_position="last"
            ).drop(columns=["_feishu_sort_seq"])
        except Exception:
            df_write = df_new

    values = []
    for _, row in df_write.iterrows():
        one = []
        for tc in target_cols:
            src = _resolve_df_column_for_sheet_header(tc, source_cols)
            v = row.get(src, "") if src else ""
            label = src if src else tc
            one.append(_normalize_cell_for_feishu(v, label))
        values.append(one)
    if not values:
        return False, "写回数据为空（过滤后无可写入行）。"

    row_chunk = min(400, FEISHU_VALUES_MAX_ROWS)
    for i in range(0, len(values), row_chunk):
        row_block = values[i : i + row_chunk]
        start = i + data_start_row
        end = start + len(row_block) - 1
        value_ranges = []
        for c0 in range(0, ncols, FEISHU_VALUES_MAX_COLS):
            c1 = min(c0 + FEISHU_VALUES_MAX_COLS, ncols)
            lo = _col_idx_to_letter(c0)
            hi = _col_idx_to_letter(c1 - 1)
            sub = [row[c0:c1] for row in row_block]
            value_ranges.append(
                {
                    "range": _sheet_join_range(sheet_id, f"{lo}{start}:{hi}{end}"),
                    "values": sub,
                }
            )
        ok, err = _post_values_batch_update(spreadsheet_token, token, value_ranges)
        if not ok:
            return False, f"写入数据失败（行 {start}-{end}）：{err}"

    # 2) 清空尾部旧数据（仅清空，不删行）
    new_rows = len(values)
    if old_rows > new_rows:
        blank_rows = old_rows - new_rows
        start = data_start_row + new_rows
        blanks = [["" for _ in target_cols] for _ in range(blank_rows)]
        for i in range(0, len(blanks), row_chunk):
            chunk = blanks[i : i + row_chunk]
            s = start + i
            e = s + len(chunk) - 1
            value_ranges = []
            for c0 in range(0, ncols, FEISHU_VALUES_MAX_COLS):
                c1 = min(c0 + FEISHU_VALUES_MAX_COLS, ncols)
                lo = _col_idx_to_letter(c0)
                hi = _col_idx_to_letter(c1 - 1)
                sub = [row[c0:c1] for row in chunk]
                value_ranges.append(
                    {
                        "range": _sheet_join_range(sheet_id, f"{lo}{s}:{hi}{e}"),
                        "values": sub,
                    }
                )
            ok, err = _post_values_batch_update(spreadsheet_token, token, value_ranges)
            if not ok:
                return False, f"清空旧尾部失败（行 {s}-{e}）：{err}"

    _set_last_error("")
    return True, f"已写回飞书电子表格，共 {len(values)} 条。"
