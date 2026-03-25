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
FEISHU_RECORD_ID_COL = "__feishu_record_id"
EXCLUDED_SHEET_NAMES = {"汇总分析", "填写备注", "百万级项目明细"}

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


def _col_idx_to_letter(idx: int) -> str:
    n = idx + 1
    letters = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(65 + rem))
    return "".join(reversed(letters)) or "A"


def _sheet_join_range(sheet_id: str, a1: str) -> str:
    return f"{sheet_id}!{a1}"


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
            return False, f"读取原表头失败：code={header_data.get('code')} msg={header_data.get('msg')}"
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
    except Exception as e:
        return False, f"读取原表头异常：{_format_http_error(e)}"

    if not target_cols:
        target_cols = [str(c).strip() for c in source_cols if str(c).strip()]
    if not target_cols:
        return False, "目标表头为空，无法写回。"

    # 探测原始分表的数据起始行（有些分表是多行表头，不一定从第2行开始）
    data_start_row = _detect_sheet_data_start_row(spreadsheet_token, sheet_id, token)

    # 旧行数（不含表头），用于清空尾部
    old_df = _load_from_sheets(spreadsheet_token, sheet_id, token)
    old_rows = 0 if old_df is None else len(old_df)

    # 1) 写数据（分块）- 从探测到的数据起始行开始，不覆盖原表头区域
    last_col = _col_idx_to_letter(len(target_cols) - 1)

    def _resolve_source_col(target_col: str) -> str | None:
        # 直接命中
        if target_col in source_cols:
            return target_col
        # 常见别名兼容
        if target_col == "拟定金额" and "实际预计金额" in source_cols:
            return "实际预计金额"
        # 去括号弱匹配（例如 验收(社区需求完成交付) -> 验收）
        tc_base = re.sub(r"[（(].*?[)）]", "", str(target_col)).strip()
        for sc in source_cols:
            scs = str(sc).strip()
            sc_base = re.sub(r"[（(].*?[)）]", "", scs).strip()
            if scs == target_col or sc_base == tc_base:
                return sc
        return None

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

    values = []
    for _, row in df_write.iterrows():
        one = []
        for tc in target_cols:
            src = _resolve_source_col(tc)
            v = row.get(src, "") if src else ""
            if pd.isna(v) or v is None:
                one.append("")
            elif isinstance(v, (int, float)):
                one.append(v)
            else:
                one.append(str(v))
        values.append(one)
    if not values:
        return False, "写回数据为空（过滤后无可写入行）。"

    chunk_size = 400
    for i in range(0, len(values), chunk_size):
        chunk = values[i : i + chunk_size]
        start = i + data_start_row
        end = start + len(chunk) - 1
        ok, err = _put_sheets_range(
            spreadsheet_token,
            sheet_id,
            f"A{start}:{last_col}{end}",
            chunk,
            token,
        )
        if not ok:
            return False, f"写入数据失败（{start}-{end}）：{err}"

    # 2) 清空尾部旧数据（仅清空，不删行）
    new_rows = len(values)
    if old_rows > new_rows:
        blank_rows = old_rows - new_rows
        start = data_start_row + new_rows
        end = data_start_row + old_rows - 1
        blanks = [["" for _ in target_cols] for _ in range(blank_rows)]
        for i in range(0, len(blanks), chunk_size):
            chunk = blanks[i : i + chunk_size]
            s = start + i
            e = s + len(chunk) - 1
            ok, err = _put_sheets_range(
                spreadsheet_token,
                sheet_id,
                f"A{s}:{last_col}{e}",
                chunk,
                token,
            )
            if not ok:
                return False, f"清空旧尾部失败（{s}-{e}）：{err}"

    _set_last_error("")
    return True, f"已写回飞书电子表格，共 {len(values)} 条。"
