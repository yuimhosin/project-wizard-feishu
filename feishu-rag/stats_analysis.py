# -*- coding: utf-8 -*-
"""
多维表格统计分析：按机构、时间等维度统计
优先从 bitable API 拉取；若 API 失败则从已同步的 doc_contents 解析
"""
import sys
import json
import re
from pathlib import Path
from collections import Counter
from datetime import datetime

_root = Path(__file__).resolve().parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from config import FEISHU_DOC_IDS, VECTOR_DB_PATH
from feishu_api_client import get_bitable_records


def _ts_to_date(ts) -> str:
    """毫秒时间戳转 YYYY/MM"""
    try:
        t = int(ts)
        if t < 1e10:
            t *= 1000
        dt = datetime.fromtimestamp(t / 1000)
        return dt.strftime("%Y/%m")
    except (ValueError, TypeError):
        return ""


def _ts_to_readable(ts) -> str:
    """毫秒时间戳转 YYYY/MM/DD HH:MM"""
    try:
        t = int(ts)
        if t < 1e10:
            t *= 1000
        elif t > 1e15:
            return str(ts)
        dt = datetime.fromtimestamp(t / 1000)
        return dt.strftime("%Y/%m/%d %H:%M")
    except (ValueError, TypeError):
        return str(ts)


# 图片/附件字段：直接省略，不展示文件名列表
_IMAGE_FIELDS = ("相关照片", "报告", "附件", "图片")


def _format_record_value(k: str, v) -> str:
    """格式化字段值：时间类转可读；图片/附件字段省略"""
    s = str(v).strip()
    if not s:
        return s
    # 图片/附件字段：整段替换为 [已省略]
    if any(f in k for f in _IMAGE_FIELDS):
        return "[图片/附件已省略]"
    # 时间类字段的 12-13 位数字转为 YYYY/MM/DD HH:MM
    if "时间" in k and s.isdigit() and 12 <= len(s) <= 13:
        return _ts_to_readable(s)
    return s


def _parse_records_from_content(content: str) -> list[dict]:
    """从 doc_contents 的 content 文本解析出记录（格式：上报时间: xxx\n上报机构: xxx\n...）"""
    if not content or not content.strip():
        return []
    records = []
    # 按「上报时间:」分割，每条记录以 上报时间 开头
    parts = re.split(r"\n(?=上报时间:)", content.strip())
    for part in parts:
        part = part.strip()
        if not part or not part.startswith("上报时间:"):
            continue
        rec = {}
        for line in part.split("\n"):
            if ":" in line:
                k, _, v = line.partition(":")
                k, v = k.strip(), v.strip()
                if k and k not in ("file_token", "tmp_url", "avatar_url"):
                    rec[k] = v
        if rec:
            records.append(rec)
    return records


def _get_records_from_doc_contents() -> list[dict]:
    """从已同步的 doc_contents.json 解析 bitable 内容（API 失败时的 fallback）"""
    contents_file = Path(VECTOR_DB_PATH) / "doc_contents.json"
    if not contents_file.exists():
        return []
    try:
        with open(contents_file, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    records = []
    for doc_id, item in data.items():
        if not isinstance(item, dict):
            continue
        if not doc_id.startswith("bitable:"):
            continue
        content = item.get("content", "")
        records.extend(_parse_records_from_content(content))
    return records


def get_records() -> tuple[list[dict], bool]:
    """
    从配置的 bitable 获取所有记录；API 失败时从 doc_contents 解析。
    返回 (records, from_cache)：from_cache=True 表示使用本地缓存。
    """
    records = []
    for item in FEISHU_DOC_IDS:
        if not item or len(item) != 2:
            continue
        source, doc_id = item
        if source == "bitable":
            app_token, table_id = doc_id if isinstance(doc_id, tuple) else ("", "")
            records.extend(get_bitable_records(app_token, table_id))
    from_cache = False
    if not records:
        records = _get_records_from_doc_contents()
        from_cache = bool(records)
    return records, from_cache


def stats_by_org(records: list[dict], org_field: str = "上报机构") -> list[tuple]:
    """按机构统计上报数量，返回 [(机构, 数量), ...] 按数量降序"""
    orgs = [r.get(org_field, "").strip() or "未填写" for r in records]
    cnt = Counter(orgs)
    return cnt.most_common()


def stats_by_month(records: list[dict], time_field: str = "上报时间") -> list[tuple]:
    """按月份统计，返回 [(YYYY/MM, 数量), ...]"""
    months = []
    for r in records:
        v = r.get(time_field, "")
        m = _ts_to_date(v) if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()) else v[:7] if v else ""
        if m:
            months.append(m)
    return Counter(months).most_common()


def stats_by_event_type(records: list[dict], field: str = "事件分类") -> list[tuple]:
    """按事件分类统计"""
    vals = [r.get(field, "").strip() or "未分类" for r in records]
    return Counter(vals).most_common()


def filter_records_by_keywords(records: list[dict], keywords: list[str], max_results: int = 30) -> list[dict]:
    """
    按关键词筛选记录（在任意字段中匹配）。
    用于「电梯」「漏水」等具体事件查询，返回匹配的详细记录。
    """
    if not records or not keywords:
        return []
    matched = []
    kw_lower = [k.lower().strip() for k in keywords if k and k.strip()]
    for r in records:
        text = " ".join(str(v) for v in r.values() if v)
        if any(k in text.lower() for k in kw_lower):
            matched.append(r)
            if len(matched) >= max_results:
                break
    return matched


def format_event_details(records: list[dict], limit: int = 20) -> str:
    """将记录格式化为可读的事件详情文本，时间戳转为 YYYY/MM/DD HH:MM"""
    if not records:
        return ""
    lines = []
    for i, r in enumerate(records[:limit], 1):
        parts = [f"【记录 {i}】"]
        for k, v in r.items():
            if v and str(v).strip() and k not in ("file_token", "tmp_url", "avatar_url"):
                formatted = _format_record_value(k, v)
                if formatted == "[图片/附件已省略]":
                    continue  # 不展示该行，减少噪音
                parts.append(f"  {k}: {formatted}")
        lines.append("\n".join(parts))
    if len(records) > limit:
        lines.append(f"\n... 共 {len(records)} 条匹配，仅展示前 {limit} 条")
    return "\n\n".join(lines)


def format_stats_report(records: list[dict], question: str = "", from_cache: bool = False) -> str:
    """
    根据问题生成统计报告。
    支持：机构上报、上报积极、统计、分析 等关键词
    支持：电梯、漏水、特种设备 等具体事件关键词 → 返回匹配的事件详情
    """
    if not records:
        return "暂无数据，请确认多维表格已配置且可访问。"

    q = (question or "").strip()
    cache_note = "\n（注：飞书 API 暂不可用，以上为本地已同步数据的统计）\n" if from_cache else ""
    q_lower = q.lower()

    # 具体事件关键词：优先返回匹配的事件详情，而非仅统计
    event_keywords = ["电梯", "漏水", "渗漏", "特种设备", "困人", "故障", "管道", "水管", "消防", "人身安全", "基础设施"]
    matched_kw = [k for k in event_keywords if k in q]
    if matched_kw:
        filtered = filter_records_by_keywords(records, matched_kw, max_results=30)
        if filtered:
            lines = [f"共找到 {len(filtered)} 条与「{'/'.join(matched_kw)}」相关的事件：\n"]
            lines.append(format_event_details(filtered, limit=15))
            return "\n".join(lines) + cache_note
        else:
            lines = [f"共 {len(records)} 条记录，但未找到与「{'/'.join(matched_kw)}」直接相关的事件。\n"]
            lines.append("可能原因：事件描述使用了其他术语，或归类在其他分类下。")
            lines.append("\n【按事件分类统计】（可参考「异常事件-基础设施」「人身安全」等）")
            for t, n in stats_by_event_type(records)[:8]:
                lines.append(f"  {t}: {n} 条")
            return "\n".join(lines) + cache_note

    lines = [f"共 {len(records)} 条记录。\n"]

    if "机构" in q_lower or "积极" in q_lower or "上报" in q_lower:
        by_org = stats_by_org(records)
        lines.append("【按上报机构统计】")
        for org, n in by_org[:15]:
            lines.append(f"  {org}: {n} 条")
        if by_org:
            lines.append(f"\n上报最积极: {by_org[0][0]} ({by_org[0][1]} 条)")

    if "月" in q_lower or "时间" in q_lower or "趋势" in q_lower:
        by_month = stats_by_month(records)
        if by_month:
            lines.append("\n【按月份统计】")
            for m, n in sorted(by_month)[-12:]:
                lines.append(f"  {m}: {n} 条")

    if "分类" in q_lower or "类型" in q_lower:
        by_type = stats_by_event_type(records)
        if by_type:
            lines.append("\n【按事件分类统计】")
            for t, n in by_type[:10]:
                lines.append(f"  {t}: {n} 条")

    if len(lines) == 1:
        by_org = stats_by_org(records)
        lines.append("【按上报机构统计】")
        for org, n in by_org[:15]:
            lines.append(f"  {org}: {n} 条")
        if by_org:
            lines.append(f"\n上报最积极: {by_org[0][0]} ({by_org[0][1]} 条)")

    return "\n".join(lines) + cache_note


if __name__ == "__main__":
    recs, from_cache = get_records()
    print(format_stats_report(recs, "哪个机构上报最积极", from_cache=from_cache))
