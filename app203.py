# -*- coding: utf-8 -*-
"""
养老社区改良改造进度管理 - Streamlit 交互看板
审核流程：社区提出 → 分级 → 专业分类 → 预算拆分 → 一线立项 → 项目部施工 → 总部运行保障协调招采/施工 → 总部督促验收
"""
import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile
import io
import base64
import os
import sqlite3
import json
import urllib.request
from datetime import datetime, date
from data_loader import load_single_csv, load_from_directory, load_uploaded, get_稳定需求_mask, TIMELINE_COLS
from location_config import 园区_TO_城市, 园区_TO_区域, 城市_COORDS

try:
    from feishu_bitable_loader import load_from_bitable
    FEISHU_BITABLE_AVAILABLE = True
except ImportError:
    FEISHU_BITABLE_AVAILABLE = False

try:
    from feishu_oauth import build_authorize_url, exchange_code_for_user
    FEISHU_OAUTH_AVAILABLE = True
except ImportError:
    FEISHU_OAUTH_AVAILABLE = False

try:
    from openai import OpenAI
    DEEPSEEK_CLIENT_AVAILABLE = True
except ImportError:
    DEEPSEEK_CLIENT_AVAILABLE = False

# PDF导出相关导入
try:
    from reportlab.lib.pagesizes import A4, letter
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

# 图表配色：饼图用 20+ 种不重复颜色，避免多分类时颜色重复
CHART_COLORS_PIE = [
    "#5470c6", "#91cc75", "#fac858", "#ee6666", "#73c0de",
    "#3ba272", "#fc8452", "#9a60b4", "#ea7ccc", "#5ad8a6",
    "#6dc8ec", "#945fb9", "#ff9845", "#1e9bb5", "#ffbf00",
    "#c23531", "#2f4554", "#61a0a8", "#d48265", "#749f83",
    "#ca8622", "#bda29a", "#6e7074", "#546570", "#c4ccd3",
]

st.set_page_config(
    page_title="养老社区改良改造进度管理",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 默认数据目录与默认单文件路径
# 这里将项目根目录下的 CSV 表作为默认文件，方便打包后的 exe 直接使用
ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = str(ROOT_DIR)
DEFAULT_SINGLE_FILE = str(ROOT_DIR / "改良改造报表-V4.csv")
# 内嵌默认数据（加密 .enc，随 git 提交，Streamlit Cloud 部署用）
DEFAULT_BUNDLED_CSV = ROOT_DIR / "改良改造报表-V4-sample.csv.enc"
LEGACY_DB_ROWS_TO_REPLACE = {337}

# 专业 9 大类（与 CSV 中「专业」列对应，用于分类统计）
专业大类 = [
    "土建设施", "供配电系统", "暖通/供冷系统", "弱电系统", "供排水系统",
    "电梯系统", "其它系统", "消防系统", "安防系统"
]

# 下拉选项预设（用于新增/修改向导）
OPT_所属业态 = ["独立", "护理", "其他"]
OPT_项目分级 = ["一级（最高级）", "二级", "三级"]
OPT_项目分类 = ["品质提升", "大修", "安全", "运营需求", "节能改造", "智能化提升", "金额10万以上的常规维修", "金额10万以上的房态更新", "其他改造"]
OPT_拟定承建组织 = ["不动产项目部", "社区分包", "社区负责"]
OPT_总部重点关注 = ["是", "否"]


def _get_dropdown_options(df: pd.DataFrame, col: str, extras: list = None) -> list:
    """从数据中提取唯一值 + 额外选项，用于下拉。"""
    opts = []
    if col in df.columns:
        opts = sorted(df[col].dropna().astype(str).unique().tolist())
    if extras:
        opts = sorted(set(opts) | set(extras))
    return [x for x in opts if x and str(x).strip() != "nan"]


DATE_RANGE_MIN = date(2020, 1, 1)
DATE_RANGE_MAX = date(2030, 12, 31)
SENTINEL_DATE = date(2000, 1, 1)  # 表示未填写


def _str_to_date(s) -> date:
    """字符串转 date，空或无效则返回 SENTINEL_DATE。"""
    if not s or (isinstance(s, str) and not str(s).strip()):
        return SENTINEL_DATE
    try:
        dt = pd.to_datetime(s, errors="coerce", format="mixed")
        if pd.isna(dt):
            return SENTINEL_DATE
        d = dt.date() if hasattr(dt, "date") else dt
        if not (DATE_RANGE_MIN <= d <= DATE_RANGE_MAX):
            return SENTINEL_DATE
        return d
    except Exception:
        return SENTINEL_DATE


def _date_to_str(d) -> str:
    """date 转 YYYY-MM-DD，SENTINEL_DATE 或 None 转为空。"""
    if d is None or (hasattr(d, "year") and d.year == 2000 and d.month == 1 and d.day == 1):
        return ""
    if isinstance(d, date):
        return d.strftime("%Y-%m-%d")
    return str(d) if d else ""

# ---------- 团队共享数据：SQLite 存储 ----------
DB_PATH = os.getenv("APP203_DB_PATH", "app203_projects.db")


def _get_db_connection():
    return sqlite3.connect(DB_PATH)


def load_from_db() -> pd.DataFrame:
    """从 SQLite 加载团队共享数据表 projects。若不存在则返回空表。"""
    if not Path(DB_PATH).exists():
        return pd.DataFrame()
    try:
        with _get_db_connection() as conn:
            return pd.read_sql("SELECT * FROM projects", conn)
    except Exception:
        return pd.DataFrame()


def save_to_db(df: pd.DataFrame):
    """将当前 DataFrame 全量写入 SQLite（覆盖 projects 表）。"""
    if df is None or df.empty:
        return
    with _get_db_connection() as conn:
        df.to_sql("projects", conn, if_exists="replace", index=False)


def _ensure_project_columns(df: pd.DataFrame) -> pd.DataFrame:
    """保证关键列存在，便于新增/修改向导统一写入。"""
    needed = [
        "序号", "园区", "所属区域", "城市", "所属业态",
        "项目分级", "项目分类", "拟定承建组织", "总部重点关注项目",
        "专业", "专业分包", "项目名称", "备注说明", "拟定金额", "上传凭证",
    ]
    out = df.copy()
    for col in needed:
        if col not in out.columns:
            out[col] = "" if col not in ["序号", "拟定金额"] else 0
    return out


def _strip_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """去掉列名为空字符串的列，避免 data_editor 因重复空列名报错。"""
    keep_cols = [c for c in df.columns if str(c).strip() != ""]
    return df[keep_cols].copy()


def _canonicalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    加载后统一规范化：只保留分析所需列、合并城市列、固定列顺序，避免多列/错位导致后面列显示为空。
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    out = _strip_empty_columns(out)
    if "社区" in out.columns and "园区" not in out.columns:
        out = out.rename(columns={"社区": "园区"})
    elif "社区" in out.columns and "园区" in out.columns:
        out["园区"] = out["园区"].fillna(out["社区"])
        out = out.drop(columns=["社区"], errors="ignore")
    if "所在城市" in out.columns:
        if "城市" not in out.columns:
            out["城市"] = out["所在城市"]
        else:
            out["城市"] = out["城市"].fillna(out["所在城市"])
        out = out.drop(columns=["所在城市"], errors="ignore")
    if "专业细分" in out.columns and "专业分包" not in out.columns:
        out["专业分包"] = out["专业细分"]
    if "专业细分" in out.columns and "专业分包" in out.columns:
        out["专业分包"] = out["专业分包"].fillna(out["专业细分"])
    if "专业细分" in out.columns:
        out = out.drop(columns=["专业细分"], errors="ignore")
    if "拟定金额" in out.columns:
        out["拟定金额"] = pd.to_numeric(out["拟定金额"], errors="coerce").fillna(0)
    if "序号" in out.columns:
        out["序号"] = pd.to_numeric(out["序号"], errors="coerce")
    base_order = [
        "序号", "园区", "所属区域", "城市", "所属业态",
        "项目分级", "项目分类", "拟定承建组织", "总部重点关注项目",
        "专业", "专业分包", "项目名称", "备注说明", "拟定金额",
    ]
    timeline_cols = [c for c in TIMELINE_COLS if c in out.columns]
    extra = ["上传凭证"] if "上传凭证" in out.columns else []
    want = base_order + timeline_cols + extra
    existing = list(out.columns)
    ordered = [c for c in want if c in existing]
    rest = [c for c in existing if c not in ordered]
    out = out[ordered + rest].copy()
    return out


def _get_next_序号(df: pd.DataFrame) -> int:
    """根据现有数据自动生成下一个序号。"""
    if "序号" not in df.columns or df.empty:
        return 1
    try:
        nums = pd.to_numeric(df["序号"], errors="coerce")
        m = nums.max()
        return int(m) + 1 if pd.notna(m) else 1
    except Exception:
        return 1


# ---------- 飞书推送（自定义机器人 Webhook）----------
def _get_feishu_webhook_url() -> str | None:
    """获取飞书 Webhook URL：Streamlit Secrets > 环境变量 FEISHU_WEBHOOK_URL。"""
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
    """转为可 JSON 序列化的值（避免 numpy/NaN 导致请求体格式无效）。"""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        if hasattr(v, "item"):  # numpy 标量
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


def _row_to_dict(row: pd.Series) -> dict:
    """将一行转为可 JSON 序列化的字典（键为字符串，值为原生类型）。"""
    out = {}
    for k, v in row.items():
        out[str(k)] = _to_json_value(v)
    return out


def _format_cell(v) -> str:
    """用于变更详情展示：None/NaN 显示为空字符串。"""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def _compute_df_diff(old_df: pd.DataFrame, new_df: pd.DataFrame) -> dict:
    """
    按「序号」对比新旧表，返回删除、新增、修改的明细及修改详情（字段级 旧值→新值）。
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
        out["deleted"].append(_row_to_dict(row))
    for sid in added_ids:
        row = new_df[new_df[key_col].astype(int) == sid].iloc[0]
        out["added"].append(_row_to_dict(row))
    for sid in common_ids:
        old_row = old_df[old_df[key_col].astype(int) == sid].iloc[0]
        new_row = new_df[new_df[key_col].astype(int) == sid].iloc[0]
        if not old_row.equals(new_row):
            out["modified"].append(_row_to_dict(new_row))
            # 计算本条修改的字段级详情：列名 旧值→新值
            changes = []
            for col in old_row.index:
                if col not in new_row.index:
                    continue
                ov = _format_cell(old_row[col])
                nv = _format_cell(new_row[col])
                if ov != nv:
                    changes.append(f"{col}：{ov or '（空）'} → {nv or '（空）'}")
            out["modified_details"].append({"序号": int(sid), "变更项": changes})
    return out


def _build_feishu_payload_from_diff(diff: dict, total_after: int, source: str = "看板编辑") -> dict:
    """根据 diff 构建飞书 Webhook 的 JSON 负载（含变更类型、修改内容及修改详情），全部为可序列化类型。"""
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
    # 追加修改详情：每条记录的字段级变更（如 总部重点关注项目：是 → 否）
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
    # 飞书流程要求：大括号 {}、message_type 为文本类型、键值对（键字符串，值可字符串/数字/布尔/数组/对象/null）
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
    """递归将 dict/list 中的值转为可 JSON 序列化的原生类型，避免请求体格式无效。"""
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
    """向飞书 Webhook 推送。payload 会先做原生类型清理再发送，避免请求体格式无效。"""
    url = _get_feishu_webhook_url()
    if not url:
        return False
    if payload is not None:
        body_dict = _ensure_native_json(payload)
        # 飞书流程要求 message_type + 键值对；若设 FEISHU_PAYLOAD_SIMPLE=1 则只发扁平键值对，changes 转为 changes_json 字符串
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
    # 调试：在终端打印本次推送的 text，便于确认是否含修改详情（若仍只看到旧文案，请检查飞书流程是否引用 text 参数）
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


def render_审核流程说明():
    """审核流程说明区块。"""
    st.markdown("### 📋 需求审核与实施流程说明")
    steps = [
        ("1. 社区提出", "一线园区提出改造需求。"),
        ("2. 紧急程度分级", "按一级（最高级）、二级、三级划分。"),
        ("3. 专业分类", "按 9 大类专业划分：土建、供配电、暖通/供冷、弱电、供排水、电梯、其它、消防、安防等。"),
        ("4. 财务预算拆分", "按预算系统进行金额拆分与汇总。"),
        ("5. 一线立项时间", "一线填写需求并提出立项时间。"),
        ("6. 项目部施工", "项目部根据已确定的需求立项组织施工。"),
        ("7. 总部运行保障部", "督促一线需求稳定，协调总部相关部门把控需求，输出给不动产进行招采、施工。"),
        ("8. 施工验收", "总部运行保障部督促一线园区进行最终施工验收。"),
    ]
    for title, desc in steps:
        st.markdown(f"- **{title}**：{desc}")
    st.divider()


def render_项目统计分析(df: pd.DataFrame, 园区选择: list):
    """项目统计分析：数量费用统计、预算差值、确定/未确定项目分析、按月份统计立项。"""
    st.subheader("项目统计分析")
    # 处理园区选择：如果为空或None，显示所有有园区信息的数据
    if 园区选择 and len(园区选择) > 0:
        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
        if valid_parks:
            sub = df[df["园区"].isin(valid_parks)]
        else:
            sub = df[df["园区"].notna()]
    else:
        sub = df[df["园区"].notna()]  # 只显示有园区信息的行
    
    # 过滤掉汇总行（序号为空或为"合计"等）
    if "序号" in sub.columns:
        sub = sub[sub["序号"].notna()]
        # 过滤掉合计行
        sub = sub[~sub["序号"].astype(str).str.strip().isin(["合计", "预算系统合计", "差", "差额", "小计"])]
        # 确保序号是数字（过滤掉非数字的序号）
        sub = sub[pd.to_numeric(sub["序号"], errors='coerce').notna()]
    else:
        st.warning("数据中未找到'序号'列，无法进行统计分析。")
        return
    
    # 标签池：先选择需要分析的字段，再展示对应统计
    st.markdown("### 🔖 标签池（选择需要分析的字段）")
    all_tags = [
        "社区（园区）",
        "所属区域",
        "所属业态",
        "项目分级",
        "项目分类",
        "拟定承建组织",
        "总部重点关注项目",
        "专业",
        "专业分包",
        "项目名称",
        "备注说明",
        "拟定金额",
    ]
    default_tags = st.session_state.get(
        "tag_pool_selection",
        ["社区（园区）", "所属区域", "项目分级", "专业", "专业分包", "拟定金额"],
    )
    selected_tags = st.multiselect(
        "请选择本次分析要关注的字段（至少选择一个）：",
        options=all_tags,
        default=[t for t in default_tags if t in all_tags],
        help=(
            "示例：\n"
            "- 只看区域对比：勾选「所属区域」「拟定金额」。\n"
            "- 看分级与专业：勾选「项目分级」「专业」「拟定金额」。\n"
            "- 只看社区层面的统计：勾选「社区（园区）」「拟定金额」。"
        ),
    )
    st.session_state["tag_pool_selection"] = selected_tags

    if not selected_tags:
        st.info("请先在上方的标签池中至少选择一个字段，然后将根据选择展示对应的统计图表。")
        return

    show_park = "社区（园区）" in selected_tags
    show_region = "所属区域" in selected_tags
    show_prof_subcontract = "专业分包" in selected_tags
    show_level_stats = "项目分级" in selected_tags
    use_amount_filter = "拟定金额" in selected_tags
    show_business_type = "所属业态" in selected_tags
    show_category = "项目分类" in selected_tags
    show_contractor = "拟定承建组织" in selected_tags
    show_focus = "总部重点关注项目" in selected_tags
    show_prof = "专业" in selected_tags

    # 按标签构造筛选条件，例如：华东地区 + 一级项目 + 金额区间
    st.markdown("### 🎯 标签筛选条件（可选）")
    col_region, col_level, col_amount = st.columns(3)
    selected_regions = []
    selected_levels = []
    amount_min = amount_max = None
    selected_business_types = []
    selected_categories = []
    selected_contractors = []
    selected_focus = []
    selected_profs = []
    selected_prof_subcontracts = []

    # 用于级联下钻的临时 DataFrame：每选择一层，就用该层结果作为下一层可选值的来源
    sub_for_opts = sub.copy()

    if show_region and "所属区域" in sub_for_opts.columns:
        with col_region:
            region_opts = (
                sub_for_opts["所属区域"]
                .dropna()
                .astype(str)
                .replace("其他", pd.NA)
                .dropna()
                .unique()
                .tolist()
            )
            region_opts = sorted(region_opts)
            selected_regions = st.multiselect(
                "选择所属区域",
                options=region_opts,
                help="例如：只看华东地区时，勾选「华东」。可多选。",
            )
            if selected_regions:
                sub_for_opts = sub_for_opts[sub_for_opts["所属区域"].isin(selected_regions)]

    if show_level_stats and "项目分级" in sub_for_opts.columns:
        with col_level:
            level_opts = (
                sub_for_opts["项目分级"]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )
            level_opts = sorted(level_opts)
            selected_levels = st.multiselect(
                "选择项目分级",
                options=level_opts,
                help="例如：只看一级项目时，勾选「一级」。可多选。",
            )
            if selected_levels:
                sub_for_opts = sub_for_opts[sub_for_opts["项目分级"].isin(selected_levels)]

    if use_amount_filter and "拟定金额" in sub_for_opts.columns:
        with col_amount:
            try:
                min_val = float(sub_for_opts["拟定金额"].min() or 0)
                max_val = float(sub_for_opts["拟定金额"].max() or 0)
            except Exception:
                min_val, max_val = 0.0, 0.0
            if max_val < min_val:
                max_val = min_val
            if min_val == max_val:
                amount_min, amount_max = min_val, max_val
                st.write(f"拟定金额范围：{min_val:,.0f} 万元")
            else:
                amount_min, amount_max = st.slider(
                    "拟定金额范围（万元）",
                    min_value=float(min_val),
                    max_value=float(max_val),
                    value=(float(min_val), float(max_val)),
                    step=max(1.0, (max_val - min_val) / 100),
                    help="例如：选择最大值为 500，则表示筛选「五百万以内」的项目。",
                )

    # 其他标签字段的多选筛选
    if use_amount_filter and amount_min is not None and amount_max is not None and "拟定金额" in sub_for_opts.columns:
        sub_for_opts = sub_for_opts[
            (sub_for_opts["拟定金额"] >= amount_min) & (sub_for_opts["拟定金额"] <= amount_max)
        ]

    if show_business_type and "项目业态" in sub_for_opts.columns:
        business_opts = (
            sub_for_opts["项目业态"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        business_opts = sorted(business_opts)
        selected_business_types = st.multiselect(
            "选择所属业态",
            options=business_opts,
            help="例如：只看某一业态的项目时，在此勾选对应业态。",
        )

        if selected_business_types:
            sub_for_opts = sub_for_opts[sub_for_opts["项目业态"].isin(selected_business_types)]

    if show_category and "项目分类" in sub_for_opts.columns:
        category_opts = (
            sub_for_opts["项目分类"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        category_opts = sorted(category_opts)
        selected_categories = st.multiselect(
            "选择项目分类",
            options=category_opts,
            help="例如：只看某一类项目时，在此勾选对应分类。",
        )

        if selected_categories:
            sub_for_opts = sub_for_opts[sub_for_opts["项目分类"].isin(selected_categories)]

    if show_contractor and "拟定承建组织" in sub_for_opts.columns:
        contractor_opts = (
            sub_for_opts["拟定承建组织"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        contractor_opts = sorted(contractor_opts)
        selected_contractors = st.multiselect(
            "选择拟定承建组织",
            options=contractor_opts,
            help="例如：只看由某个承建组织负责的项目时，在此勾选对应承建组织。",
        )

        if selected_contractors:
            sub_for_opts = sub_for_opts[sub_for_opts["拟定承建组织"].isin(selected_contractors)]

    if show_focus and "总部重点关注项目" in sub_for_opts.columns:
        focus_opts = (
            sub_for_opts["总部重点关注项目"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        focus_opts = sorted(focus_opts)
        selected_focus = st.multiselect(
            "选择总部重点关注项目标记",
            options=focus_opts,
            help="例如：只看总部重点关注的项目时，在此勾选「是」或对应标记。",
        )

        if selected_focus:
            sub_for_opts = sub_for_opts[sub_for_opts["总部重点关注项目"].isin(selected_focus)]

    if show_prof and "专业" in sub_for_opts.columns:
        prof_opts = (
            sub_for_opts["专业"]
            .dropna()
            .astype(str)
            .unique()
            .tolist()
        )
        prof_opts = sorted(prof_opts)
        selected_profs = st.multiselect(
            "选择专业",
            options=prof_opts,
            help="例如：只看电梯系统或供配电系统等某几个专业。",
        )

        if selected_profs:
            sub_for_opts = sub_for_opts[sub_for_opts["专业"].isin(selected_profs)]

    if show_prof_subcontract and ("专业分包" in sub_for_opts.columns or "专业细分" in sub_for_opts.columns):
        col_name = "专业分包" if "专业分包" in sub_for_opts.columns else "专业细分"
        sub_prof = sub_for_opts[col_name].dropna().astype(str)
        prof_sub_opts = sorted(sub_prof.unique().tolist())
        selected_prof_subcontracts = st.multiselect(
            "选择专业分包",
            options=prof_sub_opts,
            help="例如：只看某几个专业分包类型。",
        )

    # 应用筛选条件到子集数据
    if selected_regions:
        sub = sub[sub["所属区域"].isin(selected_regions)]
    if selected_levels:
        sub = sub[sub["项目分级"].isin(selected_levels)]
    if (
        use_amount_filter
        and amount_min is not None
        and amount_max is not None
        and "拟定金额" in sub.columns
    ):
        sub = sub[(sub["拟定金额"] >= amount_min) & (sub["拟定金额"] <= amount_max)]

    if selected_business_types and "项目业态" in sub.columns:
        sub = sub[sub["项目业态"].isin(selected_business_types)]
    if selected_categories and "项目分类" in sub.columns:
        sub = sub[sub["项目分类"].isin(selected_categories)]
    if selected_contractors and "拟定承建组织" in sub.columns:
        sub = sub[sub["拟定承建组织"].isin(selected_contractors)]
    if selected_focus and "总部重点关注项目" in sub.columns:
        sub = sub[sub["总部重点关注项目"].isin(selected_focus)]
    if selected_profs and "专业" in sub.columns:
        sub = sub[sub["专业"].isin(selected_profs)]
    if selected_prof_subcontracts and ("专业分包" in sub.columns or "专业细分" in sub.columns):
        col_name = "专业分包" if "专业分包" in sub.columns else "专业细分"
        sub = sub[sub[col_name].astype(str).isin(selected_prof_subcontracts)]

    if sub.empty:
        st.warning("根据当前标签筛选条件，未找到任何项目，请调整区域 / 项目分级或金额范围后重试。")
        return

    # 1. 按数量和费用统计项目，计算与预算差值（只要选择了任意标签就展示整体概览）
    st.markdown("### 📊 项目数量与费用统计")
    total_count = len(sub)
    total_amount = sub["拟定金额"].sum() if "拟定金额" in sub.columns else 0
    
    # 尝试从原始数据中提取预算系统合计（如果有汇总行）
    budget_total = 0
    # 方法1：从序号为空的汇总行中查找
    if "序号" in df.columns:
        budget_rows = df[df["序号"].isna() | (df["序号"].astype(str).str.strip() == "预算系统合计")]
        if not budget_rows.empty:
            for _, row in budget_rows.iterrows():
                if "预算系统合计" in str(row.values):
                    for col in ["拟定金额", "金额", "预算"]:
                        if col in row.index:
                            try:
                                val = row[col]
                                if pd.notna(val):
                                    budget_total = float(val)
                                    break
                            except:
                                continue
                    if budget_total > 0:
                        break
        
        # 方法2：从园区列包含"预算系统合计"的行中查找
        if budget_total == 0 and "园区" in df.columns:
            budget_rows = df[df["园区"].astype(str).str.contains("预算系统合计", na=False)]
            if not budget_rows.empty:
                for col in ["拟定金额", "金额", "预算"]:
                    if col in budget_rows.columns:
                        try:
                            val = budget_rows.iloc[0][col]
                            if pd.notna(val):
                                budget_total = float(val)
                                break
                        except:
                            continue
    
    diff = total_amount - budget_total
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("项目总数", f"{total_count:,}")
    with col2:
        st.metric("总金额（万元）", f"{total_amount:,.0f}")
    with col3:
        st.metric("预算系统合计（万元）", f"{budget_total:,.0f}" if budget_total > 0 else "未找到")
    with col4:
        st.metric("差值（万元）", f"{diff:,.0f}", delta=f"{diff:,.0f}" if diff != 0 else None)
    
    # 按园区统计（仅当在标签池中选择了“社区（园区）”时展示）
    if show_park:
        st.markdown("#### 按园区统计")
        park_stats = sub.groupby("园区", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index()
        park_stats["金额合计"] = park_stats["金额合计"].round(2)
        st.dataframe(park_stats, use_container_width=True, hide_index=True)
    
    # 按区域统计（仅当存在所属区域列且在标签池中勾选“所属区域”时展示）
    if show_region and "所属区域" in sub.columns:
        st.markdown("#### 按所属区域统计")
        region_stats = sub.groupby("所属区域", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
            园区数=("园区", "nunique"),
        ).reset_index()
        region_stats = region_stats[region_stats["所属区域"] != "其他"].sort_values("项目数", ascending=False)
        region_stats["金额合计"] = region_stats["金额合计"].round(2)
        st.dataframe(region_stats, use_container_width=True, hide_index=True)
        
        # 区域下各园区明细
        st.markdown("##### 各区域下园区明细")
        for region in region_stats["所属区域"].unique():
            region_df = sub[sub["所属区域"] == region]
            parks_in_region = region_df.groupby("园区", dropna=False).agg(
                项目数=("序号", "count"),
                金额合计=("拟定金额", "sum"),
            ).reset_index().sort_values("项目数", ascending=False)
            parks_in_region["金额合计"] = parks_in_region["金额合计"].round(2)
            
            with st.expander(f"📌 {region}（{len(parks_in_region)}个园区，{int(parks_in_region['项目数'].sum())}个项目，{parks_in_region['金额合计'].sum():,.0f}万元）"):
                st.dataframe(parks_in_region, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # 按专业分包统计（如果存在该列且在标签池中勾选“专业分包”）
    if show_prof_subcontract and ("专业分包" in sub.columns or "专业细分" in sub.columns):
        prof_subcontract_col = "专业分包" if "专业分包" in sub.columns else "专业细分"
        st.markdown("### 📦 按专业分包统计")
        by_prof_subcontract = sub.groupby(prof_subcontract_col, dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index().sort_values("金额合计", ascending=False)
        by_prof_subcontract["金额合计"] = by_prof_subcontract["金额合计"].round(2)
        by_prof_subcontract["项目数占比"] = (by_prof_subcontract["项目数"] / by_prof_subcontract["项目数"].sum() * 100).round(2)
        by_prof_subcontract["金额占比"] = (by_prof_subcontract["金额合计"] / by_prof_subcontract["金额合计"].sum() * 100).round(2)
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### 专业分包项目数统计")
            st.dataframe(by_prof_subcontract[["专业分包" if prof_subcontract_col == "专业分包" else "专业细分", "项目数", "项目数占比"]], use_container_width=True, hide_index=True)
        with col2:
            st.markdown("#### 专业分包金额统计")
            st.dataframe(by_prof_subcontract[["专业分包" if prof_subcontract_col == "专业分包" else "专业细分", "金额合计", "金额占比"]], use_container_width=True, hide_index=True)
        
        # 显示图表
        try:
            import plotly.express as px
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(
                    by_prof_subcontract, 
                    values="项目数", 
                    names=prof_subcontract_col,
                    title="专业分包项目数占比",
                    color_discrete_sequence=CHART_COLORS_PIE[:len(by_prof_subcontract)]
                )
                fig.update_traces(textposition="outside", textinfo="label+percent+value")
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            with col2:
                fig = px.pie(
                    by_prof_subcontract, 
                    values="金额合计", 
                    names=prof_subcontract_col,
                    title="专业分包金额占比",
                    color_discrete_sequence=CHART_COLORS_PIE[:len(by_prof_subcontract)]
                )
                fig.update_traces(textposition="outside", textinfo="label+percent+value")
                st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            
            # 专业与专业分包的交叉统计
            st.markdown("#### 专业与专业分包交叉统计")
            cross_stats = sub.groupby(["专业", prof_subcontract_col], dropna=False).agg(
                项目数=("序号", "count"),
                金额合计=("拟定金额", "sum"),
            ).reset_index().sort_values("金额合计", ascending=False)
            # 过滤掉"其它系统"分类
            cross_stats = cross_stats[~cross_stats["专业"].isin(["其它系统", "其他系统"])]
            cross_stats["金额合计"] = cross_stats["金额合计"].round(2)
            st.dataframe(cross_stats, use_container_width=True, hide_index=True)
        except ImportError:
            pass
    
    st.markdown("---")
    
    # 2. 一类、二类、三类项目占比统计（仅当在标签池中勾选“项目分级”）
    if show_level_stats:
        st.markdown("### 📈 项目分级占比统计")
    if show_level_stats and "项目分级" in sub.columns:
        # 映射：一级->一类，二级->二类，三级->三类
        level_mapping = {"一级": "一类", "二级": "二类", "三级": "三类"}
        sub_copy = sub.copy()
        sub_copy["项目类别"] = sub_copy["项目分级"].map(level_mapping).fillna(sub_copy["项目分级"])
        
        level_stats = sub_copy.groupby("项目类别", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
        ).reset_index()
        
        total_projects = level_stats["项目数"].sum()
        total_amount_level = level_stats["金额合计"].sum()
        
        if total_projects > 0:
            level_stats["项目数占比"] = (level_stats["项目数"] / total_projects * 100).round(2)
            level_stats["金额占比"] = (level_stats["金额合计"] / total_amount_level * 100).round(2) if total_amount_level > 0 else 0
            level_stats["金额合计"] = level_stats["金额合计"].round(2)
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("#### 项目数量占比")
                st.dataframe(level_stats[["项目类别", "项目数", "项目数占比"]], use_container_width=True, hide_index=True)
            with col2:
                st.markdown("#### 项目金额占比")
                st.dataframe(level_stats[["项目类别", "金额合计", "金额占比"]], use_container_width=True, hide_index=True)
            
            # 显示饼图
            try:
                import plotly.express as px
                col1, col2 = st.columns(2)
                with col1:
                    fig = px.pie(
                        level_stats, values="项目数", names="项目类别",
                        title="项目数量占比",
                        color_discrete_sequence=["#FF6B6B", "#4ECDC4", "#45B7D1"]
                    )
                    fig.update_traces(textposition="outside", textinfo="label+percent+value")
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
                with col2:
                    fig = px.pie(
                        level_stats, values="金额合计", names="项目类别",
                        title="项目金额占比",
                        color_discrete_sequence=["#FF6B6B", "#4ECDC4", "#45B7D1"]
                    )
                    fig.update_traces(textposition="outside", textinfo="label+percent+value")
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            except ImportError:
                pass
        else:
            st.info("暂无项目分级数据。")
    else:
        st.info("未找到项目分级列，无法进行分级占比统计。")
    
    st.markdown("---")
    
    # 3. 是否已实施判断
    st.markdown("### 🔧 项目实施状态分析")
    impl_col = None
    for col in sub.columns:
        col_str = str(col).strip()
        if "实施" in col_str and "时间" not in col_str.lower():
            impl_col = col
            break
    
    if impl_col:
        # 解析实施日期
        def parse_impl_date(series):
            """解析实施日期，支持Excel日期序列号、datetime、字符串格式"""
            result = pd.Series(pd.NaT, index=series.index, dtype='datetime64[ns]')
            
            if pd.api.types.is_datetime64_any_dtype(series):
                result = pd.to_datetime(series, errors='coerce')
                result = result.mask(result.dt.year == 1900, pd.NaT)
                return result
            
            numeric = pd.to_numeric(series, errors='coerce')
            excel_mask = pd.Series(False, index=series.index)
            if numeric.notna().any():
                excel_mask = (numeric >= 1) & (numeric <= 100000) & numeric.notna()
                if excel_mask.any():
                    result.loc[excel_mask] = pd.to_datetime(
                        numeric[excel_mask].astype(int),
                        unit='D',
                        origin='1899-12-30'
                    )
                    result = result.mask(result.dt.year == 1900, pd.NaT)
            
            str_mask = ~excel_mask & result.isna()
            if str_mask.any():
                str_series = series[str_mask].astype(str).str.strip()
                str_series = str_series.replace(['', 'nan', 'None', 'NaT'], pd.NA)
                str_mask2 = ~str_series.str.startswith('1900', na=False)
                str_parse = pd.to_datetime(str_series[str_mask2], format='mixed', errors='coerce')
                result.loc[str_mask] = str_parse
            
            return result
        
        sub_copy = sub.copy()
        sub_copy["_实施日期_parsed"] = parse_impl_date(sub_copy[impl_col])
        
        # 获取当前时间
        current_time = datetime.now()
        sub_copy["已实施"] = sub_copy["_实施日期_parsed"].notna() & (sub_copy["_实施日期_parsed"] <= pd.Timestamp(current_time))
        
        已实施项目 = sub_copy[sub_copy["已实施"]]
        未实施项目 = sub_copy[~sub_copy["已实施"]]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("已实施项目数", len(已实施项目))
            st.metric("已实施金额（万元）", f"{已实施项目['拟定金额'].sum():,.0f}" if len(已实施项目) > 0 else "0")
        with col2:
            st.metric("未实施项目数", len(未实施项目))
            st.metric("未实施金额（万元）", f"{未实施项目['拟定金额'].sum():,.0f}" if len(未实施项目) > 0 else "0")
        with col3:
            total_impl = len(sub_copy)
            if total_impl > 0:
                impl_rate = len(已实施项目) / total_impl * 100
                st.metric("实施率", f"{impl_rate:.1f}%")
            else:
                st.metric("实施率", "0%")
        
        # 按园区统计实施情况
        st.markdown("#### 各园区实施情况统计")
        park_impl_list = []
        for park in sub_copy["园区"].dropna().unique():
            park_df = sub_copy[sub_copy["园区"] == park]
            总项目数 = len(park_df)
            已实施数 = park_df["已实施"].sum()
            总金额 = park_df["拟定金额"].sum()
            已实施金额 = park_df[park_df["已实施"]]["拟定金额"].sum()
            park_impl_list.append({
                "园区": park,
                "总项目数": 总项目数,
                "已实施数": int(已实施数),
                "未实施数": int(总项目数 - 已实施数),
                "总金额": round(总金额, 2),
                "已实施金额": round(已实施金额, 2),
                "实施率": round(已实施数 / 总项目数 * 100, 1) if 总项目数 > 0 else 0,
            })
        park_impl_stats = pd.DataFrame(park_impl_list).sort_values("总金额", ascending=False)
        st.dataframe(park_impl_stats, use_container_width=True, hide_index=True)
    else:
        st.info("未找到实施日期列，无法进行实施状态分析。")
    
    st.markdown("---")
    
    # 4. 各园区的分类统计：一级项目、总部项目、重大改造项目（200万以上）
    st.markdown("### 🏢 各园区分类项目统计")
    
    # 准备数据
    park_analysis = []
    for park in sub["园区"].dropna().unique():
        park_df = sub[sub["园区"] == park]
        
        # 一级项目金额（支持多种格式：一级、1级、一级项目、1等）
        if "项目分级" in park_df.columns:
            # 将项目分级转换为字符串并去除空格，然后匹配包含"一级"、"1级"或数字"1"的值
            # 先尝试字符串匹配
            一级项目_str = park_df[
                park_df["项目分级"].astype(str).str.strip().str.contains("一级|1级", na=False, regex=True)
            ]
            # 再尝试数字匹配（如果是数字1）
            try:
                一级项目_num = park_df[pd.to_numeric(park_df["项目分级"], errors='coerce') == 1]
            except:
                一级项目_num = pd.DataFrame()
            # 合并两种匹配结果
            一级项目 = pd.concat([一级项目_str, 一级项目_num]).drop_duplicates()
        else:
            一级项目 = pd.DataFrame()
        一级项目金额 = 一级项目["拟定金额"].sum() if len(一级项目) > 0 else 0
        
        # 总部项目金额（总部重点关注项目列为"是"的项目）
        if "总部重点关注项目" in park_df.columns:
            总部项目 = park_df[
                park_df["总部重点关注项目"].astype(str).str.strip().str.contains("是", na=False, case=False)
            ]
        else:
            总部项目 = pd.DataFrame()
        总部项目金额 = 总部项目["拟定金额"].sum() if len(总部项目) > 0 else 0
        
        # 重大改造项目（单个200万以上）
        重大改造项目 = park_df[park_df["拟定金额"] >= 200]
        重大改造项目金额 = 重大改造项目["拟定金额"].sum() if len(重大改造项目) > 0 else 0
        重大改造项目数 = len(重大改造项目)
        
        # 总金额
        总金额 = park_df["拟定金额"].sum()
        
        park_analysis.append({
            "园区": park,
            "一级项目金额": round(一级项目金额, 2),
            "一级项目占比": round(一级项目金额 / 总金额 * 100, 2) if 总金额 > 0 else 0,
            "总部项目金额": round(总部项目金额, 2),
            "总部项目占比": round(总部项目金额 / 总金额 * 100, 2) if 总金额 > 0 else 0,
            "重大改造项目数": 重大改造项目数,
            "重大改造项目金额": round(重大改造项目金额, 2),
            "重大改造项目占比": round(重大改造项目金额 / 总金额 * 100, 2) if 总金额 > 0 else 0,
            "总金额": round(总金额, 2),
        })
    
    park_analysis_df = pd.DataFrame(park_analysis)
    park_analysis_df = park_analysis_df.sort_values("总金额", ascending=False)
    
    st.dataframe(park_analysis_df, use_container_width=True, hide_index=True)
    
    # 显示金额汇总信息
    total_level1 = park_analysis_df["一级项目金额"].sum()
    total_hq = park_analysis_df["总部项目金额"].sum()
    total_major = park_analysis_df["重大改造项目金额"].sum()
    total_all = park_analysis_df["总金额"].sum()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("一级项目总金额", f"{total_level1:,.0f} 万元")
    with col2:
        st.metric("总部项目总金额", f"{total_hq:,.0f} 万元")
    with col3:
        st.metric("重大改造项目总金额", f"{total_major:,.0f} 万元")
    with col4:
        st.metric("所有项目总金额", f"{total_all:,.0f} 万元")
    
    st.markdown("---")
    
    # 显示整合图表（合并到同一个坐标轴下，优化版）
    try:
        import plotly.graph_objects as go
        
        # 按总金额排序，确保图表顺序一致
        park_analysis_df_sorted = park_analysis_df.sort_values("总金额", ascending=False)
        
        # 创建单一图表，使用三Y轴（左Y轴：金额，中Y轴：项目数，右Y轴：占比）
        fig = go.Figure()
        
        # 左Y轴：金额（柱状图）
        # 1. 一级项目金额
        fig.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["一级项目金额"],
                name="一级项目金额（万元）",
                marker=dict(
                    color="#5470c6",
                    line=dict(color="#3a5a9c", width=1)
                ),
                text=park_analysis_df_sorted["一级项目金额"].apply(lambda x: f"{int(x)}万" if x > 0 else None),
                textposition="outside",
                textfont=dict(size=12, color="#5470c6"),
                hovertemplate="<b>%{x}</b><br>一级项目金额: %{y:,.0f} 万元<extra></extra>",
                yaxis="y",
                cliponaxis=False
            )
        )
        
        # 2. 总部项目金额
        fig.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["总部项目金额"],
                name="总部项目金额（万元）",
                marker=dict(
                    color="#91cc75",
                    line=dict(color="#6fa85a", width=1)
                ),
                text=park_analysis_df_sorted["总部项目金额"].apply(lambda x: f"{int(x)}万" if x > 0 else None),
                textposition="outside",
                textfont=dict(size=12, color="#91cc75"),
                hovertemplate="<b>%{x}</b><br>总部项目金额: %{y:,.0f} 万元<extra></extra>",
                yaxis="y",
                cliponaxis=False
            )
        )
        
        # 3. 重大改造项目金额
        fig.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["重大改造项目金额"],
                name="重大改造项目金额（万元）",
                marker=dict(
                    color="#fac858",
                    line=dict(color="#d4a84a", width=1)
                ),
                text=park_analysis_df_sorted["重大改造项目金额"].apply(lambda x: f"{int(x)}万" if x > 0 else None),
                textposition="outside",
                textfont=dict(size=12, color="#d4a84a"),
                hovertemplate="<b>%{x}</b><br>重大改造项目金额: %{y:,.0f} 万元<extra></extra>",
                yaxis="y",
                cliponaxis=False
            )
        )
        
        # 中Y轴：项目数量（使用独立的Y轴，避免缩放）
        max_amount = max(
            park_analysis_df_sorted["一级项目金额"].max(),
            park_analysis_df_sorted["总部项目金额"].max(),
            park_analysis_df_sorted["重大改造项目金额"].max()
        )
        max_count = park_analysis_df_sorted["重大改造项目数"].max()
        # 计算缩放因子，使项目数在视觉上与金额协调
        if max_count > 0 and max_amount > 0:
            scale_factor = max_amount / (max_count * 50)  # 调整缩放比例
        else:
            scale_factor = 1
        scaled_count = park_analysis_df_sorted["重大改造项目数"] * scale_factor
        
        # 4. 重大改造项目数量
        fig.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=scaled_count,
                name="重大改造项目数（个）",
                marker=dict(
                    color="#73c0de",
                    line=dict(color="#4a9bc0", width=1.5)
                ),
                text=park_analysis_df_sorted["重大改造项目数"].apply(lambda x: f"{int(x)}个" if x > 0 else None),
                textposition="inside",
                textfont=dict(size=11, color="#ffffff"),
                customdata=list(zip(
                    park_analysis_df_sorted["重大改造项目数"],
                    park_analysis_df_sorted["重大改造项目金额"]
                )),
                hovertemplate="<b>%{x}</b><br>重大改造项目数: %{customdata[0]:.0f} 个<br>重大改造项目金额: %{customdata[1]:,.0f} 万元<extra></extra>",
                yaxis="y",
                opacity=0.85,
                cliponaxis=False
            )
        )
        
        # 右Y轴：占比（折线图）
        # 5. 一级项目占比
        fig.add_trace(
            go.Scatter(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["一级项目占比"],
                name="一级项目占比（%）",
                mode="lines+markers",
                marker=dict(
                    color="#ee6666",
                    size=10,
                    line=dict(width=2, color="white"),
                    symbol="circle"
                ),
                line=dict(color="#ee6666", width=3),
                text=park_analysis_df_sorted["一级项目占比"].apply(lambda x: f"{x:.0f}%" if x > 0 else None),
                textposition="top center",
                textfont=dict(size=11, color="#ee6666"),
                customdata=park_analysis_df_sorted["一级项目金额"],
                hovertemplate="<b>%{x}</b><br>一级项目占比: %{y:.1f}%<br>一级项目金额: %{customdata:,.0f} 万元<extra></extra>",
                yaxis="y2",
                cliponaxis=False
            )
        )
        
        # 6. 总部项目占比
        fig.add_trace(
            go.Scatter(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["总部项目占比"],
                name="总部项目占比（%）",
                mode="lines+markers",
                marker=dict(
                    color="#ff9800",
                    size=10,
                    line=dict(width=2, color="white"),
                    symbol="square"
                ),
                line=dict(color="#ff9800", width=3, dash="dash"),
                text=park_analysis_df_sorted["总部项目占比"].apply(lambda x: f"{x:.0f}%" if x > 0 else None),
                textposition="top center",
                textfont=dict(size=11, color="#ff9800"),
                customdata=park_analysis_df_sorted["总部项目金额"],
                hovertemplate="<b>%{x}</b><br>总部项目占比: %{y:.1f}%<br>总部项目金额: %{customdata:,.0f} 万元<extra></extra>",
                yaxis="y2",
                cliponaxis=False
            )
        )
        
        # 7. 重大改造项目占比
        fig.add_trace(
            go.Scatter(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["重大改造项目占比"],
                name="重大改造项目占比（%）",
                mode="lines+markers",
                marker=dict(
                    color="#9c27b0",
                    size=10,
                    line=dict(width=2, color="white"),
                    symbol="diamond"
                ),
                line=dict(color="#9c27b0", width=3, dash="dot"),
                text=park_analysis_df_sorted["重大改造项目占比"].apply(lambda x: f"{x:.0f}%" if x > 0 else None),
                textposition="top center",
                textfont=dict(size=11, color="#9c27b0"),
                customdata=park_analysis_df_sorted["重大改造项目金额"],
                hovertemplate="<b>%{x}</b><br>重大改造项目占比: %{y:.1f}%<br>重大改造项目金额: %{customdata:,.0f} 万元<extra></extra>",
                yaxis="y2",
                cliponaxis=False
            )
        )
        
        # 更新X轴
        fig.update_xaxes(
            tickangle=-45,
            tickfont=dict(size=11),
            title_text="园区",
            title_font=dict(size=13, color="#333"),
            showgrid=True,
            gridcolor="rgba(200,200,200,0.3)",
            gridwidth=1,
            showline=True,
            linecolor="#ccc",
            linewidth=1
        )
        
        # 更新左Y轴（金额）
        fig.update_yaxes(
            title_text="金额（万元）",
            title_font=dict(size=13, color="#333"),
            tickfont=dict(size=11),
            side="left",
            showgrid=True,
            gridcolor="rgba(200,200,200,0.3)",
            gridwidth=1,
            showline=True,
            linecolor="#5470c6",
            linewidth=2,
            zeroline=True,
            zerolinecolor="rgba(200,200,200,0.5)",
            zerolinewidth=1
        )
        
        # 更新右Y轴（占比）
        fig.update_yaxes(
            title_text="占比（%）",
            title_font=dict(size=13, color="#333"),
            tickfont=dict(size=11),
            side="right",
            overlaying="y",
            range=[0, 105],
            showgrid=False,
            showline=True,
            linecolor="#ee6666",
            linewidth=2
        )
        
        # 更新整体布局
        fig.update_layout(
            height=700,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.18,
                xanchor="center",
                x=0.5,
                font=dict(size=11),
                bgcolor="rgba(255,255,255,0.95)",
                bordercolor="rgba(0,0,0,0.3)",
                borderwidth=1,
                itemwidth=30
            ),
            title=dict(
                text="<b>各园区分类项目统计（金额、项目数与占比）</b>",
                x=0.5,
                xanchor="center",
                y=0.97,
                yanchor="top",
                font=dict(size=18, family="Arial, sans-serif", color="#1f4788")
            ),
            margin=dict(t=100, b=160, l=90, r=90),
            plot_bgcolor="rgba(255,255,255,1)",
            paper_bgcolor="white",
            barmode="group",
            bargap=0.15,
            bargroupgap=0.1,
            hovermode="x unified",
            hoverlabel=dict(
                bgcolor="rgba(255,255,255,0.95)",
                bordercolor="#333",
                font_size=12,
                font_family="Arial"
            )
        )
        
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        
        # 添加一个新的金额统计图表：分组柱状图，使用对数刻度以保证小金额园区的可见性
        st.markdown("#### 📊 各园区分类项目金额统计（对数刻度）")
        fig_amount = go.Figure()
        
        # 分组柱状图：一级项目金额、总部项目金额、重大改造项目金额
        fig_amount.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["一级项目金额"],
                name="一级项目金额（万元）",
                marker=dict(color="#5470c6", line=dict(color="#3a5a9c", width=1)),
                text=park_analysis_df_sorted["一级项目金额"].apply(lambda x: f"{int(x)}" if x > 0 else ""),
                textposition="outside",
                textfont=dict(size=9, color="#5470c6"),
                hovertemplate="<b>%{x}</b><br>一级项目金额: %{y:,.0f} 万元<extra></extra>"
            )
        )
        
        fig_amount.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["总部项目金额"],
                name="总部项目金额（万元）",
                marker=dict(color="#91cc75", line=dict(color="#6fa85a", width=1)),
                text=park_analysis_df_sorted["总部项目金额"].apply(lambda x: f"{int(x)}" if x > 0 else ""),
                textposition="outside",
                textfont=dict(size=9, color="#91cc75"),
                hovertemplate="<b>%{x}</b><br>总部项目金额: %{y:,.0f} 万元<extra></extra>"
            )
        )
        
        fig_amount.add_trace(
            go.Bar(
                x=park_analysis_df_sorted["园区"],
                y=park_analysis_df_sorted["重大改造项目金额"],
                name="重大改造项目金额（万元）",
                marker=dict(color="#fac858", line=dict(color="#d4a84a", width=1)),
                text=park_analysis_df_sorted["重大改造项目金额"].apply(lambda x: f"{int(x)}" if x > 0 else ""),
                textposition="outside",
                textfont=dict(size=9, color="#d4a84a"),
                hovertemplate="<b>%{x}</b><br>重大改造项目金额: %{y:,.0f} 万元<extra></extra>"
            )
        )
        
        fig_amount.update_xaxes(
            tickangle=-45,
            tickfont=dict(size=11),
            title_text="园区",
            title_font=dict(size=13, color="#333"),
            showgrid=True,
            gridcolor="rgba(200,200,200,0.3)"
        )
        
        # 计算Y轴范围，使用对数刻度以保证小金额园区的可见性
        import math
        max_amount = park_analysis_df_sorted["总金额"].max()
        min_amount = park_analysis_df_sorted[park_analysis_df_sorted["总金额"] > 0]["总金额"].min()
        
        # 生成对数刻度的不均匀标签
        if max_amount > 0 and min_amount > 0 and not math.isnan(min_amount) and max_amount > min_amount * 2:
            # 计算对数范围
            log_min = math.log10(max(1, min_amount))  # 确保最小值至少为1
            log_max = math.log10(max_amount)
            
            # 生成不均匀的刻度值（对数间隔）
            tick_vals = []
            tick_texts = []
            
            # 生成主要刻度：1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000等
            for exp in range(int(math.floor(log_min)), int(math.ceil(log_max)) + 1):
                for multiplier in [1, 2, 5]:
                    val = multiplier * (10 ** exp)
                    if val >= max(1, min_amount * 0.5) and val <= max_amount * 1.5:
                        tick_vals.append(val)
                        if val >= 1000:
                            tick_texts.append(f"{val/1000:.1f}千")
                        elif val >= 100:
                            tick_texts.append(f"{int(val)}")
                        else:
                            tick_texts.append(f"{val}")
            
            # 去重并排序
            tick_pairs = sorted(set(zip(tick_vals, tick_texts)), key=lambda x: x[0])
            tick_vals = [v for v, _ in tick_pairs]
            tick_texts = [t for _, t in tick_pairs]
        else:
            tick_vals = None
            tick_texts = None
        
        fig_amount.update_yaxes(
            title_text="金额（万元，对数刻度）",
            title_font=dict(size=13, color="#333"),
            tickfont=dict(size=10),
            showgrid=True,
            gridcolor="rgba(200,200,200,0.3)",
            type="log",  # 使用对数刻度
            tickvals=tick_vals if tick_vals else None,
            ticktext=tick_texts if tick_texts else None,
            dtick=1  # 对数刻度的步长
        )
        
        fig_amount.update_layout(
            height=600,
            barmode="group",  # 使用分组模式，支持对数刻度
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.15,
                xanchor="center",
                x=0.5,
                font=dict(size=11)
            ),
            title=dict(
                text="<b>各园区分类项目金额统计（对数刻度，保证小金额园区可见性）</b>",
                x=0.5,
                xanchor="center",
                y=0.97,
                yanchor="top",
                font=dict(size=16, color="#1f4788")
            ),
            margin=dict(t=80, b=140, l=80, r=40),
            plot_bgcolor="rgba(255,255,255,1)",
            paper_bgcolor="white",
            hovermode="x unified"
        )
        
        st.plotly_chart(fig_amount, use_container_width=True, config={"displayModeBar": False})
        
    except ImportError:
        # 如果plotly不可用，回退到简单的表格显示
        st.info("图表库不可用，仅显示数据表格。")
    except Exception as e:
        st.warning(f"图表生成出错：{str(e)}")
    
    st.markdown("---")
    
    # 2. 确定项目（有立项日期）和未确定项目（无立项日期）分析
    st.markdown("### ✅ 项目确定状态分析")
    
    # 查找立项日期列（支持多种可能的列名）
    立项_col = None
    for col in sub.columns:
        col_str = str(col).strip()
        # 支持多种列名格式
        if any(keyword in col_str for keyword in ["需求立项", "项目立项", "立项日期", "立项"]):
            # 排除包含"审核"、"决策"等其他时间节点的列
            if "审核" not in col_str and "决策" not in col_str and "成本" not in col_str:
                立项_col = col
                break
    
    if 立项_col:
        # 创建数据副本，避免修改原始数据
        sub = sub.copy()
        
        # 解析日期列：支持Excel日期序列号、datetime对象、字符串等多种格式
        def parse_date_series(series):
            """解析日期序列，支持Excel日期序列号、datetime、字符串格式"""
            result = pd.Series(pd.NaT, index=series.index, dtype='datetime64[ns]')
            
            # 1. 如果已经是datetime类型，直接使用
            if pd.api.types.is_datetime64_any_dtype(series):
                result = pd.to_datetime(series, errors='coerce')
                # 过滤1900年的日期（Excel占位符）
                result = result.mask(result.dt.year == 1900, pd.NaT)
                return result
            
            # 2. 尝试解析为数值（Excel日期序列号）
            numeric = pd.to_numeric(series, errors='coerce')
            excel_mask = pd.Series(False, index=series.index)
            if numeric.notna().any():
                # Excel日期序列号范围：1-100000（约1900-2100年）
                excel_mask = (numeric >= 1) & (numeric <= 100000) & numeric.notna()
                if excel_mask.any():
                    # Excel基准日期：1899-12-30
                    result.loc[excel_mask] = pd.to_datetime(
                        numeric[excel_mask].astype(int), 
                        unit='D', 
                        origin='1899-12-30'
                    )
                    # 过滤1900年的日期
                    result = result.mask(result.dt.year == 1900, pd.NaT)
            
            # 3. 解析字符串格式的日期（仅对未成功解析为Excel序列号的部分）
            str_mask = ~excel_mask & result.isna()
            if str_mask.any():
                str_series = series[str_mask].astype(str).str.strip()
                str_series = str_series.replace(['', 'nan', 'None', 'NaT'], pd.NA)
                # 过滤以1900开头的字符串
                str_mask2 = ~str_series.str.startswith('1900', na=False)
                str_parse = pd.to_datetime(str_series[str_mask2], format='mixed', errors='coerce')
                result.loc[str_mask] = str_parse
            
            return result
        
        # 处理合并单元格：按园区向下填充空值
        sub[立项_col] = sub[立项_col].replace('', pd.NA)
        # 按园区和序号排序
        sorted_idx = sub.sort_values(['园区', '序号']).index
        # 按园区分组向下填充
        sub.loc[sorted_idx, 立项_col] = sub.loc[sorted_idx].groupby('园区', sort=False)[立项_col].ffill()
        
        # 解析日期
        sub["_立项日期_parsed"] = parse_date_series(sub[立项_col])
        
        # 判断是否有有效立项日期
        sub["有立项日期"] = sub["_立项日期_parsed"].notna()
        
        确定项目 = sub[sub["有立项日期"]]
        未确定项目 = sub[~sub["有立项日期"]]
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**已确定项目（有立项日期）**")
            st.metric("项目数", len(确定项目))
            st.metric("金额合计（万元）", f"{确定项目['拟定金额'].sum():,.0f}")
            if not 确定项目.empty:
                # 准备显示用的数据框
                display_df = 确定项目[["园区", "序号", "项目名称", "拟定金额"]].copy()
                # 添加格式化后的立项日期
                if "_立项日期_parsed" in 确定项目.columns:
                    display_df["立项日期"] = 确定项目["_立项日期_parsed"].dt.strftime("%Y-%m-%d")
                else:
                    display_df["立项日期"] = 确定项目[立项_col].astype(str)
                st.dataframe(
                    display_df.head(20),
                    use_container_width=True,
                    hide_index=True
                )
        
        with col2:
            st.markdown("**未确定项目（无立项日期）**")
            st.metric("项目数", len(未确定项目))
            st.metric("金额合计（万元）", f"{未确定项目['拟定金额'].sum():,.0f}")
            if not 未确定项目.empty:
                st.dataframe(
                    未确定项目[["园区", "序号", "项目名称", "拟定金额"]].head(20),
                    use_container_width=True,
                    hide_index=True
                )
        
        # 确定率统计
        st.markdown("#### 确定率统计")
        park_determination = sub.groupby("园区", dropna=False).agg(
            总项目数=("序号", "count"),
            已确定数=("有立项日期", "sum"),
        ).reset_index()
        park_determination["未确定数"] = park_determination["总项目数"] - park_determination["已确定数"]
        park_determination["确定率"] = (park_determination["已确定数"] / park_determination["总项目数"] * 100).round(1)
        st.dataframe(park_determination, use_container_width=True, hide_index=True)
    else:
        st.info("未找到立项日期列，无法进行确定/未确定项目分析。")
    
    st.markdown("---")
    
    # 3. 按月份统计立项
    st.markdown("### 📅 按月份统计立项")
    if 立项_col and "_立项日期_parsed" in sub.columns:
        # 从已解析的日期列提取月份
        sub["立项月份"] = sub["_立项日期_parsed"].dt.to_period('M').astype(str)
        有月份的项目 = sub[sub["立项月份"].notna()]
        
        if not 有月份的项目.empty:
            monthly_stats = 有月份的项目.groupby("立项月份", dropna=False).agg(
                立项项目数=("序号", "count"),
                立项金额=("拟定金额", "sum"),
            ).reset_index().sort_values("立项月份")
            monthly_stats["立项金额"] = monthly_stats["立项金额"].round(2)
            
            # 显示表格
            st.dataframe(monthly_stats, use_container_width=True, hide_index=True)
            
            # 显示图表
            try:
                import plotly.express as px
                col1, col2 = st.columns(2)
                with col1:
                    fig = px.bar(
                        monthly_stats, x="立项月份", y="立项项目数",
                        title="每月立项项目数",
                        text_auto=".0f"
                    )
                    fig.update_layout(xaxis_tickangle=-45, showlegend=False, height=350)
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
                
                with col2:
                    fig = px.bar(
                        monthly_stats, x="立项月份", y="立项金额",
                        title="每月立项金额（万元）",
                        text_auto=".0f"
                    )
                    fig.update_layout(xaxis_tickangle=-45, showlegend=False, height=350)
                    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
            except ImportError:
                st.bar_chart(monthly_stats.set_index("立项月份"))
        else:
            st.info("暂无有效的立项日期数据。")
    else:
        st.info("未找到立项日期列，无法进行月份统计。")


def _add_城市和区域列(df: pd.DataFrame) -> pd.DataFrame:
    """为 df 同时增加「城市」和「所属区域」列，不修改原表。"""
    out = df.copy()
    out["城市"] = out["园区"].map(园区_TO_城市).fillna("其他")
    out["所属区域"] = out["园区"].map(园区_TO_区域).fillna("其他")
    return out


def _build_城市_园区明细(df: pd.DataFrame) -> dict:
    """按城市汇总，每个城市下为各园区的：园区名称、项目总数、总预算。供地图 tooltip 使用。"""
    sub = df[df["城市"].notna() & (df["城市"] != "其他")]
    if sub.empty:
        return {}
    by_city_park = sub.groupby(["城市", "园区"], dropna=False).agg(
        项目数=("序号", "count"),
        金额合计=("拟定金额", "sum"),
    ).reset_index()
    out = {}
    for city in by_city_park["城市"].unique():
        rows = by_city_park[by_city_park["城市"] == city]
        parks = []
        total_n = 0
        total_a = 0
        for _, r in rows.iterrows():
            n = int(r["项目数"])
            a = int(r["金额合计"])
            parks.append({"园区名称": str(r["园区"]), "项目数": n, "预算万元": int(round(a))})
            total_n += n
            total_a += a
        out[str(city)] = {"项目总数": total_n, "总预算万元": int(round(total_a)), "园区列表": parks}
    return out


def _render_中国地图(df: pd.DataFrame, city_tooltip_data: dict):
    """中国地图：悬浮显示城市下各园区详情；点击城市后通过 URL 参数筛选并跳转下方详情。"""
    try:
        from pyecharts.charts import Geo
        from pyecharts import options as opts
        from pyecharts.commons.utils import JsCode
    except ImportError:
        st.warning("请安装 pyecharts：pip install pyecharts")
        st.info("如果已安装，请尝试：pip install pyecharts -U")
        st.info("地图显示还需要安装地图数据包：pip install echarts-china-provinces-pypkg echarts-china-cities-pypkg")
        return
    
    # 检查数据是否为空
    if df.empty:
        st.warning("数据为空，无法显示地图。")
        return
    
    # 检查是否有城市列
    if "城市" not in df.columns:
        st.warning("数据中缺少'城市'列，无法显示地图。")
        return
    
    by_city = df.groupby("城市", dropna=False).agg(
        项目数=("序号", "count"),
        金额合计=("拟定金额", "sum"),
    ).reset_index()
    
    data = []
    for _, row in by_city.iterrows():
        city = row["城市"]
        if city in 城市_COORDS and city != "其他":
            data.append((city, int(row["项目数"])))
    
    if not data:
        st.info("当前数据中暂无已配置区位的城市，或请先在侧边栏选择园区。")
        st.info(f"数据中的城市列表：{by_city['城市'].unique().tolist()}")
        st.info(f"已配置区位的城市：{list(城市_COORDS.keys())[:10]}...")
        return
    
    # 准备园区地点数据：收集所有园区的位置信息（在创建图表之前）
    park_locations = []
    for park in df["园区"].dropna().unique():
        if park in 园区_TO_城市:
            city = 园区_TO_城市[park]
            if city in 城市_COORDS:
                lon, lat = 城市_COORDS[city]
                # 统计该园区的项目数
                park_count = len(df[df["园区"] == park])
                park_locations.append((park, lon, lat, park_count))
    
    # 悬浮详情：园区名称、园区上报项目总数、园区总预算；城市级汇总（JS 中用 [] 访问中文键）
    tooltip_js = JsCode(
        """
        function(params) {
            var name = params.name;
            var value = params.value;
            var n = (value && value[2]) != null ? value[2] : (value || 0);
            var info = typeof window.MAP_TOOLTIP_DATA !== 'undefined' && window.MAP_TOOLTIP_DATA[name];
            if (info) {
                var s = '<div style="text-align:left; min-width:200px;">';
                s += '<b>' + name + '</b><br/>';
                s += '项目总数：' + (info['项目总数'] || n) + ' 项<br/>';
                s += '总预算：' + (info['总预算万元'] || 0) + ' 万元<br/>';
                s += '<hr style="margin:6px 0;"/>';
                s += '各园区：<br/>';
                var list = info['园区列表'] || [];
                for (var i = 0; i < list.length; i++) {
                    var p = list[i];
                    s += '· ' + (p['园区名称'] || '') + '｜' + (p['项目数'] || 0) + ' 项｜' + (p['预算万元'] || 0) + ' 万<br/>';
                }
                s += '</div>';
                return s;
            }
            return name + '<br/>项目数：' + n + ' 项';
        }
        """
    )
    
    # 使用Geo图表（支持同时显示地图和园区位置散点）
    geo = Geo(init_opts=opts.InitOpts(width="100%", height="500px", theme="light", renderer="canvas"))
    geo.add_schema(maptype="china", is_roam=True)
    # 添加所有城市坐标
    for city, (lon, lat) in 城市_COORDS.items():
        geo.add_coordinate(city, lon, lat)
    # 添加城市项目数散点图（使用effectScatter效果更明显）
    geo.add(
        "项目数",
        data,
        type_="effectScatter",
        symbol_size=14,
        effect_opts=opts.EffectOpts(scale=4, brush_type="stroke"),
        label_opts=opts.LabelOpts(is_show=True, formatter="{b}", font_size=11),
    )
    # 添加园区地点标记（红色散点）
    if park_locations:
        # 为每个园区添加坐标
        for park_name, lon, lat, park_count in park_locations:
            geo.add_coordinate(park_name, lon, lat)
        # 添加园区散点图
        park_data = [(park_name, park_count) for park_name, lon, lat, park_count in park_locations]
        geo.add(
            "园区位置",
            park_data,
            type_="scatter",
            symbol_size=10,
            itemstyle_opts=opts.ItemStyleOpts(color="#ff6b6b"),
            label_opts=opts.LabelOpts(is_show=True, formatter="{b}", font_size=9, position="right"),
        )
    # 设置全局选项
    geo.set_global_opts(
        title_opts=opts.TitleOpts(title="各地市项目分布（点击城市可筛选下方详情）", pos_left="center"),
        tooltip_opts=opts.TooltipOpts(trigger="item", formatter=tooltip_js),
        visualmap_opts=opts.VisualMapOpts(
            min_=min(d[1] for d in data),
            max_=max(d[1] for d in data),
            is_piecewise=False,
            pos_left="left",
            range_color=["#e0f3f8", "#0868ac"],
        ),
    )
    
    # 如果数据为空，显示备用信息
    if not data:
        st.warning("暂无地图数据可显示")
        # 显示城市列表作为备用
        if not by_city.empty:
            st.dataframe(by_city[["城市", "项目数", "金额合计"]], use_container_width=True, hide_index=True)
        return
    
    # 尝试使用pyecharts
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".html", delete=False, encoding="utf-8") as f:
            geo.render(f.name)
            html_path = f.name
        
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        
        # 检查HTML是否生成成功
        if not html or len(html) < 100:
            st.error("地图HTML生成失败，请检查pyecharts安装是否正确。")
            st.info("提示：可能需要安装地图数据包：pip install echarts-china-provinces-pypkg echarts-china-cities-pypkg")
            # 显示备用表格
            if not by_city.empty:
                st.dataframe(by_city[["城市", "项目数", "金额合计"]], use_container_width=True, hide_index=True)
            return
        
        # 检查HTML中是否包含echarts相关代码
        if "echarts" not in html.lower() and "echart" not in html.lower():
            st.warning("生成的HTML中未找到echarts相关代码，地图可能无法正常显示。")
            st.info("提示：可能需要安装地图数据包：pip install echarts-china-provinces-pypkg echarts-china-cities-pypkg")
            if not by_city.empty:
                st.dataframe(by_city[["城市", "项目数", "金额合计"]], use_container_width=True, hide_index=True)
            return
        
        # 注入 tooltip 数据与点击跳转：悬浮用 MAP_TOOLTIP_DATA，点击后带 ?selected_city= 刷新并定位下方
        import json
        tooltip_json = json.dumps(city_tooltip_data, ensure_ascii=False)
        inject = (
            "<script>\n"
            "window.MAP_TOOLTIP_DATA = " + tooltip_json + ";\n"
            "function attachMapClick() {\n"
            "  var dom = document.querySelector('[id^=\"_\"]');\n"
            "  if (dom && window.echarts) {\n"
            "    var inst = window.echarts.getInstanceByDom(dom);\n"
            "    if (inst && !inst._mapClickAttached) {\n"
            "      inst._mapClickAttached = true;\n"
            "      inst.on('click', function(params) {\n"
            "        if (params && params.name) {\n"
            "          var u = window.top.location.pathname || '/';\n"
            "          var q = 'selected_city=' + encodeURIComponent(params.name);\n"
            "          window.top.location.href = u + (u.indexOf('?')>=0 ? '&' : '?') + q;\n"
            "        }\n"
            "      });\n"
            "    }\n"
            "  }\n"
            "}\n"
            "if (document.readyState === 'complete') { setTimeout(attachMapClick, 300); }\n"
            "else { document.addEventListener('DOMContentLoaded', function() { setTimeout(attachMapClick, 300); }); }\n"
            "</script>\n"
        )
        # pyecharts 渲染的图表在 div 内，在 body 末尾插入 script
        if "</body>" in html:
            html = html.replace("</body>", inject + "</body>")
        else:
            html = html + inject
        
        # 显示地图
        st.info("使用pyecharts地图显示")
        st.components.v1.html(html, height=450, scrolling=False)
        
    except Exception as e:
        error_msg = str(e)
        st.error(f"pyecharts地图渲染出错：{error_msg}")
        st.info("已在上方显示Streamlit原生地图作为备用方案")
        st.info("如需使用pyecharts地图，请检查：")
        st.info("1) pyecharts是否正确安装：pip install pyecharts")
        st.info("2) 是否安装了地图数据包：pip install echarts-china-provinces-pypkg echarts-china-cities-pypkg")
        st.info("3) 数据是否包含城市信息")
        
        # 显示详细错误信息（仅在开发模式下）
        if st.checkbox("显示详细错误信息（调试用）", value=False):
            import traceback
            st.code(traceback.format_exc())
        
        # 显示数据表格作为备用
        if not by_city.empty:
            st.markdown("### 城市项目统计（表格视图）")
            st.dataframe(by_city[["城市", "项目数", "金额合计"]].sort_values("项目数", ascending=False), 
                        use_container_width=True, hide_index=True)
    finally:
        try:
            if 'html_path' in locals():
                Path(html_path).unlink(missing_ok=True)
        except Exception:
            pass


def _render_图表_简易(sub: pd.DataFrame):
    """无 plotly 时的简易柱状图回退。"""
    c1, c2 = st.columns(2)
    with c1:
        by_prof = sub.groupby("专业", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False)
        if not by_prof.empty:
            st.bar_chart(by_prof.set_index("专业")["项目数"])
    with c2:
        by_level = sub.groupby("项目分级", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False)
        if not by_level.empty:
            st.bar_chart(by_level.set_index("项目分级")["项目数"])
    by_park = sub.groupby("园区", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False).head(20)
    if not by_park.empty:
        st.bar_chart(by_park.set_index("园区")["项目数"])
    by_prof_m = sub.groupby("专业", dropna=False).agg(金额=("拟定金额", "sum")).reset_index().sort_values("金额", ascending=False)
    if not by_prof_m.empty:
        st.bar_chart(by_prof_m.set_index("专业")["金额"])


def generate_pdf_report_html(df: pd.DataFrame, 园区选择: list, output_path: str = None):
    """生成PDF报告，使用HTML转PDF方式，完整保留网页所有内容"""
    if output_path is None:
        output_path = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf').name
    
    # 处理园区选择
    if 园区选择 and len(园区选择) > 0:
        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
        if valid_parks:
            sub = df[df["园区"].isin(valid_parks)]
        else:
            sub = df[df["园区"].notna()]
    else:
        sub = df[df["园区"].notna()]
    
    # 过滤汇总行
    if "序号" in sub.columns:
        sub = sub[sub["序号"].notna()]
        sub = sub[~sub["序号"].astype(str).str.strip().isin(["合计", "预算系统合计", "差", "差额", "小计"])]
        sub = sub[pd.to_numeric(sub["序号"], errors='coerce').notna()]
    
    # 添加城市和区域列
    df_with_location = _add_城市和区域列(df)
    if 园区选择 and len(园区选择) > 0:
        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
        if valid_parks:
            sub_location = df_with_location[df_with_location["园区"].isin(valid_parks)]
        else:
            sub_location = df_with_location[df_with_location["园区"].notna()]
    else:
        sub_location = df_with_location[df_with_location["园区"].notna()]
    
    if "序号" in sub_location.columns:
        sub_location = sub_location[sub_location["序号"].notna()]
        sub_location = sub_location[~sub_location["序号"].astype(str).str.strip().isin(["合计", "预算系统合计", "差", "差额", "小计"])]
        sub_location = sub_location[pd.to_numeric(sub_location["序号"], errors='coerce').notna()]
    
    # 生成HTML内容
    html_content = generate_html_report(df, sub, sub_location, 园区选择)
    
    # 尝试使用weasyprint转换为PDF
    try:
        from weasyprint import HTML, CSS
        from weasyprint.text.fonts import FontConfiguration
        
        font_config = FontConfiguration()
        HTML(string=html_content).write_pdf(
            output_path,
            stylesheets=[CSS(string='''
                @page {
                    size: A4;
                    margin: 2cm;
                }
                body {
                    font-family: "Microsoft YaHei", "SimSun", "SimHei", sans-serif;
                    font-size: 12px;
                    line-height: 1.6;
                }
                h1 { font-size: 24px; color: #1f4788; margin-top: 20px; margin-bottom: 15px; }
                h2 { font-size: 20px; color: #2c5aa0; margin-top: 18px; margin-bottom: 12px; }
                h3 { font-size: 16px; color: #4a7bc8; margin-top: 15px; margin-bottom: 10px; }
                h4 { font-size: 14px; margin-top: 12px; margin-bottom: 8px; }
                table { border-collapse: collapse; width: 100%; margin: 10px 0; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #4a7bc8; color: white; font-weight: bold; }
                tr:nth-child(even) { background-color: #f2f2f2; }
                .chart-container { margin: 20px 0; text-align: center; }
                .section { page-break-inside: avoid; margin-bottom: 30px; }
            ''')],
            font_config=font_config
        )
        return output_path
    except (ImportError, Exception) as e:
        # 如果weasyprint不可用或失败，使用subprocess调用独立的playwright脚本
        weasyprint_error = str(e)
        try:
            import subprocess
            import os
            import sys
            
            # 创建临时HTML文件
            html_file = tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8')
            html_file.write(html_content)
            html_file.close()
            html_path = html_file.name
            
            # 转换文件路径为file:// URL格式
            html_abs_path = os.path.abspath(html_path)
            file_url = f"file:///{html_abs_path.replace(os.sep, '/')}"
            
            # 创建独立的playwright脚本文件
            script_content = f'''# -*- coding: utf-8 -*-
import asyncio
import sys
from playwright.async_api import async_playwright

async def main():
    html_url = "{file_url}"
    pdf_path = r"{output_path}"
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(html_url, wait_until="networkidle", timeout=30000)
        await page.pdf(
            path=pdf_path,
            format="A4",
            print_background=True,
            margin={{"top": "2cm", "right": "2cm", "bottom": "2cm", "left": "2cm"}}
        )
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
'''
            
            script_file = tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False, encoding='utf-8')
            script_file.write(script_content)
            script_file.close()
            script_path = script_file.name
            
            # 使用subprocess运行脚本（在新进程中，避免asyncio冲突）
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=120,
                cwd=os.path.dirname(script_path)
            )
            
            # 清理临时文件
            Path(script_path).unlink(missing_ok=True)
            Path(html_path).unlink(missing_ok=True)
            
            if result.returncode != 0:
                error_msg = result.stderr if result.stderr else result.stdout
                raise Exception(f"Playwright脚本执行失败 (返回码: {result.returncode}): {error_msg}")
            
            if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
                raise Exception("PDF文件未生成或文件为空")
            
            return output_path
        except ImportError as e2:
            raise ImportError(f"PDF生成失败。weasyprint错误: {weasyprint_error}。playwright未安装，请运行: pip install playwright && playwright install chromium")
        except subprocess.TimeoutExpired:
            raise Exception(f"PDF生成超时。请检查playwright是否正确安装: playwright install chromium")
        except Exception as e2:
            # 删除临时文件（如果存在）
            try:
                if 'html_path' in locals():
                    Path(html_path).unlink(missing_ok=True)
                if 'script_path' in locals():
                    Path(script_path).unlink(missing_ok=True)
            except:
                pass
            raise Exception(f"PDF生成失败。weasyprint错误: {weasyprint_error}。playwright错误: {str(e2)}。请检查playwright是否正确安装: playwright install chromium")


def generate_interactive_html(df: pd.DataFrame, 园区选择: list) -> str:
    """生成完全交互式的HTML文件，包含所有数据和交互功能，效果与运行程序一致"""
    import json
    
    # 准备数据：将DataFrame转换为JSON格式
    # 过滤汇总行
    df_clean = df.copy()
    if "序号" in df_clean.columns:
        df_clean = df_clean[df_clean["序号"].notna()]
        df_clean = df_clean[~df_clean["序号"].astype(str).str.strip().isin(["合计", "预算系统合计", "差", "差额", "小计"])]
        df_clean = df_clean[pd.to_numeric(df_clean["序号"], errors='coerce').notna()]
    
    # 添加城市和区域列
    df_with_location = _add_城市和区域列(df_clean)
    
    # 转换为JSON（处理NaN值）
    def convert_to_json_serializable(obj):
        if pd.isna(obj):
            return None
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.strftime('%Y-%m-%d')
        if isinstance(obj, (int, float)):
            return float(obj) if not pd.isna(obj) else None
        return str(obj)
    
    data_records = []
    for _, row in df_with_location.iterrows():
        record = {}
        for col in df_with_location.columns:
            val = row[col]
            record[col] = convert_to_json_serializable(val)
        data_records.append(record)
    
    # 获取所有园区列表
    parks_list = sorted([p for p in df_with_location["园区"].dropna().unique().tolist() 
                        if p and str(p).strip() and str(p) != "未知园区"])
    
    # 默认选中的园区
    default_parks = 园区选择 if 园区选择 and len(园区选择) > 0 else parks_list
    
    # 序列化JSON数据
    # 将JSON对象序列化为字符串，然后再次转义以便在JavaScript中作为字符串字面量使用
    data_json_raw = json.dumps(data_records, ensure_ascii=False)
    parks_json_raw = json.dumps(parks_list, ensure_ascii=False)
    
    # 将JSON字符串转换为JavaScript字符串字面量（转义引号、反斜杠等特殊字符）
    # 使用json.dumps再次转义，确保在JavaScript中可以安全使用
    data_json = json.dumps(data_json_raw)
    parks_json = json.dumps(parks_json_raw)
    
    # 生成HTML
    html_content = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>养老社区改良改造进度管理看板</title>
    <script src="https://cdn.plot.ly/plotly-2.26.0.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: "Microsoft YaHei", "SimSun", "SimHei", Arial, sans-serif;
            font-size: 14px;
            line-height: 1.6;
            color: #333;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
            background-color: white;
            min-height: 100vh;
        }}
        .header {{
            background: linear-gradient(135deg, #1f4788 0%, #4a7bc8 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            font-size: 28px;
            margin-bottom: 10px;
        }}
        .header .caption {{
            font-size: 14px;
            opacity: 0.9;
        }}
        .sidebar {{
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            border: 1px solid #e0e0e0;
        }}
        .sidebar h3 {{
            color: #1f4788;
            margin-bottom: 15px;
            font-size: 18px;
        }}
        .multiselect {{
            width: 100%;
            padding: 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
            background-color: white;
            max-height: 200px;
            overflow-y: auto;
        }}
        .multiselect option {{
            padding: 5px;
        }}
        .multiselect option:checked {{
            background-color: #4a7bc8;
            color: white;
        }}
        .tabs {{
            display: flex;
            border-bottom: 2px solid #e0e0e0;
            margin-bottom: 20px;
            overflow-x: auto;
        }}
        .tab-button {{
            padding: 12px 24px;
            background-color: #f8f9fa;
            border: none;
            border-bottom: 3px solid transparent;
            cursor: pointer;
            font-size: 15px;
            color: #666;
            transition: all 0.3s;
            white-space: nowrap;
        }}
        .tab-button:hover {{
            background-color: #e9ecef;
            color: #1f4788;
        }}
        .tab-button.active {{
            background-color: white;
            color: #1f4788;
            border-bottom-color: #4a7bc8;
            font-weight: bold;
        }}
        .tab-content {{
            display: none;
            padding: 20px 0;
        }}
        .tab-content.active {{
            display: block;
        }}
        .section {{
            margin-bottom: 30px;
        }}
        .section h2 {{
            font-size: 22px;
            color: #1f4788;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #4a7bc8;
        }}
        .section h3 {{
            font-size: 18px;
            color: #2c5aa0;
            margin: 20px 0 10px 0;
        }}
        .section h4 {{
            font-size: 16px;
            color: #4a7bc8;
            margin: 15px 0 8px 0;
        }}
        .metrics {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .metric {{
            background: linear-gradient(135deg, #f0f8ff 0%, #e6f3ff 100%);
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #4a7bc8;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .metric-label {{
            font-size: 13px;
            color: #666;
            margin-bottom: 8px;
        }}
        .metric-value {{
            font-size: 24px;
            font-weight: bold;
            color: #1f4788;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            font-size: 13px;
            background-color: white;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        th {{
            background-color: #4a7bc8;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: bold;
            position: sticky;
            top: 0;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #e0e0e0;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        tr:nth-child(even) {{
            background-color: #fafafa;
        }}
        .chart-container {{
            margin: 20px 0;
            background-color: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }}
        .info-box {{
            background-color: #e7f3ff;
            border-left: 4px solid #4a7bc8;
            padding: 15px;
            margin: 15px 0;
            border-radius: 4px;
        }}
        .warning-box {{
            background-color: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 15px;
            margin: 15px 0;
            border-radius: 4px;
        }}
        .expander {{
            margin: 10px 0;
        }}
        .expander-header {{
            background-color: #f8f9fa;
            padding: 12px;
            cursor: pointer;
            border-radius: 4px;
            border: 1px solid #e0e0e0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .expander-header:hover {{
            background-color: #e9ecef;
        }}
        .expander-content {{
            display: none;
            padding: 15px;
            border: 1px solid #e0e0e0;
            border-top: none;
            border-radius: 0 0 4px 4px;
        }}
        .expander-content.active {{
            display: block;
        }}
        .expander-icon {{
            transition: transform 0.3s;
        }}
        .expander-header.active .expander-icon {{
            transform: rotate(90deg);
        }}
        ul {{
            padding-left: 25px;
            margin: 10px 0;
        }}
        li {{
            margin: 8px 0;
        }}
        .data-table-container {{
            overflow-x: auto;
            margin: 15px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏠 养老社区改良改造进度管理看板</h1>
            <div class="caption">需求审核流程：社区提出 → 分级 → 专业分类 → 预算拆分 → 一线立项 → 项目部施工 → 总部协调招采/施工 → 督促验收</div>
        </div>
        
        <div class="sidebar">
            <h3>📊 数据筛选</h3>
            <label for="park-select" style="display: block; margin-bottom: 8px; font-weight: bold;">筛选园区：</label>
            <select id="park-select" class="multiselect" multiple size="8">
                {''.join([f'<option value="{p}" {"selected" if p in default_parks else ""}>{p}</option>' for p in parks_list])}
            </select>
            <div style="margin-top: 10px; font-size: 12px; color: #666;">
                💡 提示：按住 Ctrl (Windows) 或 Cmd (Mac) 键可多选
            </div>
            
            <div style="margin-top: 20px;">
                <div class="expander">
                    <div class="expander-header" onclick="toggleExpander(this)">
                        <span><strong>📋 需求审核与实施流程说明</strong></span>
                        <span class="expander-icon">▶</span>
                    </div>
                    <div class="expander-content">
                        <ul style="margin: 10px 0; padding-left: 20px;">
                            <li><strong>1. 社区提出：</strong>一线园区提出改造需求。</li>
                            <li><strong>2. 紧急程度分级：</strong>按一级（最高级）、二级、三级划分。</li>
                            <li><strong>3. 专业分类：</strong>按 9 大类专业划分：土建、供配电、暖通/供冷、弱电、供排水、电梯、其它、消防、安防等。</li>
                            <li><strong>4. 财务预算拆分：</strong>按预算系统进行金额拆分与汇总。</li>
                            <li><strong>5. 一线立项时间：</strong>一线填写需求并提出立项时间。</li>
                            <li><strong>6. 项目部施工：</strong>项目部根据已确定的需求立项组织施工。</li>
                            <li><strong>7. 总部运行保障部：</strong>督促一线需求稳定，协调总部相关部门把控需求，输出给不动产进行招采、施工。</li>
                            <li><strong>8. 施工验收：</strong>总部运行保障部督促一线园区进行最终施工验收。</li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="tabs">
            <button class="tab-button active" onclick="switchTab(0)">项目统计分析</button>
            <button class="tab-button" onclick="switchTab(1)">统计</button>
            <button class="tab-button" onclick="switchTab(2)">地区分析</button>
            <button class="tab-button" onclick="switchTab(3)">各园区分级分类</button>
            <button class="tab-button" onclick="switchTab(4)">总部视图</button>
            <button class="tab-button" onclick="switchTab(5)">全部项目</button>
        </div>
        
        <div id="tab-0" class="tab-content active"></div>
        <div id="tab-1" class="tab-content"></div>
        <div id="tab-2" class="tab-content"></div>
        <div id="tab-3" class="tab-content"></div>
        <div id="tab-4" class="tab-content"></div>
        <div id="tab-5" class="tab-content"></div>
    </div>
    
    <script>
        // 数据存储
        const allData = JSON.parse({data_json});
        const parksList = JSON.parse({parks_json});
        let filteredData = [...allData];
        let currentTab = 0;
        
        // 园区筛选
        document.getElementById('park-select').addEventListener('change', function() {{
            const selectedParks = Array.from(this.selectedOptions).map(opt => opt.value);
            if (selectedParks.length === 0) {{
                filteredData = allData.filter(d => d.园区 && d.园区 !== null && d.园区 !== '');
            }} else {{
                filteredData = allData.filter(d => selectedParks.includes(d.园区));
            }}
            renderAllTabs();
        }});
        
        // 标签页切换
        function switchTab(index) {{
            currentTab = index;
            document.querySelectorAll('.tab-button').forEach((btn, i) => {{
                btn.classList.toggle('active', i === index);
            }});
            document.querySelectorAll('.tab-content').forEach((content, i) => {{
                content.classList.toggle('active', i === index);
            }});
        }}
        
        // 工具函数
        function formatNumber(num) {{
            if (num === null || num === undefined || isNaN(num)) return '0';
            return parseFloat(num).toLocaleString('zh-CN', {{maximumFractionDigits: 2}});
        }}
        
        function formatCurrency(num) {{
            if (num === null || num === undefined || isNaN(num)) return '0';
            return parseFloat(num).toLocaleString('zh-CN', {{maximumFractionDigits: 0}});
        }}
        
        function getValue(row, col) {{
            return row[col] !== null && row[col] !== undefined ? row[col] : '';
        }}
        
        function isValidNumber(val) {{
            return val !== null && val !== undefined && !isNaN(val) && val !== '';
        }}
        
        // 过滤有效项目（有序号且为数字）
        function getValidProjects(data) {{
            return data.filter(d => {{
                const seq = d.序号;
                if (!seq || seq === null || seq === '') return false;
                const seqStr = String(seq).trim();
                if (['合计', '预算系统合计', '差', '差额', '小计'].includes(seqStr)) return false;
                return !isNaN(parseFloat(seq));
            }});
        }}
        
        // 渲染所有标签页
        function renderAllTabs() {{
            renderTab0(); // 项目统计分析
            renderTab1(); // 统计
            renderTab2(); // 地区分析
            renderTab3(); // 各园区分级分类
            renderTab4(); // 总部视图
            renderTab5(); // 全部项目
        }}
        
        // 日期解析工具函数
        function parseDate(dateStr) {{
            if (!dateStr || dateStr === null || dateStr === undefined || dateStr === '') return null;
            const str = String(dateStr).trim();
            if (str === '' || str === 'nan' || str === 'None' || str.startsWith('1900')) return null;
            
            // 尝试解析为日期
            const date = new Date(str);
            if (!isNaN(date.getTime()) && date.getFullYear() >= 2000) {{
                return date;
            }}
            
            // 尝试解析Excel日期序列号
            const num = parseFloat(str);
            if (!isNaN(num) && num >= 1 && num <= 100000) {{
                const excelDate = new Date(1899, 11, 30);
                excelDate.setDate(excelDate.getDate() + num);
                if (excelDate.getFullYear() >= 2000) {{
                    return excelDate;
                }}
            }}
            
            return null;
        }}
        
        // 稳定需求判断：需求已立项（需求立项日期有效）且非无效日期
        function isStableRequirement(d) {{
            // 查找需求立项列
            let 立项Col = null;
            for (let key in d) {{
                if (key.includes('需求立项')) {{
                    立项Col = key;
                    break;
                }}
            }}
            if (!立项Col) return false;
            
            const date = parseDate(d[立项Col]);
            return date !== null && date.getFullYear() >= 2000;
        }}
        
        // 标签页0: 项目统计分析
        function renderTab0() {{
            const validData = getValidProjects(filteredData);
            const container = document.getElementById('tab-0');
            
            if (validData.length === 0) {{
                container.innerHTML = '<div class="warning-box">当前筛选条件下暂无数据。</div>';
                return;
            }}
            
            // 计算统计数据
            const totalCount = validData.length;
            const totalAmount = validData.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0);
            
            // 尝试提取预算系统合计（从原始数据中查找汇总行）
            let budgetTotal = 0;
            const allDataForBudget = getValidProjects(allData);
            for (let d of allDataForBudget) {{
                const seq = String(d.序号 || '').trim();
                if (seq === '预算系统合计' || seq === '合计') {{
                    const amt = parseFloat(d.拟定金额) || parseFloat(d.金额) || parseFloat(d.预算) || 0;
                    if (amt > 0) {{
                        budgetTotal = amt;
                        break;
                    }}
                }}
            }}
            const diff = totalAmount - budgetTotal;
            
            // 按园区统计
            const parkStats = {{}};
            validData.forEach(d => {{
                const park = d.园区 || '未知';
                if (!parkStats[park]) {{
                    parkStats[park] = {{count: 0, amount: 0}};
                }}
                parkStats[park].count++;
                parkStats[park].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按所属区域统计
            const regionStats = {{}};
            validData.forEach(d => {{
                const region = d.所属区域 || '其他';
                if (region !== '其他') {{
                    if (!regionStats[region]) {{
                        regionStats[region] = {{count: 0, amount: 0, parks: new Set()}};
                    }}
                    regionStats[region].count++;
                    regionStats[region].amount += parseFloat(d.拟定金额) || 0;
                    if (d.园区) regionStats[region].parks.add(d.园区);
                }}
            }});
            
            // 按项目分级统计
            const levelStats = {{}};
            validData.forEach(d => {{
                const level = d.项目分级 || '未分类';
                if (!levelStats[level]) {{
                    levelStats[level] = {{count: 0, amount: 0}};
                }}
                levelStats[level].count++;
                levelStats[level].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 映射：一级->一类，二级->二类，三级->三类
            const levelMapping = {{'一级': '一类', '二级': '二类', '三级': '三类'}};
            const levelStatsMapped = {{}};
            Object.keys(levelStats).forEach(level => {{
                const mappedLevel = levelMapping[level] || level;
                if (!levelStatsMapped[mappedLevel]) {{
                    levelStatsMapped[mappedLevel] = {{count: 0, amount: 0}};
                }}
                levelStatsMapped[mappedLevel].count += levelStats[level].count;
                levelStatsMapped[mappedLevel].amount += levelStats[level].amount;
            }});
            
            // 项目实施状态分析
            let implCol = null;
            for (let key in validData[0]) {{
                if (key.includes('实施') && !key.toLowerCase().includes('时间')) {{
                    implCol = key;
                    break;
                }}
            }}
            
            let 已实施项目 = [];
            let 未实施项目 = [];
            let parkImplStats = {{}};
            
            if (implCol) {{
                const now = new Date();
                validData.forEach(d => {{
                    const implDate = parseDate(d[implCol]);
                    const isImplemented = implDate !== null && implDate <= now;
                    
                    if (isImplemented) {{
                        已实施项目.push(d);
                    }} else {{
                        未实施项目.push(d);
                    }}
                    
                    const park = d.园区 || '未知';
                    if (!parkImplStats[park]) {{
                        parkImplStats[park] = {{total: 0, implemented: 0, amount: 0, implAmount: 0}};
                    }}
                    parkImplStats[park].total++;
                    parkImplStats[park].amount += parseFloat(d.拟定金额) || 0;
                    if (isImplemented) {{
                        parkImplStats[park].implemented++;
                        parkImplStats[park].implAmount += parseFloat(d.拟定金额) || 0;
                    }}
                }});
            }}
            
            // 项目确定状态分析（有立项日期）
            let 立项Col = null;
            for (let key in validData[0]) {{
                if ((key.includes('需求立项') || key.includes('项目立项') || key.includes('立项日期') || key.includes('立项')) &&
                    !key.includes('审核') && !key.includes('决策') && !key.includes('成本')) {{
                    立项Col = key;
                    break;
                }}
            }}
            
            let 确定项目 = [];
            let 未确定项目 = [];
            let parkDeterminationStats = {{}};
            let monthlyStats = {{}};
            
            if (立项Col) {{
                // 按园区分组，向下填充空值（模拟合并单元格）
                const parkGroups = {{}};
                validData.forEach(d => {{
                    const park = d.园区 || '未知';
                    if (!parkGroups[park]) parkGroups[park] = [];
                    parkGroups[park].push(d);
                }});
                
                Object.keys(parkGroups).forEach(park => {{
                    let lastDate = null;
                    parkGroups[park].forEach(d => {{
                        const dateVal = d[立项Col];
                        if (dateVal && dateVal !== null && dateVal !== '') {{
                            lastDate = dateVal;
                        }} else if (lastDate) {{
                            d[立项Col + '_filled'] = lastDate;
                        }} else {{
                            d[立项Col + '_filled'] = dateVal;
                        }}
                    }});
                }});
                
                validData.forEach(d => {{
                    const dateVal = d[立项Col + '_filled'] || d[立项Col];
                    const hasDate = parseDate(dateVal) !== null;
                    
                    if (hasDate) {{
                        确定项目.push(d);
                        
                        // 按月统计
                        const date = parseDate(dateVal);
                        if (date) {{
                            const month = date.getFullYear() + '-' + String(date.getMonth() + 1).padStart(2, '0');
                            if (!monthlyStats[month]) {{
                                monthlyStats[month] = {{count: 0, amount: 0}};
                            }}
                            monthlyStats[month].count++;
                            monthlyStats[month].amount += parseFloat(d.拟定金额) || 0;
                        }}
                    }} else {{
                        未确定项目.push(d);
                    }}
                    
                    const park = d.园区 || '未知';
                    if (!parkDeterminationStats[park]) {{
                        parkDeterminationStats[park] = {{total: 0, determined: 0}};
                    }}
                    parkDeterminationStats[park].total++;
                    if (hasDate) parkDeterminationStats[park].determined++;
                }});
            }}
            
            // 各园区分类项目统计
            const parkAnalysis = {{}};
            validData.forEach(d => {{
                const park = d.园区 || '未知';
                if (!parkAnalysis[park]) {{
                    parkAnalysis[park] = {{
                        total: 0,
                        level1: 0,
                        hq: 0,
                        major: 0,
                        majorCount: 0
                    }};
                }}
                const amount = parseFloat(d.拟定金额) || 0;
                parkAnalysis[park].total += amount;
                
                // 一级项目识别：支持多种格式（一级、1级、一级项目、1等）
                const levelStr = String(d.项目分级 || '').trim();
                let isLevel1 = false;
                // 字符串匹配：包含"一级"或"1级"
                if (levelStr && (levelStr.includes('一级') || levelStr.includes('1级'))) {{
                    isLevel1 = true;
                }}
                // 数字匹配：如果是数字1
                if (!isLevel1) {{
                    const levelNum = parseFloat(levelStr);
                    if (!isNaN(levelNum) && levelNum === 1) {{
                        isLevel1 = true;
                    }}
                }}
                if (isLevel1) {{
                    parkAnalysis[park].level1 += amount;
                }}
                
                const hqFocus = String(d.总部重点关注项目 || '').trim();
                if (hqFocus === '是' || hqFocus.toLowerCase() === 'yes') {{
                    parkAnalysis[park].hq += amount;
                }}
                
                if (amount >= 200) {{
                    parkAnalysis[park].major += amount;
                    parkAnalysis[park].majorCount++;
                }}
            }});
            
            let html = `
                <div class="section">
                    <h2>📊 项目数量与费用统计</h2>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">项目总数</div>
                            <div class="metric-value">${{formatNumber(totalCount)}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总金额（万元）</div>
                            <div class="metric-value">${{formatCurrency(totalAmount)}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">预算系统合计（万元）</div>
                            <div class="metric-value">${{budgetTotal > 0 ? formatCurrency(budgetTotal) : '未找到'}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">差值（万元）</div>
                            <div class="metric-value" style="color: ${{diff !== 0 ? (diff > 0 ? '#d32f2f' : '#388e3c') : '#666'}};">${{formatCurrency(diff)}}</div>
                        </div>
                    </div>
                    
                    <h3>按园区统计</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>园区</th><th>项目数</th><th>金额合计（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(parkStats).sort((a, b) => parkStats[b].amount - parkStats[a].amount).map(park => `
                                    <tr>
                                        <td>${{park}}</td>
                                        <td>${{parkStats[park].count}}</td>
                                        <td>${{formatCurrency(parkStats[park].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    ${{Object.keys(regionStats).length > 0 ? `
                    <h3>按所属区域统计</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>所属区域</th><th>项目数</th><th>金额合计（万元）</th><th>园区数</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(regionStats).sort((a, b) => regionStats[b].count - regionStats[a].count).map(region => `
                                    <tr>
                                        <td>${{region}}</td>
                                        <td>${{regionStats[region].count}}</td>
                                        <td>${{formatCurrency(regionStats[region].amount)}}</td>
                                        <td>${{regionStats[region].parks.size}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h4>各区域下园区明细</h4>
                    ${{Object.keys(regionStats).sort((a, b) => regionStats[b].count - regionStats[a].count).map(region => {{
                        const regionData = validData.filter(d => d.所属区域 === region);
                        const parkStatsInRegion = {{}};
                        regionData.forEach(d => {{
                            const park = d.园区 || '未知';
                            if (!parkStatsInRegion[park]) {{
                                parkStatsInRegion[park] = {{count: 0, amount: 0}};
                            }}
                            parkStatsInRegion[park].count++;
                            parkStatsInRegion[park].amount += parseFloat(d.拟定金额) || 0;
                        }});
                        return `
                            <div class="expander">
                                <div class="expander-header" onclick="toggleExpander(this)">
                                    <span><strong>${{region}}</strong>（${{Object.keys(parkStatsInRegion).length}}个园区，${{regionStats[region].count}}个项目，${{formatCurrency(regionStats[region].amount)}}万元）</span>
                                    <span class="expander-icon">▶</span>
                                </div>
                                <div class="expander-content">
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>园区</th><th>项目数</th><th>金额合计（万元）</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{Object.keys(parkStatsInRegion).sort((a, b) => parkStatsInRegion[b].amount - parkStatsInRegion[a].amount).map(park => `
                                                    <tr>
                                                        <td>${{park}}</td>
                                                        <td>${{parkStatsInRegion[park].count}}</td>
                                                        <td>${{formatCurrency(parkStatsInRegion[park].amount)}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                        `;
                    }}).join('')}}
                    ` : ''}}
                    
                    <h3>📈 项目分级占比统计</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>项目类别</th><th>项目数</th><th>项目数占比(%)</th><th>金额合计（万元）</th><th>金额占比(%)</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(levelStatsMapped).map(level => {{
                                    const count = levelStatsMapped[level].count;
                                    const amount = levelStatsMapped[level].amount;
                                    const countPercent = totalCount > 0 ? (count / totalCount * 100).toFixed(2) : 0;
                                    const amountPercent = totalAmount > 0 ? (amount / totalAmount * 100).toFixed(2) : 0;
                                    return `
                                        <tr>
                                            <td>${{level}}</td>
                                            <td>${{count}}</td>
                                            <td>${{countPercent}}%</td>
                                            <td>${{formatCurrency(amount)}}</td>
                                            <td>${{amountPercent}}%</td>
                                        </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <div class="chart-container">
                        <div id="chart-level-count"></div>
                    </div>
                    <div class="chart-container">
                        <div id="chart-level-amount"></div>
                    </div>
                    
                    ${{(validData[0] && (validData[0].专业分包 || validData[0].专业细分)) ? `
                    <h3>📦 按专业分包统计</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>专业分包</th><th>项目数</th><th>项目数占比(%)</th><th>金额合计（万元）</th><th>金额占比(%)</th></tr>
                            </thead>
                            <tbody>
                                ${{(() => {{
                                    const profSubcontractCol = validData[0].专业分包 ? '专业分包' : '专业细分';
                                    const profSubcontractStats = {{}};
                                    validData.forEach(d => {{
                                        const val = d[profSubcontractCol] || '未分类';
                                        if (!profSubcontractStats[val]) {{
                                            profSubcontractStats[val] = {{count: 0, amount: 0}};
                                        }}
                                        profSubcontractStats[val].count++;
                                        profSubcontractStats[val].amount += parseFloat(d.拟定金额) || 0;
                                    }});
                                    const totalCount = validData.length;
                                    const totalAmount = validData.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0);
                                    return Object.keys(profSubcontractStats).sort((a, b) => profSubcontractStats[b].amount - profSubcontractStats[a].amount).map(key => {{
                                        const stats = profSubcontractStats[key];
                                        const countPercent = totalCount > 0 ? (stats.count / totalCount * 100).toFixed(2) : 0;
                                        const amountPercent = totalAmount > 0 ? (stats.amount / totalAmount * 100).toFixed(2) : 0;
                                        return `
                                            <tr>
                                                <td>${{key || '未分类'}}</td>
                                                <td>${{stats.count}}</td>
                                                <td>${{countPercent}}%</td>
                                                <td>${{formatCurrency(stats.amount)}}</td>
                                                <td>${{amountPercent}}%</td>
                                            </tr>
                                        `;
                                    }}).join('');
                                }})()}}
                            </tbody>
                        </table>
                    </div>
                    
                    <div class="chart-container">
                        <div id="chart-prof-subcontract-count"></div>
                    </div>
                    <div class="chart-container">
                        <div id="chart-prof-subcontract-amount"></div>
                    </div>
                    
                    <h4>专业与专业分包交叉统计</h4>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>专业</th><th>专业分包</th><th>项目数</th><th>金额合计（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{(() => {{
                                    const profSubcontractCol = validData[0].专业分包 ? '专业分包' : '专业细分';
                                    const crossStats = {{}};
                                    validData.forEach(d => {{
                                        const prof = d.专业 || '未分类';
                                        const subcontract = d[profSubcontractCol] || '未分类';
                                        // 过滤掉"其它系统"分类
                                        if (prof === '其它系统' || prof === '其他系统') return;
                                        const key = prof + '|' + subcontract;
                                        if (!crossStats[key]) {{
                                            crossStats[key] = {{prof: prof, subcontract: subcontract, count: 0, amount: 0}};
                                        }}
                                        crossStats[key].count++;
                                        crossStats[key].amount += parseFloat(d.拟定金额) || 0;
                                    }});
                                    return Object.keys(crossStats).sort((a, b) => crossStats[b].amount - crossStats[a].amount).map(key => {{
                                        const stats = crossStats[key];
                                        return `
                                            <tr>
                                                <td>${{stats.prof || '未分类'}}</td>
                                                <td>${{stats.subcontract || '未分类'}}</td>
                                                <td>${{stats.count}}</td>
                                                <td>${{formatCurrency(stats.amount)}}</td>
                                            </tr>
                                        `;
                                    }}).join('');
                                }})()}}
                            </tbody>
                        </table>
                    </div>
                    ` : ''}}
                    
                    ${{implCol ? `
                    <h3>🔧 项目实施状态分析</h3>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">已实施项目数</div>
                            <div class="metric-value">${{已实施项目.length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">已实施金额（万元）</div>
                            <div class="metric-value">${{formatCurrency(已实施项目.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">未实施项目数</div>
                            <div class="metric-value">${{未实施项目.length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">未实施金额（万元）</div>
                            <div class="metric-value">${{formatCurrency(未实施项目.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">实施率</div>
                            <div class="metric-value">${{validData.length > 0 ? (已实施项目.length / validData.length * 100).toFixed(1) : 0}}%</div>
                        </div>
                    </div>
                    
                    <h4>各园区实施情况统计</h4>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>园区</th><th>总项目数</th><th>已实施数</th><th>未实施数</th><th>总金额（万元）</th><th>已实施金额（万元）</th><th>实施率(%)</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(parkImplStats).sort((a, b) => parkImplStats[b].amount - parkImplStats[a].amount).map(park => {{
                                    const stats = parkImplStats[park];
                                    const rate = stats.total > 0 ? (stats.implemented / stats.total * 100).toFixed(1) : 0;
                                    return `
                                        <tr>
                                            <td>${{park}}</td>
                                            <td>${{stats.total}}</td>
                                            <td>${{stats.implemented}}</td>
                                            <td>${{stats.total - stats.implemented}}</td>
                                            <td>${{formatCurrency(stats.amount)}}</td>
                                            <td>${{formatCurrency(stats.implAmount)}}</td>
                                            <td>${{rate}}%</td>
                                        </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                    ` : '<div class="info-box">未找到实施日期列，无法进行实施状态分析。</div>'
                    }}
                    
                    <h3>🏢 各园区分类项目统计</h3>
                    
                    ${{(() => {{
                        const totalLevel1 = Object.values(parkAnalysis).reduce((sum, s) => sum + s.level1, 0);
                        const totalHq = Object.values(parkAnalysis).reduce((sum, s) => sum + s.hq, 0);
                        const totalMajor = Object.values(parkAnalysis).reduce((sum, s) => sum + s.major, 0);
                        const totalAll = Object.values(parkAnalysis).reduce((sum, s) => sum + s.total, 0);
                        return `
                            <div class="metrics">
                                <div class="metric">
                                    <div class="metric-label">一级项目总金额</div>
                                    <div class="metric-value">${{formatCurrency(totalLevel1)}} 万元</div>
                                </div>
                                <div class="metric">
                                    <div class="metric-label">总部项目总金额</div>
                                    <div class="metric-value">${{formatCurrency(totalHq)}} 万元</div>
                                </div>
                                <div class="metric">
                                    <div class="metric-label">重大改造项目总金额</div>
                                    <div class="metric-value">${{formatCurrency(totalMajor)}} 万元</div>
                                </div>
                                <div class="metric">
                                    <div class="metric-label">所有项目总金额</div>
                                    <div class="metric-value">${{formatCurrency(totalAll)}} 万元</div>
                                </div>
                            </div>
                        `;
                    }})()}}
                    
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>园区</th><th>一级项目金额（万元）</th><th>一级项目占比(%)</th><th>总部项目金额（万元）</th><th>总部项目占比(%)</th><th>重大改造项目数</th><th>重大改造项目金额（万元）</th><th>重大改造项目占比(%)</th><th>总金额（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(parkAnalysis).sort((a, b) => parkAnalysis[b].total - parkAnalysis[a].total).map(park => {{
                                    const stats = parkAnalysis[park];
                                    const level1Percent = stats.total > 0 ? (stats.level1 / stats.total * 100).toFixed(2) : 0;
                                    const hqPercent = stats.total > 0 ? (stats.hq / stats.total * 100).toFixed(2) : 0;
                                    const majorPercent = stats.total > 0 ? (stats.major / stats.total * 100).toFixed(2) : 0;
                                    return `
                                        <tr>
                                            <td>${{park}}</td>
                                            <td>${{formatCurrency(stats.level1)}}</td>
                                            <td>${{level1Percent}}%</td>
                                            <td>${{formatCurrency(stats.hq)}}</td>
                                            <td>${{hqPercent}}%</td>
                                            <td>${{stats.majorCount}}</td>
                                            <td>${{formatCurrency(stats.major)}}</td>
                                            <td>${{majorPercent}}%</td>
                                            <td>${{formatCurrency(stats.total)}}</td>
                                        </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h4>各园区分类项目统计（金额、项目数与占比）</h4>
                    <div class="chart-container">
                        <div id="chart-park-combined"></div>
                    </div>
                    
                    <h4>各园区分类项目金额统计（对数刻度）</h4>
                    <div class="chart-container">
                        <div id="chart-park-log-scale"></div>
                    </div>
                    
                    <h4>各园区分类项目统计（单独图表）</h4>
                    <div class="chart-container">
                        <div id="chart-park-level1"></div>
                    </div>
                    <div class="chart-container">
                        <div id="chart-park-hq"></div>
                    </div>
                    <div class="chart-container">
                        <div id="chart-park-major-amount"></div>
                    </div>
                    <div class="chart-container">
                        <div id="chart-park-major-count"></div>
                    </div>
                    
                    ${{立项Col ? `
                    <h3>✅ 项目确定状态分析</h3>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">已确定项目数（有立项日期）</div>
                            <div class="metric-value">${{确定项目.length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">已确定金额合计（万元）</div>
                            <div class="metric-value">${{formatCurrency(确定项目.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">未确定项目数（无立项日期）</div>
                            <div class="metric-value">${{未确定项目.length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">未确定金额合计（万元）</div>
                            <div class="metric-value">${{formatCurrency(未确定项目.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0))}}</div>
                        </div>
                    </div>
                    
                    <h4>确定率统计</h4>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>园区</th><th>总项目数</th><th>已确定数</th><th>未确定数</th><th>确定率(%)</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(parkDeterminationStats).map(park => {{
                                    const stats = parkDeterminationStats[park];
                                    const rate = stats.total > 0 ? (stats.determined / stats.total * 100).toFixed(1) : 0;
                                    return `
                                        <tr>
                                            <td>${{park}}</td>
                                            <td>${{stats.total}}</td>
                                            <td>${{stats.determined}}</td>
                                            <td>${{stats.total - stats.determined}}</td>
                                            <td>${{rate}}%</td>
                                        </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    ${{Object.keys(monthlyStats).length > 0 ? `
                    <h3>📅 按月份统计立项</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>立项月份</th><th>立项项目数</th><th>立项金额（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(monthlyStats).sort().map(month => `
                                    <tr>
                                        <td>${{month}}</td>
                                        <td>${{monthlyStats[month].count}}</td>
                                        <td>${{formatCurrency(monthlyStats[month].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <div class="chart-container">
                        <div id="chart-monthly-count"></div>
                    </div>
                    <div class="chart-container">
                        <div id="chart-monthly-amount"></div>
                    </div>
                    ` : ''
                    }}
                    ` : '<div class="info-box">未找到立项日期列，无法进行确定/未确定项目分析。</div>'
                    }}
                </div>
            `;
            
            container.innerHTML = html;
            
            // 渲染图表
            setTimeout(() => {{
                const levelLabels = Object.keys(levelStatsMapped);
                const levelCounts = levelLabels.map(l => levelStatsMapped[l].count);
                const levelAmounts = levelLabels.map(l => levelStatsMapped[l].amount);
                
                Plotly.newPlot('chart-level-count', [{{
                    values: levelCounts,
                    labels: levelLabels,
                    type: 'pie',
                    textinfo: 'label+percent+value',
                    textposition: 'outside',
                    marker: {{colors: ['#FF6B6B', '#4ECDC4', '#45B7D1']}}
                }}], {{
                    title: '项目数量占比',
                    showlegend: true
                }}, {{displayModeBar: false}});
                
                Plotly.newPlot('chart-level-amount', [{{
                    values: levelAmounts,
                    labels: levelLabels,
                    type: 'pie',
                    textinfo: 'label+percent+value',
                    textposition: 'outside',
                    marker: {{colors: ['#FF6B6B', '#4ECDC4', '#45B7D1']}}
                }}], {{
                    title: '项目金额占比',
                    showlegend: true
                }}, {{displayModeBar: false}});
                
                // 各园区分类项目图表
                const parkLabels = Object.keys(parkAnalysis).sort((a, b) => parkAnalysis[b].total - parkAnalysis[a].total);
                const level1Amounts = parkLabels.map(p => parkAnalysis[p].level1);
                const hqAmounts = parkLabels.map(p => parkAnalysis[p].hq);
                const majorAmounts = parkLabels.map(p => parkAnalysis[p].major);
                const majorCounts = parkLabels.map(p => parkAnalysis[p].majorCount);
                const level1Percents = parkLabels.map(p => parkAnalysis[p].total > 0 ? (parkAnalysis[p].level1 / parkAnalysis[p].total * 100).toFixed(2) : 0);
                const hqPercents = parkLabels.map(p => parkAnalysis[p].total > 0 ? (parkAnalysis[p].hq / parkAnalysis[p].total * 100).toFixed(2) : 0);
                const majorPercents = parkLabels.map(p => parkAnalysis[p].total > 0 ? (parkAnalysis[p].major / parkAnalysis[p].total * 100).toFixed(2) : 0);
                
                // 复杂整合图表（多Y轴）
                const maxAmount = Math.max(...level1Amounts, ...hqAmounts, ...majorAmounts);
                const maxCount = Math.max(...majorCounts);
                const scaleFactor = maxCount > 0 && maxAmount > 0 ? maxAmount / (maxCount * 50) : 1;
                const scaledCounts = majorCounts.map(c => c * scaleFactor);
                
                Plotly.newPlot('chart-park-combined', [
                    // 一级项目金额
                    {{
                        x: parkLabels,
                        y: level1Amounts,
                        type: 'bar',
                        name: '一级项目金额（万元）',
                        marker: {{color: '#5470c6', line: {{color: '#3a5a9c', width: 1}}}},
                        text: level1Amounts.map(a => a > 0 ? formatCurrency(a) + '万' : ''),
                        textposition: 'outside',
                        yaxis: 'y'
                    }},
                    // 总部项目金额
                    {{
                        x: parkLabels,
                        y: hqAmounts,
                        type: 'bar',
                        name: '总部项目金额（万元）',
                        marker: {{color: '#91cc75', line: {{color: '#6fa85a', width: 1}}}},
                        text: hqAmounts.map(a => a > 0 ? formatCurrency(a) + '万' : ''),
                        textposition: 'outside',
                        yaxis: 'y'
                    }},
                    // 重大改造项目金额
                    {{
                        x: parkLabels,
                        y: majorAmounts,
                        type: 'bar',
                        name: '重大改造项目金额（万元）',
                        marker: {{color: '#fac858', line: {{color: '#d4a84a', width: 1}}}},
                        text: majorAmounts.map(a => a > 0 ? formatCurrency(a) + '万' : ''),
                        textposition: 'outside',
                        yaxis: 'y'
                    }},
                    // 重大改造项目数量（缩放后）
                    {{
                        x: parkLabels,
                        y: scaledCounts,
                        type: 'bar',
                        name: '重大改造项目数（个）',
                        marker: {{color: '#73c0de', line: {{color: '#4a9bc0', width: 1.5}}}},
                        text: majorCounts.map(c => c > 0 ? c + '个' : ''),
                        textposition: 'inside',
                        opacity: 0.85,
                        yaxis: 'y'
                    }},
                    // 一级项目占比
                    {{
                        x: parkLabels,
                        y: level1Percents,
                        type: 'scatter',
                        mode: 'lines+markers',
                        name: '一级项目占比（%）',
                        marker: {{color: '#ee6666', size: 10, line: {{width: 2, color: 'white'}}, symbol: 'circle'}},
                        line: {{color: '#ee6666', width: 3}},
                        yaxis: 'y2'
                    }},
                    // 总部项目占比
                    {{
                        x: parkLabels,
                        y: hqPercents,
                        type: 'scatter',
                        mode: 'lines+markers',
                        name: '总部项目占比（%）',
                        marker: {{color: '#ff9800', size: 10, line: {{width: 2, color: 'white'}}, symbol: 'square'}},
                        line: {{color: '#ff9800', width: 3, dash: 'dash'}},
                        yaxis: 'y2'
                    }},
                    // 重大改造项目占比
                    {{
                        x: parkLabels,
                        y: majorPercents,
                        type: 'scatter',
                        mode: 'lines+markers',
                        name: '重大改造项目占比（%）',
                        marker: {{color: '#9c27b0', size: 10, line: {{width: 2, color: 'white'}}, symbol: 'diamond'}},
                        line: {{color: '#9c27b0', width: 3, dash: 'dot'}},
                        yaxis: 'y2'
                    }}
                ], {{
                    title: '各园区分类项目统计（金额、项目数与占比）',
                    xaxis: {{tickangle: -45, title: '园区'}},
                    yaxis: {{title: '金额（万元）', side: 'left'}},
                    yaxis2: {{title: '占比（%）', side: 'right', overlaying: 'y', range: [0, 105]}},
                    barmode: 'group',
                    height: 700,
                    showlegend: true,
                    legend: {{orientation: 'h', yanchor: 'bottom', y: -0.18, xanchor: 'center', x: 0.5}}
                }}, {{displayModeBar: false}});
                
                // 对数刻度图表
                const maxTotal = Math.max(...parkLabels.map(p => parkAnalysis[p].total));
                const minTotal = Math.min(...parkLabels.filter(p => parkAnalysis[p].total > 0).map(p => parkAnalysis[p].total));
                let tickVals = null;
                let tickTexts = null;
                if (maxTotal > 0 && minTotal > 0 && maxTotal > minTotal * 2) {{
                    const logMin = Math.log10(Math.max(1, minTotal));
                    const logMax = Math.log10(maxTotal);
                    tickVals = [];
                    tickTexts = [];
                    for (let exp = Math.floor(logMin); exp <= Math.ceil(logMax); exp++) {{
                        for (let mult of [1, 2, 5]) {{
                            const val = mult * Math.pow(10, exp);
                            if (val >= Math.max(1, minTotal * 0.5) && val <= maxTotal * 1.5) {{
                                tickVals.push(val);
                                if (val >= 1000) {{
                                    tickTexts.push((val / 1000).toFixed(1) + '千');
                                }} else if (val >= 100) {{
                                    tickTexts.push(Math.floor(val).toString());
                                }} else {{
                                    tickTexts.push(val.toString());
                                }}
                            }}
                        }}
                    }}
                    // 去重并排序
                    const pairs = Array.from(new Set(tickVals.map((v, i) => [v, tickTexts[i]]))).sort((a, b) => a[0] - b[0]);
                    tickVals = pairs.map(p => p[0]);
                    tickTexts = pairs.map(p => p[1]);
                }}
                
                Plotly.newPlot('chart-park-log-scale', [
                    {{
                        x: parkLabels,
                        y: level1Amounts,
                        type: 'bar',
                        name: '一级项目金额（万元）',
                        marker: {{color: '#5470c6', line: {{color: '#3a5a9c', width: 1}}}},
                        text: level1Amounts.map(a => a > 0 ? formatCurrency(a) : ''),
                        textposition: 'outside'
                    }},
                    {{
                        x: parkLabels,
                        y: hqAmounts,
                        type: 'bar',
                        name: '总部项目金额（万元）',
                        marker: {{color: '#91cc75', line: {{color: '#6fa85a', width: 1}}}},
                        text: hqAmounts.map(a => a > 0 ? formatCurrency(a) : ''),
                        textposition: 'outside'
                    }},
                    {{
                        x: parkLabels,
                        y: majorAmounts,
                        type: 'bar',
                        name: '重大改造项目金额（万元）',
                        marker: {{color: '#fac858', line: {{color: '#d4a84a', width: 1}}}},
                        text: majorAmounts.map(a => a > 0 ? formatCurrency(a) : ''),
                        textposition: 'outside'
                    }}
                ], {{
                    title: '各园区分类项目金额统计（对数刻度，保证小金额园区可见性）',
                    xaxis: {{tickangle: -45, title: '园区'}},
                    yaxis: {{
                        title: '金额（万元，对数刻度）',
                        type: 'log',
                        tickvals: tickVals,
                        ticktext: tickTexts
                    }},
                    barmode: 'group',
                    height: 600,
                    showlegend: true,
                    legend: {{orientation: 'h', yanchor: 'bottom', y: -0.15, xanchor: 'center', x: 0.5}}
                }}, {{displayModeBar: false}});
                
                Plotly.newPlot('chart-park-level1', [{{
                    x: parkLabels,
                    y: level1Amounts,
                    type: 'bar',
                    text: level1Amounts.map(a => formatCurrency(a)),
                    textposition: 'outside',
                    marker: {{color: '#FF6B6B'}}
                }}], {{
                    title: '各园区一级项目金额（万元）',
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '金额（万元）'}},
                    showlegend: false,
                    height: 350
                }}, {{displayModeBar: false}});
                
                Plotly.newPlot('chart-park-hq', [{{
                    x: parkLabels,
                    y: hqAmounts,
                    type: 'bar',
                    text: hqAmounts.map(a => formatCurrency(a)),
                    textposition: 'outside',
                    marker: {{color: '#4ECDC4'}}
                }}], {{
                    title: '各园区总部项目金额（万元）',
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '金额（万元）'}},
                    showlegend: false,
                    height: 350
                }}, {{displayModeBar: false}});
                
                Plotly.newPlot('chart-park-major-amount', [{{
                    x: parkLabels,
                    y: majorAmounts,
                    type: 'bar',
                    text: majorAmounts.map(a => formatCurrency(a)),
                    textposition: 'outside',
                    marker: {{color: '#45B7D1'}}
                }}], {{
                    title: '各园区重大改造项目金额（万元，≥200万）',
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '金额（万元）'}},
                    showlegend: false,
                    height: 350
                }}, {{displayModeBar: false}});
                
                Plotly.newPlot('chart-park-major-count', [{{
                    x: parkLabels,
                    y: majorCounts,
                    type: 'bar',
                    text: majorCounts,
                    textposition: 'outside',
                    marker: {{color: '#9a60b4'}}
                }}], {{
                    title: '各园区重大改造项目数量（≥200万）',
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '项目数'}},
                    showlegend: false,
                    height: 350
                }}, {{displayModeBar: false}});
                
                // 按月份统计图表
                if (Object.keys(monthlyStats).length > 0) {{
                    const months = Object.keys(monthlyStats).sort();
                    const monthlyCounts = months.map(m => monthlyStats[m].count);
                    const monthlyAmounts = months.map(m => monthlyStats[m].amount);
                    
                    Plotly.newPlot('chart-monthly-count', [{{
                        x: months,
                        y: monthlyCounts,
                        type: 'bar',
                        text: monthlyCounts,
                        textposition: 'outside',
                        marker: {{color: '#5470c6'}}
                    }}], {{
                        title: '每月立项项目数',
                        xaxis: {{tickangle: -45}},
                        yaxis: {{title: '项目数'}},
                        showlegend: false,
                        height: 350
                    }}, {{displayModeBar: false}});
                    
                    Plotly.newPlot('chart-monthly-amount', [{{
                        x: months,
                        y: monthlyAmounts,
                        type: 'bar',
                        text: monthlyAmounts.map(a => formatCurrency(a)),
                        textposition: 'outside',
                        marker: {{color: '#91cc75'}}
                    }}], {{
                        title: '每月立项金额（万元）',
                        xaxis: {{tickangle: -45}},
                        yaxis: {{title: '金额（万元）'}},
                        showlegend: false,
                        height: 350
                    }}, {{displayModeBar: false}});
                }}
                
                // 专业分包统计图表
                if (validData[0] && (validData[0].专业分包 || validData[0].专业细分)) {{
                    const profSubcontractCol = validData[0].专业分包 ? '专业分包' : '专业细分';
                    const profSubcontractStats = {{}};
                    validData.forEach(d => {{
                        const val = d[profSubcontractCol] || '未分类';
                        if (!profSubcontractStats[val]) {{
                            profSubcontractStats[val] = {{count: 0, amount: 0}};
                        }}
                        profSubcontractStats[val].count++;
                        profSubcontractStats[val].amount += parseFloat(d.拟定金额) || 0;
                    }});
                    
                    const profSubcontractLabels = Object.keys(profSubcontractStats).sort((a, b) => profSubcontractStats[b].amount - profSubcontractStats[a].amount);
                    const profSubcontractCounts = profSubcontractLabels.map(l => profSubcontractStats[l].count);
                    const profSubcontractAmounts = profSubcontractLabels.map(l => profSubcontractStats[l].amount);
                    
                    const colors = ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452', '#9a60b4'];
                    
                    Plotly.newPlot('chart-prof-subcontract-count', [{{
                        values: profSubcontractCounts,
                        labels: profSubcontractLabels,
                        type: 'pie',
                        textinfo: 'label+percent+value',
                        textposition: 'outside',
                        marker: {{colors: colors.slice(0, profSubcontractLabels.length)}}
                    }}], {{
                        title: '专业分包项目数占比',
                        showlegend: true
                    }}, {{displayModeBar: false}});
                    
                    Plotly.newPlot('chart-prof-subcontract-amount', [{{
                        values: profSubcontractAmounts,
                        labels: profSubcontractLabels,
                        type: 'pie',
                        textinfo: 'label+percent+value',
                        textposition: 'outside',
                        marker: {{colors: colors.slice(0, profSubcontractLabels.length)}}
                    }}], {{
                        title: '专业分包金额占比',
                        showlegend: true
                    }}, {{displayModeBar: false}});
                }}
            }}, 100);
        }}
        
        // 标签页1: 统计
        function renderTab1() {{
            const validData = getValidProjects(filteredData);
            const container = document.getElementById('tab-1');
            
            if (validData.length === 0) {{
                container.innerHTML = '<div class="warning-box">当前筛选条件下暂无数据。</div>';
                return;
            }}
            
            // 按专业统计（过滤掉"其它系统"）
            const profStats = {{}};
            validData.forEach(d => {{
                const prof = d.专业 || '未分类';
                // 过滤掉"其它系统"分类
                if (prof === '其它系统' || prof === '其他系统') return;
                if (!profStats[prof]) {{
                    profStats[prof] = {{count: 0, amount: 0}};
                }}
                profStats[prof].count++;
                profStats[prof].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按项目分级统计金额
            const levelAmountStats = {{}};
            validData.forEach(d => {{
                const level = d.项目分级 || '未分类';
                if (!levelAmountStats[level]) {{
                    levelAmountStats[level] = 0;
                }}
                levelAmountStats[level] += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按园区统计金额
            const parkAmountStats = {{}};
            validData.forEach(d => {{
                const park = d.园区 || '未知';
                if (!parkAmountStats[park]) {{
                    parkAmountStats[park] = 0;
                }}
                parkAmountStats[park] += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按城市统计金额
            const cityAmountStats = {{}};
            validData.forEach(d => {{
                const city = d.城市 || '其他';
                if (city !== '其他') {{
                    if (!cityAmountStats[city]) {{
                        cityAmountStats[city] = 0;
                    }}
                    cityAmountStats[city] += parseFloat(d.拟定金额) || 0;
                }}
            }});
            
            // 按区域统计金额
            const regionAmountStats = {{}};
            validData.forEach(d => {{
                const region = d.所属区域 || '其他';
                if (region !== '其他') {{
                    if (!regionAmountStats[region]) {{
                        regionAmountStats[region] = 0;
                    }}
                    regionAmountStats[region] += parseFloat(d.拟定金额) || 0;
                }}
            }});
            
            // 按专业分包统计（如果存在）
            const hasProfSubcontract = validData[0] && (validData[0].专业分包 || validData[0].专业细分);
            const profSubcontractCol = hasProfSubcontract ? (validData[0].专业分包 ? '专业分包' : '专业细分') : null;
            const profSubcontractStats = {{}};
            if (hasProfSubcontract) {{
                validData.forEach(d => {{
                    const val = d[profSubcontractCol] || '未分类';
                    if (!profSubcontractStats[val]) {{
                        profSubcontractStats[val] = {{count: 0, amount: 0}};
                    }}
                    profSubcontractStats[val].count++;
                    profSubcontractStats[val].amount += parseFloat(d.拟定金额) || 0;
                }});
            }}
            
            // 按区域统计（详细统计，包含项目数、金额、园区数）
            const regionDetailedStats = {{}};
            validData.forEach(d => {{
                const region = d.所属区域 || '其他';
                if (region !== '其他') {{
                    if (!regionDetailedStats[region]) {{
                        regionDetailedStats[region] = {{count: 0, amount: 0, parks: new Set()}};
                    }}
                    regionDetailedStats[region].count++;
                    regionDetailedStats[region].amount += parseFloat(d.拟定金额) || 0;
                    if (d.园区) regionDetailedStats[region].parks.add(d.园区);
                }}
            }});
            
            // 按区域下各园区统计
            const regionParkDetails = {{}};
            Object.keys(regionDetailedStats).forEach(region => {{
                const regionData = validData.filter(d => d.所属区域 === region);
                const parkStatsInRegion = {{}};
                regionData.forEach(d => {{
                    const park = d.园区 || '未知';
                    if (!parkStatsInRegion[park]) {{
                        parkStatsInRegion[park] = {{count: 0, amount: 0}};
                    }}
                    parkStatsInRegion[park].count++;
                    parkStatsInRegion[park].amount += parseFloat(d.拟定金额) || 0;
                }});
                regionParkDetails[region] = parkStatsInRegion;
            }});
            
            let html = `
                <div class="section">
                    ${{Object.keys(regionDetailedStats).length > 0 ? `
                    <h2>📊 按区域统计分析</h2>
                    
                    <h3>各区域项目统计</h3>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">总区域数</div>
                            <div class="metric-value">${{Object.keys(regionDetailedStats).length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总项目数</div>
                            <div class="metric-value">${{formatNumber(Object.values(regionDetailedStats).reduce((sum, r) => sum + r.count, 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总金额（万元）</div>
                            <div class="metric-value">${{formatCurrency(Object.values(regionDetailedStats).reduce((sum, r) => sum + r.amount, 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总园区数</div>
                            <div class="metric-value">${{formatNumber(Object.values(regionDetailedStats).reduce((sum, r) => sum + r.parks.size, 0))}}</div>
                        </div>
                    </div>
                    
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>所属区域</th><th>项目数</th><th>金额合计（万元）</th><th>园区数</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(regionDetailedStats).sort((a, b) => regionDetailedStats[b].count - regionDetailedStats[a].count).map(region => {{
                                    const stats = regionDetailedStats[region];
                                    return `
                                        <tr>
                                            <td>${{region}}</td>
                                            <td>${{stats.count}}</td>
                                            <td>${{formatCurrency(stats.amount)}}</td>
                                            <td>${{stats.parks.size}}</td>
                                        </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h3>各区域下园区明细</h3>
                    ${{Object.keys(regionDetailedStats).sort((a, b) => regionDetailedStats[b].count - regionDetailedStats[a].count).map(region => {{
                        const stats = regionDetailedStats[region];
                        const parkDetails = regionParkDetails[region];
                        return `
                            <div class="expander">
                                <div class="expander-header" onclick="toggleExpander(this)">
                                    <span><strong>${{region}}</strong>（${{Object.keys(parkDetails).length}}个园区，${{stats.count}}个项目，${{formatCurrency(stats.amount)}}万元）</span>
                                    <span class="expander-icon">▶</span>
                                </div>
                                <div class="expander-content">
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>园区</th><th>项目数</th><th>金额合计（万元）</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{Object.keys(parkDetails).sort((a, b) => parkDetails[b].amount - parkDetails[a].amount).map(park => `
                                                    <tr>
                                                        <td>${{park}}</td>
                                                        <td>${{parkDetails[park].count}}</td>
                                                        <td>${{formatCurrency(parkDetails[park].amount)}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            </div>
                        `;
                    }}).join('')}}
                    
                    <hr style="margin: 30px 0;"/>
                    ` : ''}}
                    
                    <h2>📊 图表统计</h2>
                    
                    <h3>按专业 · 项目数</h3>
                    <div class="chart-container">
                        <div id="chart-prof-count"></div>
                    </div>
                    
                    <h3>按项目分级 · 金额占比</h3>
                    <div class="chart-container">
                        <div id="chart-level-amount-pie"></div>
                    </div>
                    
                    <h3>按园区 · 金额（万元）</h3>
                    <div class="chart-container">
                        <div id="chart-park-amount"></div>
                    </div>
                    
                    <h3>按城市 · 金额（万元）</h3>
                    <div class="chart-container">
                        <div id="chart-city-amount"></div>
                    </div>
                    
                    <h3>按所属区域 · 金额分布（万元）</h3>
                    <div class="chart-container">
                        <div id="chart-region-amount"></div>
                    </div>
                    
                    <h3>按专业 · 金额合计（万元）</h3>
                    <div class="chart-container">
                        <div id="chart-prof-amount"></div>
                    </div>
                    
                    ${{hasProfSubcontract ? `
                    <h3>按专业分包 · 项目数</h3>
                    <div class="chart-container">
                        <div id="chart-prof-subcontract-count-tab1"></div>
                    </div>
                    
                    <h3>按专业分包 · 金额占比</h3>
                    <div class="chart-container">
                        <div id="chart-prof-subcontract-amount-tab1"></div>
                    </div>
                    ` : ''}}
                </div>
            `;
            
            container.innerHTML = html;
            
            // 渲染图表
            setTimeout(() => {{
                // 按专业项目数
                const profLabels = Object.keys(profStats).sort((a, b) => profStats[b].count - profStats[a].count);
                const profCounts = profLabels.map(p => profStats[p].count);
                Plotly.newPlot('chart-prof-count', [{{
                    x: profLabels,
                    y: profCounts,
                    type: 'bar',
                    marker: {{color: profCounts, colorscale: 'Blues'}},
                    text: profCounts,
                    textposition: 'outside'
                }}], {{
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '项目数'}},
                    showlegend: false,
                    margin: {{t: 20, b: 80}}
                }}, {{displayModeBar: false}});
                
                // 按项目分级金额占比
                const levelLabels = Object.keys(levelAmountStats);
                const levelAmounts = levelLabels.map(l => levelAmountStats[l]);
                Plotly.newPlot('chart-level-amount-pie', [{{
                    values: levelAmounts,
                    labels: levelLabels,
                    type: 'pie',
                    hole: 0.35,
                    textinfo: 'label+percent+value',
                    textposition: 'outside',
                    texttemplate: '%{{label}}<br>%{{percent}}<br>%{{value:,.0f}}万元'
                }}], {{
                    showlegend: true,
                    legend: {{orientation: 'h', yanchor: 'bottom', y: -0.2}}
                }}, {{displayModeBar: false}});
                
                // 按园区金额
                const parkLabels = Object.keys(parkAmountStats).sort((a, b) => parkAmountStats[b] - parkAmountStats[a]).slice(0, 20);
                const parkAmounts = parkLabels.map(p => parkAmountStats[p]);
                Plotly.newPlot('chart-park-amount', [{{
                    x: parkLabels,
                    y: parkAmounts,
                    type: 'bar',
                    marker: {{color: parkAmounts, colorscale: 'Blues'}},
                    text: parkAmounts.map(a => formatCurrency(a)),
                    textposition: 'outside'
                }}], {{
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '金额（万元）'}},
                    showlegend: false,
                    margin: {{t: 20, b: 80}}
                }}, {{displayModeBar: false}});
                
                // 按城市金额
                const cityLabels = Object.keys(cityAmountStats).sort((a, b) => cityAmountStats[b] - cityAmountStats[a]);
                const cityAmounts = cityLabels.map(c => cityAmountStats[c]);
                if (cityLabels.length > 0) {{
                    Plotly.newPlot('chart-city-amount', [{{
                        x: cityLabels,
                        y: cityAmounts,
                        type: 'bar',
                        marker: {{color: cityAmounts, colorscale: 'Teal'}},
                        text: cityAmounts.map(a => formatCurrency(a)),
                        textposition: 'outside'
                    }}], {{
                        xaxis: {{tickangle: -45}},
                        yaxis: {{title: '金额（万元）'}},
                        showlegend: false,
                        margin: {{t: 20, b: 80}}
                    }}, {{displayModeBar: false}});
                }}
                
                // 按区域金额
                const regionLabels = Object.keys(regionAmountStats).sort((a, b) => regionAmountStats[b] - regionAmountStats[a]);
                const regionAmounts = regionLabels.map(r => regionAmountStats[r]);
                if (regionLabels.length > 0) {{
                    Plotly.newPlot('chart-region-amount', [{{
                        values: regionAmounts,
                        labels: regionLabels,
                        type: 'pie',
                        hole: 0.4,
                        textinfo: 'label+percent+value',
                        textposition: 'outside',
                        texttemplate: '%{{label}}<br>%{{percent}}<br>%{{value:,.0f}}万元',
                        marker: {{colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8']}}
                    }}], {{
                        showlegend: true,
                        legend: {{orientation: 'h', yanchor: 'bottom', y: -0.15}}
                    }}, {{displayModeBar: false}});
                }}
                
                // 按专业金额
                const profAmountLabels = Object.keys(profStats).sort((a, b) => profStats[b].amount - profStats[a].amount);
                const profAmounts = profAmountLabels.map(p => profStats[p].amount);
                Plotly.newPlot('chart-prof-amount', [{{
                    x: profAmountLabels,
                    y: profAmounts,
                    type: 'bar',
                    marker: {{color: profAmounts, colorscale: 'Viridis'}},
                    text: profAmounts.map(a => formatCurrency(a)),
                    textposition: 'outside'
                }}], {{
                    xaxis: {{tickangle: -45}},
                    yaxis: {{title: '金额（万元）'}},
                    showlegend: false,
                    margin: {{t: 20, b: 80}}
                }}, {{displayModeBar: false}});
                
                // 按专业分包统计图表
                if (hasProfSubcontract) {{
                    const profSubcontractLabels = Object.keys(profSubcontractStats).sort((a, b) => profSubcontractStats[b].amount - profSubcontractStats[a].amount);
                    const profSubcontractCounts = profSubcontractLabels.map(l => profSubcontractStats[l].count);
                    const profSubcontractAmounts = profSubcontractLabels.map(l => profSubcontractStats[l].amount);
                    const colors = ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3ba272', '#fc8452', '#9a60b4'];
                    
                    Plotly.newPlot('chart-prof-subcontract-count-tab1', [{{
                        x: profSubcontractLabels,
                        y: profSubcontractCounts,
                        type: 'bar',
                        marker: {{color: profSubcontractCounts, colorscale: 'Blues'}},
                        text: profSubcontractCounts,
                        textposition: 'outside'
                    }}], {{
                        title: '按专业分包 · 项目数',
                        xaxis: {{tickangle: -45}},
                        yaxis: {{title: '项目数'}},
                        showlegend: false,
                        margin: {{t: 20, b: 80}},
                        height: 400
                    }}, {{displayModeBar: false}});
                    
                    Plotly.newPlot('chart-prof-subcontract-amount-tab1', [{{
                        values: profSubcontractAmounts,
                        labels: profSubcontractLabels,
                        type: 'pie',
                        hole: 0.35,
                        textinfo: 'label+percent+value',
                        textposition: 'outside',
                        texttemplate: '%{{label}}<br>%{{percent}}<br>%{{value:,.0f}}万元',
                        marker: {{colors: colors.slice(0, profSubcontractLabels.length)}}
                    }}], {{
                        title: '按专业分包 · 金额占比',
                        showlegend: true,
                        legend: {{orientation: 'h', yanchor: 'bottom', y: -0.2}}
                    }}, {{displayModeBar: false}});
                }}
            }}, 100);
        }}
        
        // 标签页2: 地区分析
        function renderTab2() {{
            const validData = getValidProjects(filteredData);
            const container = document.getElementById('tab-2');
            
            if (validData.length === 0) {{
                container.innerHTML = '<div class="warning-box">当前筛选条件下暂无数据。</div>';
                return;
            }}
            
            // 按区域统计
            const regionStats = {{}};
            validData.forEach(d => {{
                const region = d.所属区域 || '其他';
                if (region !== '其他') {{
                    if (!regionStats[region]) {{
                        regionStats[region] = {{
                            count: 0,
                            amount: 0,
                            parks: new Set(),
                            cities: new Set()
                        }};
                    }}
                    regionStats[region].count++;
                    regionStats[region].amount += parseFloat(d.拟定金额) || 0;
                    if (d.园区) regionStats[region].parks.add(d.园区);
                    if (d.城市) regionStats[region].cities.add(d.城市);
                }}
            }});
            
            let html = `
                <div class="section">
                    <h2>🌍 地区分析：按所属区域统计</h2>
                    
                    <h3>📊 区域总览</h3>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">总区域数</div>
                            <div class="metric-value">${{Object.keys(regionStats).length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总项目数</div>
                            <div class="metric-value">${{formatNumber(Object.values(regionStats).reduce((sum, r) => sum + r.count, 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总金额（万元）</div>
                            <div class="metric-value">${{formatCurrency(Object.values(regionStats).reduce((sum, r) => sum + r.amount, 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总园区数</div>
                            <div class="metric-value">${{formatNumber(Object.values(regionStats).reduce((sum, r) => sum + r.parks.size, 0))}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">总城市数</div>
                            <div class="metric-value">${{formatNumber(Object.values(regionStats).reduce((sum, r) => sum + r.cities.size, 0))}}</div>
                        </div>
                    </div>
                    
                    <h4>各区域统计汇总</h4>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>所属区域</th><th>项目数</th><th>金额合计（万元）</th><th>平均项目金额（万元）</th><th>园区数</th><th>城市数</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(regionStats).sort((a, b) => regionStats[b].count - regionStats[a].count).map(region => {{
                                    const stats = regionStats[region];
                                    const avgAmount = stats.count > 0 ? (stats.amount / stats.count).toFixed(2) : 0;
                                    return `
                                        <tr>
                                            <td>${{region}}</td>
                                            <td>${{stats.count}}</td>
                                            <td>${{formatCurrency(stats.amount)}}</td>
                                            <td>${{avgAmount}}</td>
                                            <td>${{stats.parks.size}}</td>
                                            <td>${{stats.cities.size}}</td>
                                        </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h3>📈 区域对比分析</h3>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0;">
                        <div class="chart-container">
                            <div id="chart-region-count-bar"></div>
                        </div>
                        <div class="chart-container">
                            <div id="chart-region-amount-bar"></div>
                        </div>
                        <div class="chart-container">
                            <div id="chart-region-amount-pie"></div>
                        </div>
                        <div class="chart-container">
                            <div id="chart-region-count-pie"></div>
                        </div>
                    </div>
                    
                    <h3>🔍 各区域详细分析</h3>
                    ${{Object.keys(regionStats).sort((a, b) => regionStats[b].count - regionStats[a].count).map(region => {{
                        const stats = regionStats[region];
                        const regionData = validData.filter(d => d.所属区域 === region);
                        
                        // 按园区统计
                        const parkStatsInRegion = {{}};
                        regionData.forEach(d => {{
                            const park = d.园区 || '未知';
                            if (!parkStatsInRegion[park]) {{
                                parkStatsInRegion[park] = {{count: 0, amount: 0}};
                            }}
                            parkStatsInRegion[park].count++;
                            parkStatsInRegion[park].amount += parseFloat(d.拟定金额) || 0;
                        }});
                        
                        // 按专业统计（过滤掉"其它系统"）
                        const profStatsInRegion = {{}};
                        regionData.forEach(d => {{
                            const prof = d.专业 || '未分类';
                            // 过滤掉"其它系统"分类
                            if (prof === '其它系统' || prof === '其他系统') return;
                            if (!profStatsInRegion[prof]) {{
                                profStatsInRegion[prof] = {{count: 0, amount: 0}};
                            }}
                            profStatsInRegion[prof].count++;
                            profStatsInRegion[prof].amount += parseFloat(d.拟定金额) || 0;
                        }});
                        
                        // 按城市统计
                        const cityStatsInRegion = {{}};
                        regionData.forEach(d => {{
                            const city = d.城市 || '未知';
                            if (!cityStatsInRegion[city]) {{
                                cityStatsInRegion[city] = {{count: 0, amount: 0, parks: new Set()}};
                            }}
                            cityStatsInRegion[city].count++;
                            cityStatsInRegion[city].amount += parseFloat(d.拟定金额) || 0;
                            if (d.园区) cityStatsInRegion[city].parks.add(d.园区);
                        }});
                        
                        // 按项目分级统计
                        const levelStatsInRegion = {{}};
                        regionData.forEach(d => {{
                            const level = d.项目分级 || '未分类';
                            if (!levelStatsInRegion[level]) {{
                                levelStatsInRegion[level] = {{count: 0, amount: 0}};
                            }}
                            levelStatsInRegion[level].count++;
                            levelStatsInRegion[level].amount += parseFloat(d.拟定金额) || 0;
                        }});
                        
                        return `
                            <div class="expander">
                                <div class="expander-header" onclick="toggleExpander(this)">
                                    <span><strong>${{region}}</strong> - ${{stats.parks.size}}个园区，${{stats.count}}个项目，${{formatCurrency(stats.amount)}}万元</span>
                                    <span class="expander-icon">▶</span>
                                </div>
                                <div class="expander-content">
                                    <div class="metrics">
                                        <div class="metric">
                                            <div class="metric-label">项目数</div>
                                            <div class="metric-value">${{stats.count}}</div>
                                        </div>
                                        <div class="metric">
                                            <div class="metric-label">金额合计（万元）</div>
                                            <div class="metric-value">${{formatCurrency(stats.amount)}}</div>
                                        </div>
                                        <div class="metric">
                                            <div class="metric-label">园区数</div>
                                            <div class="metric-value">${{stats.parks.size}}</div>
                                        </div>
                                        <div class="metric">
                                            <div class="metric-label">城市数</div>
                                            <div class="metric-value">${{stats.cities.size}}</div>
                                        </div>
                                    </div>
                                    
                                    <h4>各园区统计</h4>
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>园区</th><th>项目数</th><th>金额合计（万元）</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{Object.keys(parkStatsInRegion).sort((a, b) => parkStatsInRegion[b].amount - parkStatsInRegion[a].amount).map(park => `
                                                    <tr>
                                                        <td>${{park}}</td>
                                                        <td>${{parkStatsInRegion[park].count}}</td>
                                                        <td>${{formatCurrency(parkStatsInRegion[park].amount)}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                    
                                    <h4>各城市统计</h4>
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>城市</th><th>项目数</th><th>金额合计（万元）</th><th>园区数</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{Object.keys(cityStatsInRegion).sort((a, b) => cityStatsInRegion[b].count - cityStatsInRegion[a].count).map(city => `
                                                    <tr>
                                                        <td>${{city}}</td>
                                                        <td>${{cityStatsInRegion[city].count}}</td>
                                                        <td>${{formatCurrency(cityStatsInRegion[city].amount)}}</td>
                                                        <td>${{cityStatsInRegion[city].parks.size}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                    
                                    <h4>按专业分类统计</h4>
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>专业</th><th>项目数</th><th>金额合计（万元）</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{Object.keys(profStatsInRegion).sort((a, b) => profStatsInRegion[b].amount - profStatsInRegion[a].amount).map(prof => `
                                                    <tr>
                                                        <td>${{prof}}</td>
                                                        <td>${{profStatsInRegion[prof].count}}</td>
                                                        <td>${{formatCurrency(profStatsInRegion[prof].amount)}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                    
                                    <h4>按项目分级统计</h4>
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>项目分级</th><th>项目数</th><th>金额合计（万元）</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{Object.keys(levelStatsInRegion).sort((a, b) => levelStatsInRegion[b].count - levelStatsInRegion[a].count).map(level => `
                                                    <tr>
                                                        <td>${{level || '未分类'}}</td>
                                                        <td>${{levelStatsInRegion[level].count}}</td>
                                                        <td>${{formatCurrency(levelStatsInRegion[level].amount)}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                    
                                    <h4>项目明细（前20条）</h4>
                                    <div class="data-table-container">
                                        <table>
                                            <thead>
                                                <tr><th>园区</th><th>城市</th><th>序号</th><th>项目分级</th>${{regionData[0] && regionData[0].项目分类 ? '<th>项目分类</th>' : ''}}<th>专业</th><th>项目名称</th><th>拟定金额</th></tr>
                                            </thead>
                                            <tbody>
                                                ${{regionData.slice(0, 20).map(d => `
                                                    <tr>
                                                        <td>${{getValue(d, '园区')}}</td>
                                                        <td>${{getValue(d, '城市')}}</td>
                                                        <td>${{getValue(d, '序号')}}</td>
                                                        <td>${{getValue(d, '项目分级')}}</td>
                                                        ${{d.项目分类 ? `<td>${{getValue(d, '项目分类')}}</td>` : ''}}
                                                        <td>${{getValue(d, '专业')}}</td>
                                                        <td>${{getValue(d, '项目名称')}}</td>
                                                        <td>${{formatCurrency(getValue(d, '拟定金额'))}}</td>
                                                    </tr>
                                                `).join('')}}
                                            </tbody>
                                        </table>
                                    </div>
                                    ${{regionData.length > 20 ? `<p style="color: #666; font-size: 12px; margin-top: 10px;">共 ${{regionData.length}} 条项目，仅显示前20条。可在「全部项目」Tab 中查看完整列表。</p>` : ''}}
                                </div>
                            </div>
                        `;
                    }}).join('')}}
                </div>
            `;
            
            container.innerHTML = html;
            
            // 渲染区域对比图表
            setTimeout(() => {{
                const regionLabels = Object.keys(regionStats).sort((a, b) => regionStats[b].count - regionStats[a].count);
                const regionCounts = regionLabels.map(r => regionStats[r].count);
                const regionAmounts = regionLabels.map(r => regionStats[r].amount);
                const colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#98D8C8'];
                
                // 第一个子图：项目数柱状图
                Plotly.newPlot('chart-region-count-bar', [{{
                    x: regionLabels,
                    y: regionCounts,
                    type: 'bar',
                    marker: {{color: colors.slice(0, regionLabels.length)}},
                    text: regionCounts,
                    textposition: 'outside'
                }}], {{
                    title: '各区域项目数对比',
                    xaxis: {{title: '所属区域', tickangle: 0}},
                    yaxis: {{title: '项目数'}},
                    showlegend: false,
                    height: 350
                }}, {{displayModeBar: false}});
                
                // 第二个子图：金额柱状图
                Plotly.newPlot('chart-region-amount-bar', [{{
                    x: regionLabels,
                    y: regionAmounts,
                    type: 'bar',
                    marker: {{color: colors.slice(0, regionLabels.length)}},
                    text: regionAmounts.map(a => formatCurrency(a)),
                    textposition: 'outside'
                }}], {{
                    title: '各区域金额对比（万元）',
                    xaxis: {{title: '所属区域', tickangle: 0}},
                    yaxis: {{title: '金额（万元）'}},
                    showlegend: false,
                    height: 350
                }}, {{displayModeBar: false}});
                
                // 第三个子图：金额分布饼图
                Plotly.newPlot('chart-region-amount-pie', [{{
                    values: regionAmounts,
                    labels: regionLabels,
                    type: 'pie',
                    hole: 0.4,
                    textinfo: 'label+percent+value',
                    texttemplate: '%{{label}}<br>%{{percent}}<br>%{{value:,.0f}}万元',
                    marker: {{colors: colors.slice(0, regionLabels.length)}}
                }}], {{
                    title: '各区域金额分布（万元）',
                    showlegend: true,
                    height: 350
                }}, {{displayModeBar: false}});
                
                // 第四个子图：项目数分布饼图
                Plotly.newPlot('chart-region-count-pie', [{{
                    values: regionCounts,
                    labels: regionLabels,
                    type: 'pie',
                    hole: 0.4,
                    textinfo: 'label+percent+value',
                    texttemplate: '%{{label}}<br>%{{percent}}<br>%{{value}}项',
                    marker: {{colors: colors.slice(0, regionLabels.length)}}
                }}], {{
                    title: '各区域项目数分布',
                    showlegend: true,
                    height: 350
                }}, {{displayModeBar: false}});
            }}, 100);
        }}
        
        // 标签页3: 各园区分级分类
        function renderTab3() {{
            const validData = getValidProjects(filteredData);
            const container = document.getElementById('tab-3');
            
            if (validData.length === 0) {{
                container.innerHTML = '<div class="warning-box">当前筛选条件下暂无数据。</div>';
                return;
            }}
            
            // 按分级统计
            const levelStats = {{}};
            validData.forEach(d => {{
                const level = d.项目分级 || '未分类';
                if (!levelStats[level]) {{
                    levelStats[level] = {{count: 0, amount: 0}};
                }}
                levelStats[level].count++;
                levelStats[level].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按专业统计（过滤掉"其它系统"）
            const profStats = {{}};
            validData.forEach(d => {{
                const prof = d.专业 || '未分类';
                // 过滤掉"其它系统"分类
                if (prof === '其它系统' || prof === '其他系统') return;
                if (!profStats[prof]) {{
                    profStats[prof] = {{count: 0, amount: 0}};
                }}
                profStats[prof].count++;
                profStats[prof].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按园区统计
            const parkStats = {{}};
            validData.forEach(d => {{
                const park = d.园区 || '未知';
                if (!parkStats[park]) {{
                    parkStats[park] = {{count: 0, amount: 0}};
                }}
                parkStats[park].count++;
                parkStats[park].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 按专业分包统计（如果存在）
            const hasProfSubcontract = validData[0] && (validData[0].专业分包 || validData[0].专业细分);
            const profSubcontractCol = hasProfSubcontract ? (validData[0].专业分包 ? '专业分包' : '专业细分') : null;
            const profSubcontractStats = {{}};
            if (hasProfSubcontract) {{
                validData.forEach(d => {{
                    const val = d[profSubcontractCol] || '未分类';
                    if (!profSubcontractStats[val]) {{
                        profSubcontractStats[val] = {{count: 0, amount: 0}};
                    }}
                    profSubcontractStats[val].count++;
                    profSubcontractStats[val].amount += parseFloat(d.拟定金额) || 0;
                }});
            }}
            
            let html = `
                <div class="section">
                    <h2>📋 各园区分级分类统计</h2>
                    
                    <h3>按紧急程度（分级）</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>项目分级</th><th>项目数</th><th>金额合计（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(levelStats).map(level => `
                                    <tr>
                                        <td>${{level || '未分类'}}</td>
                                        <td>${{levelStats[level].count}}</td>
                                        <td>${{formatCurrency(levelStats[level].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h3>按专业分类</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>专业</th><th>项目数</th><th>金额合计（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(profStats).map(prof => `
                                    <tr>
                                        <td>${{prof || '未分类'}}</td>
                                        <td>${{profStats[prof].count}}</td>
                                        <td>${{formatCurrency(profStats[prof].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    ${{hasProfSubcontract ? `
                    <h3>按专业分包</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>专业分包</th><th>项目数</th><th>金额合计（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(profSubcontractStats).sort((a, b) => profSubcontractStats[b].amount - profSubcontractStats[a].amount).map(key => `
                                    <tr>
                                        <td>${{key || '未分类'}}</td>
                                        <td>${{profSubcontractStats[key].count}}</td>
                                        <td>${{formatCurrency(profSubcontractStats[key].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    ` : ''}}
                    
                    <h3>按园区</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>园区</th><th>项目数</th><th>金额合计（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(parkStats).sort((a, b) => parkStats[b].amount - parkStats[a].amount).map(park => `
                                    <tr>
                                        <td>${{park}}</td>
                                        <td>${{parkStats[park].count}}</td>
                                        <td>${{formatCurrency(parkStats[park].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h3>全部项目清单（可筛选）</h3>
                    <div style="margin: 15px 0;">
                        <label style="display: inline-block; margin-right: 15px;">
                            <strong>按分级筛选：</strong>
                            <select id="level-filter-tab3" multiple style="padding: 5px; min-width: 150px;" onchange="filterTab3()">
                                <option value="">全部</option>
                                ${{Object.keys(levelStats).map(level => `
                                    <option value="${{level}}">${{level || '未分类'}}</option>
                                `).join('')}}
                            </select>
                        </label>
                        <label style="display: inline-block; margin-right: 15px;">
                            <strong>按专业筛选：</strong>
                            <select id="prof-filter-tab3" multiple style="padding: 5px; min-width: 150px;" onchange="filterTab3()">
                                <option value="">全部</option>
                                ${{Object.keys(profStats).map(prof => `
                                    <option value="${{prof}}">${{prof || '未分类'}}</option>
                                `).join('')}}
                            </select>
                        </label>
                        ${{hasProfSubcontract ? `
                        <label style="display: inline-block;">
                            <strong>按专业分包筛选：</strong>
                            <select id="prof-subcontract-filter-tab3" multiple style="padding: 5px; min-width: 150px;" onchange="filterTab3()">
                                <option value="">全部</option>
                                ${{Object.keys(profSubcontractStats).map(key => `
                                    <option value="${{key}}">${{key || '未分类'}}</option>
                                `).join('')}}
                            </select>
                        </label>
                        ` : ''}}
                    </div>
                    <div class="info-box">
                        <p id="filter-count-tab3">共 ${{validData.length}} 条项目</p>
                    </div>
                    <div class="data-table-container">
                        <table id="detail-table-tab3">
                            <thead>
                                <tr>
                                    <th>园区</th>
                                    <th>序号</th>
                                    <th>项目分级</th>
                                    ${{validData[0] && validData[0].项目分类 ? '<th>项目分类</th>' : ''}}
                                    <th>专业</th>
                                    ${{hasProfSubcontract ? '<th>专业分包</th>' : ''}}
                                    <th>项目名称</th>
                                    <th>拟定金额</th>
                                    ${{validData[0] && validData[0].拟定承建组织 ? '<th>拟定承建组织</th>' : ''}}
                                    ${{validData[0] && validData[0].需求立项 ? '<th>需求立项</th>' : ''}}
                                    ${{validData[0] && (validData[0].验收 || validData[0]['验收(社区需求完成交付)']) ? '<th>验收</th>' : ''}}
                                </tr>
                            </thead>
                            <tbody>
                                ${{validData.map(d => {{
                                    const profSubcontractVal = hasProfSubcontract ? (getValue(d, profSubcontractCol) || '') : '';
                                    return `
                                    <tr data-level="${{getValue(d, '项目分级')}}" data-prof="${{getValue(d, '专业')}}" ${{hasProfSubcontract ? `data-prof-subcontract="${{profSubcontractVal}}"` : ''}}>
                                        <td>${{getValue(d, '园区')}}</td>
                                        <td>${{getValue(d, '序号')}}</td>
                                        <td>${{getValue(d, '项目分级')}}</td>
                                        ${{d.项目分类 ? `<td>${{getValue(d, '项目分类')}}</td>` : ''}}
                                        <td>${{getValue(d, '专业')}}</td>
                                        ${{hasProfSubcontract ? `<td>${{profSubcontractVal || '未分类'}}</td>` : ''}}
                                        <td>${{getValue(d, '项目名称')}}</td>
                                        <td>${{formatCurrency(getValue(d, '拟定金额'))}}</td>
                                        ${{d.拟定承建组织 ? `<td>${{getValue(d, '拟定承建组织')}}</td>` : ''}}
                                        ${{d.需求立项 ? `<td>${{getValue(d, '需求立项')}}</td>` : ''}}
                                        ${{(d.验收 || d['验收(社区需求完成交付)']) ? `<td>${{getValue(d, '验收(社区需求完成交付)') || getValue(d, '验收')}}</td>` : ''}}
                                    </tr>
                                    `;
                                }}).join('')}}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
            
            container.innerHTML = html;
        }}
        
        // 标签页3的筛选功能
        function filterTab3() {{
            const levelFilter = Array.from(document.getElementById('level-filter-tab3').selectedOptions).map(opt => opt.value).filter(v => v);
            const profFilter = Array.from(document.getElementById('prof-filter-tab3').selectedOptions).map(opt => opt.value).filter(v => v);
            const profSubcontractFilterEl = document.getElementById('prof-subcontract-filter-tab3');
            const profSubcontractFilter = profSubcontractFilterEl ? Array.from(profSubcontractFilterEl.selectedOptions).map(opt => opt.value).filter(v => v) : [];
            
            const rows = document.querySelectorAll('#detail-table-tab3 tbody tr');
            let visibleCount = 0;
            
            rows.forEach(row => {{
                const level = row.getAttribute('data-level') || '';
                const prof = row.getAttribute('data-prof') || '';
                const profSubcontract = row.getAttribute('data-prof-subcontract') || '';
                
                const levelMatch = levelFilter.length === 0 || levelFilter.includes(level);
                const profMatch = profFilter.length === 0 || profFilter.includes(prof);
                const profSubcontractMatch = profSubcontractFilter.length === 0 || profSubcontractFilter.includes(profSubcontract);
                
                if (levelMatch && profMatch && profSubcontractMatch) {{
                    row.style.display = '';
                    visibleCount++;
                }} else {{
                    row.style.display = 'none';
                }}
            }});
            
            document.getElementById('filter-count-tab3').textContent = `共 ${{visibleCount}} 条项目`;
        }}
        
        // 标签页4: 总部视图
        function renderTab4() {{
            const validData = getValidProjects(filteredData);
            const container = document.getElementById('tab-4');
            
            if (validData.length === 0) {{
                container.innerHTML = '<div class="warning-box">当前筛选条件下暂无数据。</div>';
                return;
            }}
            
            // 稳定需求判断：需求已立项（需求立项日期有效）且非无效日期
            const stableData = validData.filter(d => isStableRequirement(d));
            
            // 按园区统计稳定需求
            const stableParkStats = {{}};
            stableData.forEach(d => {{
                const park = d.园区 || '未知';
                if (!stableParkStats[park]) {{
                    stableParkStats[park] = {{count: 0, amount: 0}};
                }}
                stableParkStats[park].count++;
                stableParkStats[park].amount += parseFloat(d.拟定金额) || 0;
            }});
            
            // 查找验收列和实施列
            let acceptCol = null;
            let implCol = null;
            for (let key in validData[0]) {{
                if (!acceptCol && (key.includes('验收') || key === '验收(社区需求完成交付)')) {{
                    acceptCol = key;
                }}
                if (!implCol && key.includes('实施') && !key.toLowerCase().includes('时间')) {{
                    implCol = key;
                }}
            }}
            
            // 施工进展与验收时间预告
            const previewData = validData.map(d => {{
                const preview = {{
                    园区: d.园区 || '',
                    序号: d.序号 || '',
                    项目名称: d.项目名称 || '',
                    拟定金额: parseFloat(d.拟定金额) || 0,
                    拟定承建组织: d.拟定承建组织 || '',
                    实施时间: implCol ? (d[implCol] || '') : '',
                    验收时间: acceptCol ? (d[acceptCol] || '') : ''
                }};
                
                // 判断验收日期是否有效
                const acceptDateStr = preview.验收时间;
                preview.验收有效 = false;
                if (acceptDateStr && acceptDateStr !== null && acceptDateStr !== '') {{
                    const str = String(acceptDateStr).trim();
                    if (str && !str.startsWith('-') && !str.includes('1900')) {{
                        preview.验收有效 = true;
                    }}
                }}
                
                return preview;
            }});
            
            const acceptPreview = previewData.filter(d => d.验收有效).sort((a, b) => {{
                const dateA = parseDate(a.验收时间);
                const dateB = parseDate(b.验收时间);
                if (!dateA) return 1;
                if (!dateB) return -1;
                return dateA - dateB;
            }});
            
            let html = `
                <div class="section">
                    <h2>🏢 总部视图：稳定需求与施工验收</h2>
                    
                    <h3>各园区已确定稳定需求数量与金额</h3>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">稳定需求项目数</div>
                            <div class="metric-value">${{stableData.length}}</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">稳定需求金额合计（万元）</div>
                            <div class="metric-value">${{formatCurrency(stableData.reduce((sum, d) => sum + (parseFloat(d.拟定金额) || 0), 0))}}</div>
                        </div>
                    </div>
                    
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr><th>园区</th><th>稳定需求数量</th><th>稳定需求金额（万元）</th></tr>
                            </thead>
                            <tbody>
                                ${{Object.keys(stableParkStats).sort((a, b) => stableParkStats[b].amount - stableParkStats[a].amount).map(park => `
                                    <tr>
                                        <td>${{park}}</td>
                                        <td>${{stableParkStats[park].count}}</td>
                                        <td>${{formatCurrency(stableParkStats[park].amount)}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h3>施工进展与验收时间预告</h3>
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>园区</th>
                                    <th>序号</th>
                                    <th>项目名称</th>
                                    <th>拟定金额（万元）</th>
                                    <th>拟定承建组织</th>
                                    <th>实施时间</th>
                                    <th>验收时间</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${{previewData.map(d => `
                                    <tr style="background-color: ${{d.验收有效 ? '#e8f5e9' : ''}}">
                                        <td>${{d.园区}}</td>
                                        <td>${{d.序号}}</td>
                                        <td>${{d.项目名称}}</td>
                                        <td>${{formatCurrency(d.拟定金额)}}</td>
                                        <td>${{d.拟定承建组织}}</td>
                                        <td>${{d.实施时间}}</td>
                                        <td>${{d.验收时间}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    
                    <h4>验收时间预告（仅含有效日期）</h4>
                    ${{acceptPreview.length > 0 ? `
                    <div class="data-table-container">
                        <table>
                            <thead>
                                <tr>
                                    <th>园区</th>
                                    <th>序号</th>
                                    <th>项目名称</th>
                                    <th>拟定金额（万元）</th>
                                    <th>拟定承建组织</th>
                                    <th>实施时间</th>
                                    <th>验收时间</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${{acceptPreview.map(d => `
                                    <tr>
                                        <td>${{d.园区}}</td>
                                        <td>${{d.序号}}</td>
                                        <td>${{d.项目名称}}</td>
                                        <td>${{formatCurrency(d.拟定金额)}}</td>
                                        <td>${{d.拟定承建组织}}</td>
                                        <td>${{d.实施时间}}</td>
                                        <td>${{d.验收时间}}</td>
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                    ` : '<div class="info-box">暂无有效验收日期，请在一线填报「验收(社区需求完成交付)」节点。</div>'
                    }}
                </div>
            `;
            
            container.innerHTML = html;
        }}
        
        // 标签页5: 全部项目
        function renderTab5() {{
            const validData = getValidProjects(filteredData);
            const container = document.getElementById('tab-5');
            
            if (validData.length === 0) {{
                container.innerHTML = '<div class="warning-box">当前筛选条件下暂无数据。</div>';
                return;
            }}
            
            // 获取所有列
            const columns = new Set();
            validData.forEach(d => {{
                Object.keys(d).forEach(k => columns.add(k));
            }});
            const columnList = ['园区', '所属区域', '城市', ...Array.from(columns).filter(c => !['园区', '所属区域', '城市'].includes(c))];
            
            let html = `
                <div class="section">
                    <h2>📑 全部项目清单</h2>
                    <div class="info-box">
                        <p>共 ${{validData.length}} 条项目，以下列出所有项目明细。</p>
                    </div>
                    <div class="data-table-container" style="overflow-x: auto;">
                        <table style="font-size: 11px;">
                            <thead>
                                <tr>
                                    ${{columnList.map(col => `<th>${{col}}</th>`).join('')}}
                                </tr>
                            </thead>
                            <tbody>
                                ${{validData.map(d => `
                                    <tr>
                                        ${{columnList.map(col => {{
                                            const val = getValue(d, col);
                                            if (isValidNumber(val) && (col.includes('金额') || col.includes('金额'))) {{
                                                return `<td>${{formatCurrency(val)}}</td>`;
                                            }} else if (isValidNumber(val)) {{
                                                return `<td>${{formatNumber(val)}}</td>`;
                                            }} else {{
                                                return `<td>${{String(val).substring(0, 50)}}</td>`;
                                            }}
                                        }}).join('')}}
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
            
            container.innerHTML = html;
        }}
        
        // 展开/收起功能
        function toggleExpander(header) {{
            header.classList.toggle('active');
            const content = header.nextElementSibling;
            content.classList.toggle('active');
        }}
        
        // 初始化渲染
        renderAllTabs();
    </script>
</body>
</html>'''
    
    return html_content


def generate_html_report(df: pd.DataFrame, sub: pd.DataFrame, sub_location: pd.DataFrame, 园区选择: list) -> str:
    """生成交互式HTML报告（新版本，完全交互式）"""
    return generate_interactive_html(df, 园区选择)





def generate_pdf_report(df: pd.DataFrame, 园区选择: list, output_path: str = None):
    """生成PDF报告，包含所有页面内容（使用HTML转PDF方式）"""
    return generate_pdf_report_html(df, 园区选择, output_path)


def _get_deepseek_api_key(provided: str | None = None) -> str | None:
    """获取 DeepSeek API Key：优先使用传入值，否则 session_state、Streamlit Secrets、环境变量。"""
    if provided and str(provided).strip():
        return str(provided).strip()
    key = st.session_state.get("deepseek_api_key") or ""
    if key and str(key).strip():
        return str(key).strip()
    # Streamlit Secrets：无 secrets.toml 时会 FileNotFoundError，键不存在会 KeyError
    try:
        if hasattr(st, "secrets") and st.secrets:
            for secret_key in ("DEEPSEEK_API_KEY", "deepseek_api_key"):
                try:
                    val = st.secrets[secret_key]
                    if val and str(val).strip():
                        return str(val).strip()
                except (KeyError, AttributeError, TypeError):
                    continue
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return os.getenv("DEEPSEEK_API_KEY") or None


def _get_deepseek_client(api_key: str | None = None):
    """构造 DeepSeek 客户端，API Key 来自参数或 _get_deepseek_api_key。"""
    final_key = api_key or _get_deepseek_api_key()
    if not (DEEPSEEK_CLIENT_AVAILABLE and final_key):
        return None
    try:
        client = OpenAI(
            api_key=final_key,
            base_url="https://api.deepseek.com",
        )
        return client
    except Exception:
        return None


def _answer_with_deepseek(api_key: str | None, question: str, df: pd.DataFrame) -> str:
    """调用 DeepSeek 接口回答使用说明或分析问题。"""
    client = _get_deepseek_client(api_key)
    if client is None:
        return (
            "未检测到可用的 DeepSeek 客户端。\n\n"
            "请在左侧或当前页中正确填写 DeepSeek API Key（建议使用 Streamlit Secrets 或环境变量），"
            "或联系管理员配置后再重试。"
        )
    # 只提供列信息，不传输完整数据
    cols = list(df.columns)[:30]
    system_prompt = (
        "你是一个面向业务同事的中文 AI 助手，负责解答关于“养老社区改良改造进度管理看板”的使用问题，"
        "并根据已经加载到应用中的 DataFrame 数据给出简单的数据查询建议。\n\n"
        "返回要求：\n"
        "1. 用简体中文回答。\n"
        "2. 如果问题是“如何使用”类（例如如何上传、如何手动输入数据），请用步骤化说明回答。\n"
        "3. 如果是“帮我查找/统计”类问题，请：\n"
        "   - 先用自然语言说明大致的筛选逻辑（比如要按哪个字段、什么条件过滤、是否与月份有关等）；\n"
        "   - 给出用户可以在当前看板中如何操作的指引（例如去哪个 Tab、用哪些筛选器）。\n"
        "4. 不要编造不存在的字段名，字段名仅限于下面这批实际存在的列。\n"
    )
    user_prompt = (
        f"用户问题：{question}\n\n"
        f"当前数据列名如下（最多 30 个）：{', '.join(cols)}\n\n"
        "注意：你无法直接访问完整数据，只能基于这些列名和业务含义来回答和给出操作建议。"
    )
    try:
        resp = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )
        content = resp.choices[0].message.content or ""
        return content.strip()
    except Exception as e:
        return f"调用 DeepSeek 接口失败：{e}"


def render_地图与统计(df: pd.DataFrame, 园区选择: list):
    """地图与统计 Tab：中国地图 + 按专业/分级/园区/区域图表。"""
    df_with_location = _add_城市和区域列(df)
    # 处理园区选择：如果为空或None，显示所有有园区信息的数据
    if 园区选择 and len(园区选择) > 0:
        valid_parks = [p for p in 园区选择 if p and pd.notna(p)]
        if valid_parks:
            sub = df_with_location[df_with_location["园区"].isin(valid_parks)]
        else:
            sub = df_with_location[df_with_location["园区"].notna()]
    else:
        sub = df_with_location[df_with_location["园区"].notna()]  # 只显示有园区信息的行

    st.subheader("中国地图 · 各地市项目分布")
    # 为地图构造城市-园区明细，用于 tooltip 展示
    city_tooltip_data = _build_城市_园区明细(sub)
    _render_中国地图(sub, city_tooltip_data)
    
    st.markdown("---")
    st.subheader("数据统计")
    st.markdown("### 📊 按区域统计分析")
    
    # 区域统计表格
    if "所属区域" in sub.columns:
        st.markdown("#### 各区域项目统计")
        by_region = sub.groupby("所属区域", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum"),
            园区数=("园区", "nunique"),
        ).reset_index()
        by_region = by_region[by_region["所属区域"] != "其他"].sort_values("项目数", ascending=False)
        by_region["金额合计"] = by_region["金额合计"].round(2)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("总区域数", len(by_region))
        with col2:
            st.metric("总项目数", int(by_region["项目数"].sum()))
        with col3:
            st.metric("总金额（万元）", f"{by_region['金额合计'].sum():,.0f}")
        with col4:
            st.metric("总园区数", int(by_region["园区数"].sum()))
        
        st.dataframe(by_region, use_container_width=True, hide_index=True)
        
        # 区域下各园区明细
        st.markdown("#### 各区域下园区明细")
        for region in by_region["所属区域"].unique():
            region_df = sub[sub["所属区域"] == region]
            parks_in_region = region_df.groupby("园区", dropna=False).agg(
                项目数=("序号", "count"),
                金额合计=("拟定金额", "sum"),
            ).reset_index().sort_values("项目数", ascending=False)
            parks_in_region["金额合计"] = parks_in_region["金额合计"].round(2)
            
            with st.expander(f"📌 {region}（{len(parks_in_region)}个园区，{int(parks_in_region['项目数'].sum())}个项目，{parks_in_region['金额合计'].sum():,.0f}万元）"):
                st.dataframe(parks_in_region, use_container_width=True, hide_index=True)
        
        st.markdown("---")
    
    st.markdown("### 图表统计")

    try:
        import plotly.express as px
    except ImportError:
        st.warning("请安装 plotly 以使用美化图表：pip install plotly")
        _render_图表_简易(sub)
        return

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**按专业 · 项目数**")
        by_prof = sub.groupby("专业", dropna=False).agg(项目数=("序号", "count")).reset_index().sort_values("项目数", ascending=False)
        # 过滤掉"其它系统"分类
        by_prof = by_prof[~by_prof["专业"].isin(["其它系统", "其他系统"])]
        if not by_prof.empty:
            fig = px.bar(
                by_prof, x="专业", y="项目数", color="项目数",
                color_continuous_scale="Blues", text_auto=".0f",
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False, margin=dict(t=20, b=80), height=320, xaxis_title="", yaxis_title="项目数")
            fig.update_traces(textfont_size=10)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with c2:
        st.markdown("**按项目分级 · 金额占比**")
        by_level = sub.groupby("项目分级", dropna=False).agg(
            项目数=("序号", "count"),
            金额合计=("拟定金额", "sum")
        ).reset_index().sort_values("金额合计", ascending=False)
        if not by_level.empty:
            colors = (CHART_COLORS_PIE * (1 + len(by_level) // len(CHART_COLORS_PIE)))[: len(by_level)]
            fig = px.pie(
                by_level, values="金额合计", names="项目分级", title="",
                color_discrete_sequence=colors, hole=0.35,
            )
            fig.update_traces(
                textposition="outside",
                textinfo="label+percent+value",
                texttemplate="%{label}<br>%{percent}<br>%{value:,.0f}万元",
                textfont_size=12,
                pull=[0.02] * len(by_level),
            )
            fig.update_layout(
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.2),
                margin=dict(t=20, b=60, l=20, r=20),
                height=380,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    c3, c4 = st.columns(2)
    with c3:
        st.markdown("**按园区 · 金额（万元）**")
        by_park = sub.groupby("园区", dropna=False).agg(金额合计=("拟定金额", "sum")).reset_index().sort_values("金额合计", ascending=False)
        if not by_park.empty:
            by_park["金额合计"] = by_park["金额合计"].round(2)
            fig = px.bar(
                by_park, x="园区", y="金额合计", color="金额合计",
                color_continuous_scale="Blues", text_auto=".0f",
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False, margin=dict(t=20, b=80), height=320, xaxis_title="", yaxis_title="金额（万元）")
            fig.update_traces(textfont_size=10)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    with c4:
        st.markdown("**按城市 · 金额（万元）**")
        by_city = sub.groupby("城市", dropna=False).agg(金额合计=("拟定金额", "sum")).reset_index()
        by_city = by_city[by_city["城市"] != "其他"].sort_values("金额合计", ascending=False)
        if not by_city.empty:
            by_city["金额合计"] = by_city["金额合计"].round(2)
            fig = px.bar(
                by_city, x="城市", y="金额合计", color="金额合计",
                color_continuous_scale="Teal", text_auto=".0f",
            )
            fig.update_layout(xaxis_tickangle=-45, showlegend=False, margin=dict(t=20, b=80), height=320, xaxis_title="", yaxis_title="金额（万元）")
            fig.update_traces(textfont_size=10)
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.markdown("**按专业 · 金额合计（万元）**")
    by_prof_m = sub.groupby("专业", dropna=False).agg(金额=("拟定金额", "sum")).reset_index().sort_values("金额", ascending=False)
    # 过滤掉"其它系统"分类
    by_prof_m = by_prof_m[~by_prof_m["专业"].isin(["其它系统", "其他系统"])]
    if not by_prof_m.empty:
        fig = px.bar(
            by_prof_m, x="专业", y="金额", color="金额",
            color_continuous_scale="Viridis", text_auto=".0f",
        )
        fig.update_layout(xaxis_tickangle=-45, showlegend=False, margin=dict(t=20, b=80), height=360, xaxis_title="", yaxis_title="金额（万元）")
        fig.update_traces(textfont_size=10)
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def _render_project_wizard(df: pd.DataFrame):
    """项目新增 / 修改：平铺表单。新增有必填校验，修改全部选填，只改想改的字段。"""
    import uuid
    df_raw = df.copy()
    df_all = _ensure_project_columns(df)

    mode = st.radio("操作类型", ["修改已有项目", "新增项目"], horizontal=True)

    if mode == "修改已有项目":
        st.markdown("### 步骤 1：筛选要修改的项目")
        st.caption("先选园区，再在该园区内按项目名称选择要修改的那条记录。")

        candidates = df_raw.copy()
        parks_list = sorted(df_raw["园区"].dropna().astype(str).unique().tolist())
        parks_list = [p for p in parks_list if p and str(p).strip() and str(p) != "nan"]

        if not parks_list:
            st.info("未发现任何园区数据，请先上传/导入数据。")
            return

        园区 = st.selectbox("园区*", options=parks_list, index=0, key="edit_park_select")
        candidates = candidates[candidates["园区"].astype(str) == str(园区)]
        if candidates.empty:
            st.info("该园区下未找到项目记录，请尝试更换园区。")
            return

        if candidates.empty:
            st.info("未找到匹配项目，可切换到“新增项目”，或调整查找条件。")
            return

        st.caption(f"在园区「{园区}」内找到 {len(candidates)} 条记录，请选择项目：")
        display_cols = ["序号", "园区", "项目名称", "项目分级", "拟定金额"]
        display_cols = [c for c in display_cols if c in candidates.columns]
        st.dataframe(candidates[display_cols].head(50), use_container_width=True, hide_index=True)

        if "项目名称" not in candidates.columns or "序号" not in candidates.columns:
            st.error("数据缺少「项目名称」或「序号」列，无法进行按名称选择。")
            return

        # 解决同名项目歧义：下拉展示“名称（序号）”，内部用序号定位
        name_to_seq = {}
        name_options = []
        for _, r in candidates.iterrows():
            seq_raw = r.get("序号", None)
            seq_num = None
            try:
                if pd.notna(seq_raw):
                    seq_num = int(float(seq_raw))
            except Exception:
                seq_num = None

            nm_raw = r.get("项目名称", "")
            nm = str(nm_raw).strip() if nm_raw is not None else ""
            if not nm:
                nm = "（未命名项目）"

            if seq_num is None:
                continue
            label = f"{nm}（序号 {seq_num}）"
            if label not in name_to_seq:
                name_to_seq[label] = seq_num
                name_options.append(label)

        if not name_options:
            st.info("未能构建有效的项目名称下拉选项，请检查数据的“序号/项目名称”列格式。")
            return

        chosen_label = st.selectbox("项目名称*（下拉选择）", options=name_options, index=0, key="edit_name_select")
        chosen_seq = name_to_seq[chosen_label]
        target_row = df_all[df_all["序号"].astype(int) == int(chosen_seq)].iloc[0]

        st.markdown("---")
        st.markdown(f"### 步骤 2：编辑项目（序号 {int(target_row['序号'])}）")
        st.caption("提示：如在侧边栏勾选了「保存到数据库时同时推送到飞书」，保存后本次修改的内容（含字段变更详情）将推送到飞书。")

        seq_val = int(target_row["序号"])

        with st.expander("危险操作：删除该项目", expanded=False):
            with st.form(f"delete_project_form_{seq_val}"):
                st.warning("删除后将从团队共享数据库中移除该条记录，且不可恢复（除非重新导入原始数据）。")
                delete_clicked = st.form_submit_button("🗑 删除该项目", type="primary")
            if delete_clicked:
                df_new = df_all[df_all["序号"].astype(int) != seq_val].copy()
                save_to_db(df_new)
                if _get_feishu_webhook_url():
                    diff = {"deleted": [_row_to_dict(target_row)], "added": [], "modified": []}
                    payload = _build_feishu_payload_from_diff(diff, len(df_new), source="向导删除")
                    push_to_feishu(payload=payload)
                st.success(f"已删除序号为 {seq_val} 的项目。")
                st.rerun()

        st.markdown("### 修改入口（下拉选择后才显示详情）")
        csel1, csel2 = st.columns(2)
        with csel1:
            timeline_opts = ["（不修改日期）"] + list(TIMELINE_COLS)
            chosen_timeline_col = st.selectbox("日期更改（下拉选择节点）", options=timeline_opts, index=0, key=f"edit_date_menu_{seq_val}")
        with csel2:
            info_opts = ["（不修改项目信息）", "基础信息", "项目属性", "专业与名称"]
            chosen_info_group = st.selectbox("项目信息修改（下拉选择类别）", options=info_opts, index=0, key=f"edit_info_menu_{seq_val}")

        if chosen_timeline_col and chosen_timeline_col != "（不修改日期）":
            st.markdown("---")
            st.markdown(f"#### 日期更改详情框：{chosen_timeline_col}")
            with st.form(f"edit_date_form_{seq_val}"):
                raw_val = target_row.get(chosen_timeline_col, "")
                existing_d = _str_to_date(raw_val)
                default_d = existing_d if existing_d != SENTINEL_DATE else date(2026, 1, 1)
                new_date = st.date_input(
                    f"「{chosen_timeline_col}」日期",
                    value=default_d,
                    min_value=DATE_RANGE_MIN,
                    max_value=DATE_RANGE_MAX,
                    format="YYYY-MM-DD",
                    key=f"edit_date_picker_{seq_val}_{chosen_timeline_col}",
                )
                save_date_clicked = st.form_submit_button("💾 保存日期更改")
            if save_date_clicked:
                df_new = df_all.copy()
                mask = df_new["序号"].astype(int) == seq_val
                if chosen_timeline_col in df_new.columns:
                    df_new.loc[mask, chosen_timeline_col] = _date_to_str(new_date)
                save_to_db(df_new)
                if _get_feishu_webhook_url():
                    modified_row = df_new.loc[mask].iloc[0]
                    changes = [f"{chosen_timeline_col}：{_format_cell(target_row.get(chosen_timeline_col, '')) or '（空）'} → {_format_cell(modified_row.get(chosen_timeline_col, '')) or '（空）'}"]
                    modified_details = [{"序号": seq_val, "变更项": changes}]
                    diff = {
                        "deleted": [],
                        "added": [],
                        "modified": [_row_to_dict(modified_row)],
                        "modified_details": modified_details,
                    }
                    payload = _build_feishu_payload_from_diff(diff, len(df_new), source="向导修改-日期")
                    push_to_feishu(payload=payload)
                st.success("已保存日期更改。")
                st.rerun()

        if chosen_info_group and chosen_info_group != "（不修改项目信息）":
            st.markdown("---")
            st.markdown(f"#### 项目信息修改详情框：{chosen_info_group}")
            with st.form(f"edit_info_form_{seq_val}_{chosen_info_group}"):
                updates = {}

                if chosen_info_group == "基础信息":
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        st.text_input("序号（自动）", value=str(seq_val), disabled=True)
                        园区_options = sorted(set(df_all["园区"].dropna().astype(str).tolist()) | set(园区_TO_城市.keys()))
                        园区默认 = str(target_row.get("园区", ""))
                        园区2 = st.selectbox(
                            "园区（选填）",
                            options=[""] + 园区_options,
                            index=(园区_options.index(园区默认) + 1) if 园区默认 in 园区_options else 0,
                        )
                        updates["园区"] = 园区2
                    with c2:
                        区域_opts = _get_dropdown_options(df_all, "所属区域", list(园区_TO_区域.values()))
                        _v = str(target_row.get("所属区域", ""))
                        所属区域 = st.selectbox("所属区域（选填）", options=[""] + 区域_opts, index=区域_opts.index(_v) + 1 if _v in 区域_opts else 0)
                        updates["所属区域"] = 所属区域
                    with c3:
                        城市_opts = _get_dropdown_options(df_all, "城市", list(园区_TO_城市.values()))
                        _cv = str(target_row.get("城市", ""))
                        城市 = st.selectbox("所在城市（选填）", options=[""] + 城市_opts, index=城市_opts.index(_cv) + 1 if _cv in 城市_opts else 0)
                        updates["城市"] = 城市

                    业态_opts = _get_dropdown_options(df_all, "所属业态", OPT_所属业态)
                    _ev = str(target_row.get("所属业态", ""))
                    所属业态 = st.selectbox("所属业态（选填）", options=[""] + 业态_opts, index=业态_opts.index(_ev) + 1 if _ev in 业态_opts else 0)
                    updates["所属业态"] = 所属业态

                elif chosen_info_group == "项目属性":
                    c4, c5, c6 = st.columns(3)
                    with c4:
                        分级_opts = _get_dropdown_options(df_all, "项目分级", OPT_项目分级)
                        _lv = str(target_row.get("项目分级", ""))
                        项目分级 = st.selectbox("项目分级（选填）", options=[""] + 分级_opts, index=分级_opts.index(_lv) + 1 if _lv in 分级_opts else 0)
                        updates["项目分级"] = 项目分级
                    with c5:
                        分类_opts = _get_dropdown_options(df_all, "项目分类", OPT_项目分类)
                        _cv2 = str(target_row.get("项目分类", ""))
                        项目分类 = st.selectbox("项目分类（选填）", options=[""] + 分类_opts, index=分类_opts.index(_cv2) + 1 if _cv2 in 分类_opts else 0)
                        updates["项目分类"] = 项目分类
                    with c6:
                        承建_opts = _get_dropdown_options(df_all, "拟定承建组织", OPT_拟定承建组织)
                        _bv = str(target_row.get("拟定承建组织", ""))
                        拟定承建组织 = st.selectbox("拟定承建组织（选填）", options=[""] + 承建_opts, index=承建_opts.index(_bv) + 1 if _bv in 承建_opts else 0)
                        updates["拟定承建组织"] = 拟定承建组织

                    c7, c8 = st.columns(2)
                    with c7:
                        总部_opts = [x for x in _get_dropdown_options(df_all, "总部重点关注项目", OPT_总部重点关注) if x]
                        _zv = str(target_row.get("总部重点关注项目", ""))
                        总部重点关注项目 = st.selectbox("总部重点关注项目（选填）", options=[""] + 总部_opts, index=总部_opts.index(_zv) + 1 if _zv in 总部_opts else 0)
                        updates["总部重点关注项目"] = 总部重点关注项目
                    with c8:
                        拟定金额 = st.number_input("拟定金额（万元）", min_value=0.0, value=float(target_row.get("拟定金额") or 0.0), step=1.0)
                        updates["拟定金额"] = 拟定金额

                elif chosen_info_group == "专业与名称":
                    c9, c10 = st.columns(2)
                    with c9:
                        专业_opts = _get_dropdown_options(df_all, "专业", 专业大类)
                        _pv = str(target_row.get("专业", ""))
                        专业 = st.selectbox("专业（选填）", options=[""] + 专业_opts, index=专业_opts.index(_pv) + 1 if _pv in 专业_opts else 0)
                        updates["专业"] = 专业
                    with c10:
                        分包_opts = _get_dropdown_options(df_all, "专业分包")
                        _sbv = str(target_row.get("专业分包", ""))
                        专业分包 = st.selectbox("专业分包（选填）", options=[""] + 分包_opts, index=分包_opts.index(_sbv) + 1 if _sbv in 分包_opts else 0)
                        updates["专业分包"] = 专业分包
                    项目名称 = st.text_input("项目名称（选填）", value=str(target_row.get("项目名称", "")))
                    备注说明 = st.text_area("备注说明（选填）", value=str(target_row.get("备注说明", "")))
                    updates["项目名称"] = 项目名称
                    updates["备注说明"] = 备注说明

                save_info_clicked = st.form_submit_button("💾 保存项目信息更改")

            if save_info_clicked:
                if "拟定金额" in updates:
                    if float(updates.get("拟定金额") or 0) <= 0:
                        st.error("拟定金额需大于 0。")
                        return

                df_new = df_all.copy()
                mask = df_new["序号"].astype(int) == seq_val
                for col, val in updates.items():
                    if col in df_new.columns:
                        df_new.loc[mask, col] = val
                save_to_db(df_new)
                if _get_feishu_webhook_url():
                    modified_row = df_new.loc[mask].iloc[0]
                    changes = []
                    for col in target_row.index:
                        if col not in modified_row.index:
                            continue
                        ov = _format_cell(target_row[col])
                        nv = _format_cell(modified_row[col])
                        if ov != nv:
                            changes.append(f"{col}：{ov or '（空）'} → {nv or '（空）'}")
                    modified_details = [{"序号": seq_val, "变更项": changes}]
                    diff = {
                        "deleted": [],
                        "added": [],
                        "modified": [_row_to_dict(modified_row)],
                        "modified_details": modified_details,
                    }
                    payload = _build_feishu_payload_from_diff(diff, len(df_new), source="向导修改-信息")
                    push_to_feishu(payload=payload)
                st.success("已保存项目信息更改。")
                st.rerun()
        return

    # ---------- 新增项目 ----------
    st.markdown("### 新增项目")
    df_all = _ensure_project_columns(df_all)
    next_seq = _get_next_序号(df_all)
    required_fields = ["园区", "所属业态", "项目分级", "项目分类", "拟定承建组织", "专业", "项目名称", "拟定金额"]

    with st.form("add_project_form"):
        st.caption(f"新项目序号将自动设置为：{next_seq}")
        st.caption("提示：如在侧边栏勾选了「保存到数据库时同时推送到飞书」，保存后本次录入的内容（含字段信息）将推送到飞书。")

        c1, c2, c3 = st.columns(3)
        with c1:
            parks = sorted(set(df_all["园区"].dropna().astype(str).tolist()) | set(园区_TO_城市.keys()))
            园区 = st.selectbox("园区*", options=[""] + parks)
        with c2:
            区域_opts = _get_dropdown_options(df_all, "所属区域", list(园区_TO_区域.values()))
            所属区域 = st.selectbox("所属区域（选填）", options=[""] + 区域_opts)
        with c3:
            城市_opts = _get_dropdown_options(df_all, "城市", list(园区_TO_城市.values()))
            城市 = st.selectbox("所在城市（选填）", options=[""] + 城市_opts)

        c4, c5, c6 = st.columns(3)
        with c4:
            业态_opts = _get_dropdown_options(df_all, "所属业态", OPT_所属业态)
            所属业态 = st.selectbox("所属业态*", options=[""] + 业态_opts)
        with c5:
            分级_opts = _get_dropdown_options(df_all, "项目分级", OPT_项目分级)
            项目分级 = st.selectbox("项目分级*", options=[""] + 分级_opts)
        with c6:
            分类_opts = _get_dropdown_options(df_all, "项目分类", OPT_项目分类)
            项目分类 = st.selectbox("项目分类*", options=[""] + 分类_opts)

        c7, c8 = st.columns(2)
        with c7:
            承建_opts = _get_dropdown_options(df_all, "拟定承建组织", OPT_拟定承建组织)
            拟定承建组织 = st.selectbox("拟定承建组织*", options=[""] + 承建_opts)
        with c8:
            总部_opts = [x for x in _get_dropdown_options(df_all, "总部重点关注项目", OPT_总部重点关注) if x]
            总部重点关注项目 = st.selectbox("总部重点关注项目（选填）", options=[""] + 总部_opts)

        c9, c10 = st.columns(2)
        with c9:
            专业_opts = _get_dropdown_options(df_all, "专业", 专业大类)
            专业 = st.selectbox("专业*", options=[""] + 专业_opts)
        with c10:
            分包_opts = _get_dropdown_options(df_all, "专业分包")
            专业分包 = st.selectbox("专业分包（选填）", options=[""] + 分包_opts)

        项目名称 = st.text_input("项目名称*")
        备注说明 = st.text_area("备注说明（选填）")
        拟定金额 = st.number_input("拟定金额（万元）*", min_value=0.0, value=0.0, step=1.0)

        st.markdown("**项目节点日期**（日期全列出，可统一填写）")
        timeline_values = {}
        unify_all = st.checkbox("统一填写所有节点日期", value=False, key="add_timeline_unify_all")
        if unify_all:
            unified_date = st.date_input(
                "统一日期（将写入所有节点）",
                value=date(2026, 1, 1),
                min_value=DATE_RANGE_MIN,
                max_value=DATE_RANGE_MAX,
                format="YYYY-MM-DD",
                key="add_timeline_unified_date",
            )
            for col in TIMELINE_COLS:
                timeline_values[col] = _date_to_str(unified_date)
        else:
            st.caption("不想填写的节点勾选“留空”。")
            for col in TIMELINE_COLS:
                cc1, cc2 = st.columns([3, 1])
                with cc1:
                    dval = st.date_input(
                        col,
                        value=date(2026, 1, 1),
                        min_value=DATE_RANGE_MIN,
                        max_value=DATE_RANGE_MAX,
                        format="YYYY-MM-DD",
                        key=f"add_timeline_date_{col}",
                    )
                with cc2:
                    leave_empty = st.checkbox("留空", value=True, key=f"add_timeline_empty_{col}")
                timeline_values[col] = "" if leave_empty else _date_to_str(dval)

        submitted = st.form_submit_button("✅ 完成并写入数据库")

    if submitted:
        form_dict = {
            "序号": next_seq,
            "园区": 园区,
            "所属区域": 所属区域,
            "城市": 城市,
            "所属业态": 所属业态,
            "项目分级": 项目分级,
            "项目分类": 项目分类,
            "拟定承建组织": 拟定承建组织,
            "总部重点关注项目": 总部重点关注项目,
            "专业": 专业,
            "专业分包": 专业分包,
            "项目名称": 项目名称,
            "备注说明": 备注说明,
            "拟定金额": 拟定金额,
        }
        missing = [k for k in required_fields if k != "拟定金额" and not str(form_dict.get(k, "")).strip()]
        if "拟定金额" in required_fields:
            amt = float(form_dict.get("拟定金额") or 0)
            if amt <= 0:
                missing.append("拟定金额（需大于 0）")
        if missing:
            st.error(f"以下字段为必填：{', '.join(missing)}")
            return

        if not form_dict["所属区域"] and 园区 in 园区_TO_区域:
            form_dict["所属区域"] = 园区_TO_区域[园区]
        if not form_dict["城市"] and 园区 in 园区_TO_城市:
            form_dict["城市"] = 园区_TO_城市[园区]

        token = str(uuid.uuid4())
        form_dict["上传凭证"] = token
        for col in TIMELINE_COLS:
            if col not in form_dict:
                form_dict[col] = ""
            if col in timeline_values:
                form_dict[col] = timeline_values[col]

        df_new_row = pd.DataFrame([form_dict])
        df_all2 = pd.concat([df_all, df_new_row], ignore_index=True)
        save_to_db(df_all2)
        if _get_feishu_webhook_url():
            diff = {"deleted": [], "added": [_row_to_dict(df_new_row.iloc[0])], "modified": []}
            payload = _build_feishu_payload_from_diff(diff, len(df_all2), source="向导新增")
            push_to_feishu(payload=payload)
        st.success(f"已写入数据库。上传凭证：{token}")
        st.info("请截图或记录该凭证号，后续如需确认或审计可用于检索。")
        st.rerun()


def _require_feishu_login() -> bool:
    """登录门禁：当 FEISHU_LOGIN_REQUIRED=1 或 OAuth 配置完整且未显式关闭时，未登录用户需通过飞书 OAuth 登录后才能访问。"""
    login_required = str(os.getenv("FEISHU_LOGIN_REQUIRED", "")).strip()
    if login_required == "0":
        return True
    if login_required != "1":
        app_id = os.getenv("FEISHU_APP_ID")
        secret = os.getenv("FEISHU_APP_SECRET")
        redirect = os.getenv("FEISHU_REDIRECT_URI")
        if not (app_id and secret and redirect):
            return True
    if not FEISHU_OAUTH_AVAILABLE:
        st.warning("飞书登录模块未就绪，请确认 feishu_oauth.py 存在。")
        return True
    app_id = os.getenv("FEISHU_APP_ID")
    secret = os.getenv("FEISHU_APP_SECRET")
    redirect = os.getenv("FEISHU_REDIRECT_URI")
    if not app_id or not secret or not redirect:
        st.warning("请配置 FEISHU_APP_ID、FEISHU_APP_SECRET、FEISHU_REDIRECT_URI 以启用飞书登录。")
        return True
    user = st.session_state.get("feishu_user")
    if user:
        return True
    query = st.query_params
    code = query.get("code")
    if code:
        try:
            u = exchange_code_for_user(code)
            if u:
                st.session_state["feishu_user"] = u
                st.query_params.clear()
                st.rerun()
        except Exception as e:
            st.error(f"登录失败：{e}")
            return False
    auth_url = build_authorize_url(redirect, state="app203")
    st.markdown("### 请先登录")
    st.markdown("使用飞书账号登录后即可访问本看板。")
    st.link_button("飞书登录", auth_url, type="primary")
    return False


def main():
    if not _require_feishu_login():
        return

    st.title("养老社区改良改造进度管理看板")
    st.caption("需求审核流程：社区提出 → 分级 → 专业分类 → 预算拆分 → 一线立项 → 项目部施工 → 总部协调招采/施工 → 督促验收")

    # 侧边栏：用户信息 + 数据源
    with st.sidebar:
        if st.session_state.get("feishu_user"):
            u = st.session_state["feishu_user"]
            name = u.get("name") or u.get("user_id") or u.get("open_id", "未知")
            st.caption(f"👤 {name}")
            if st.button("退出登录", key="logout"):
                del st.session_state["feishu_user"]
                st.rerun()
        st.header("数据源")
        source_options = ["数据库（团队共享）", "飞书多维表格", "上传文件（覆盖数据库）", "目录下全部 CSV（覆盖数据库）"]
        source = st.radio("数据来源", source_options, index=0)
        df_db = load_from_db()
        df = pd.DataFrame()

        if source == "飞书多维表格":
            if not FEISHU_BITABLE_AVAILABLE:
                st.warning("未安装飞书加载模块，请确认 feishu_bitable_loader.py 存在。")
            elif not os.getenv("FEISHU_APP_ID") or not os.getenv("FEISHU_APP_SECRET"):
                st.warning("请配置 FEISHU_APP_ID 和 FEISHU_APP_SECRET（Streamlit Secrets 或环境变量）。")
            else:
                bitable_url = st.text_input(
                    "飞书多维表格链接",
                    value=os.getenv(
                        "FEISHU_BITABLE_URL",
                        "https://tkhome.feishu.cn/wiki/DFIYwb1ELigVNgkdJQAcoPArnRg?sheet=0zsvcA&table=tblodAIOVXskb6KM&view=vew6WTXj0C",
                    ),
                    placeholder="https://xxx.feishu.cn/base/AppToken 或 wiki 链接含 ?table=TableId",
                )
                if bitable_url.strip():
                    if st.button("🔄 从飞书加载", key="load_feishu"):
                        with st.spinner("正在从飞书加载..."):
                            loaded = load_from_bitable(bitable_url.strip())
                        if loaded.empty:
                            st.warning("未获取到数据，请检查链接和权限（应用需有该多维表格的读取权限）。")
                        else:
                            st.session_state["df_from_feishu"] = loaded
                            st.success(f"已从飞书加载，共 {len(loaded)} 条记录。")
                            st.rerun()
                    if "df_from_feishu" in st.session_state:
                        df = st.session_state["df_from_feishu"]
                        st.caption(f"当前显示飞书数据，共 {len(df)} 条。可「导入到数据库」后切换为数据库进行编辑。")
                        if st.button("✅ 导入到数据库（覆盖）", type="primary", key="feishu_to_db"):
                            save_to_db(df)
                            if _get_feishu_webhook_url():
                                if push_to_feishu(f"【养老社区进度表】已从飞书多维表格导入，共 {len(df)} 条记录。"):
                                    st.success("已导入到数据库并推送至飞书。")
                                else:
                                    st.success("已导入到数据库。"); st.warning("飞书推送失败。")
                            else:
                                st.success("已导入到数据库。")
                            del st.session_state["df_from_feishu"]
                            st.rerun()
                else:
                    st.info("请填写飞书多维表格链接，或在 Secrets 中配置 FEISHU_BITABLE_URL。")

        elif source == "数据库（团队共享）":
            default_csv = DEFAULT_BUNDLED_CSV if DEFAULT_BUNDLED_CSV.exists() else Path(DEFAULT_SINGLE_FILE)
            if df_db.empty:
                # 优先内嵌 .enc（Streamlit Cloud）；其次 改良改造报表-V4.csv
                if default_csv.exists():
                    try:
                        df = load_single_csv(str(default_csv))
                        if not df.empty:
                            save_to_db(df)
                            if _get_feishu_webhook_url():
                                if push_to_feishu(f"【养老社区进度表】已用「{default_csv.name}」初始化，共 {len(df)} 条记录。"):
                                    st.success(f"已用「{default_csv.name}」初始化团队共享数据库，共 {len(df)} 条记录；已推送至飞书。")
                                else:
                                    st.success(f"已用「{default_csv.name}」初始化团队共享数据库，共 {len(df)} 条记录。"); st.warning("飞书推送失败，请检查 Webhook 或网络。")
                            else:
                                st.success(f"已用「{default_csv.name}」初始化团队共享数据库，共 {len(df)} 条记录。")
                        else:
                            st.info("当前数据库中暂无数据，请通过下方“上传文件”或“目录下全部 CSV”导入一次。")
                    except Exception as e:
                        st.warning(f"无法从默认 CSV 加载：{e}。请通过下方“上传文件”导入。")
                        df = pd.DataFrame()
                else:
                    st.info("当前数据库中暂无数据，请通过下方“上传文件”或“目录下全部 CSV”导入一次。")
            else:
                if len(df_db) in LEGACY_DB_ROWS_TO_REPLACE and default_csv.exists():
                    try:
                        df_new = load_single_csv(str(default_csv))
                        if not df_new.empty:
                            save_to_db(df_new)
                            df_db = df_new
                    except Exception as e:
                        st.warning(f"检测到历史旧数据但自动替换失败：{e}")
                st.success(f"已从数据库加载，共 {len(df_db)} 条记录（所有用户共享）。")
                df = df_db

        elif source == "上传文件（覆盖数据库）":
            uploaded = st.file_uploader(
                "上传 CSV 或 Excel 文件（导入并覆盖数据库）",
                type=["csv", "xlsx", "xls"],
                help="支持 .csv 或 .xlsx。xlsx 会按分表自动识别进度表并合并（表头两行、含序号/项目分级/专业/拟定金额）。",
            )
            if uploaded is not None:
                suffix = Path(uploaded.name).suffix.lower() or ".csv"
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp.write(uploaded.getvalue())
                    tmp_path = tmp.name
                try:
                    name = uploaded.name
                    园区名 = "燕园" if "燕园" in name else ("蜀园" if "蜀园" in name else None)
                    df = load_uploaded(tmp_path, filename=name, 园区名=园区名)
                    if df.empty:
                        st.warning("文件已解析但未找到有效数据行。请确认：表头为两行，且含「序号」「项目分级」「专业」「拟定金额」等列。")
                    else:
                        st.success(f"已解析：{name}，共 {len(df)} 条记录。")
                        if st.button("✅ 将本次上传的数据保存为团队共享数据库（覆盖原有数据）", type="primary"):
                            save_to_db(df)
                            if _get_feishu_webhook_url():
                                if push_to_feishu(f"【养老社区进度表】已更新，共 {len(df)} 条记录。（上传文件：{name}）"):
                                    st.success("已保存到 SQLite 数据库并已推送至飞书。")
                                else:
                                    st.success("已保存到 SQLite 数据库。"); st.warning("飞书推送失败，请检查 Webhook 或网络。")
                            else:
                                st.success("已保存到 SQLite 数据库。所有用户刷新页面后将看到最新数据。")
                            st.rerun()
                except Exception as e:
                    st.error(f"解析失败：{e}")
                    import traceback
                    st.code(traceback.format_exc(), language=None)
            if df.empty and df_db.empty:
                single_path = st.text_input("或填写本地文件路径（.csv / .xlsx）并导入数据库", value=DEFAULT_SINGLE_FILE)
                if single_path and Path(single_path).exists():
                    try:
                        df = load_uploaded(single_path, filename=Path(single_path).name)
                        st.success(f"已从路径加载，共 {len(df)} 条记录。点击下方按钮保存到数据库。")
                        if st.button("保存到数据库", key="save_from_path"):
                            save_to_db(df)
                            if _get_feishu_webhook_url():
                                if push_to_feishu(f"【养老社区进度表】已更新，共 {len(df)} 条记录。（本地路径导入）"):
                                    st.success("已保存到 SQLite 数据库并已推送至飞书。")
                                else:
                                    st.success("已保存到 SQLite 数据库。"); st.warning("飞书推送失败，请检查 Webhook 或网络。")
                            else:
                                st.success("已保存到 SQLite 数据库。")
                            st.rerun()
                    except Exception as e:
                        st.error(f"加载失败：{e}")
                else:
                    st.info("请在上方上传 CSV/Excel，或填写有效的本地文件路径。")
            if df.empty and not df_db.empty:
                df = df_db

        else:  # 目录下全部 CSV（覆盖数据库）
            dir_path = st.text_input("数据目录路径（导入并覆盖数据库）", value=DEFAULT_DATA_DIR)
            pattern = st.text_input("文件名匹配", value="*养老*进度*.csv")
            if dir_path and Path(dir_path).is_dir():
                try:
                    df = load_from_directory(dir_path, pattern)
                    if df.empty:
                        st.warning("目录已扫描但未解析到有效数据，请检查文件名与表头格式。")
                    else:
                        st.success(f"已从目录加载，共 {len(df)} 条记录。")
                        if st.button("✅ 将目录数据保存为团队共享数据库（覆盖原有数据）", type="primary"):
                            save_to_db(df)
                            if _get_feishu_webhook_url():
                                if push_to_feishu(f"【养老社区进度表】已更新，共 {len(df)} 条记录。（目录导入）"):
                                    st.success("已保存到 SQLite 数据库并已推送至飞书。")
                                else:
                                    st.success("已保存到 SQLite 数据库。"); st.warning("飞书推送失败，请检查 Webhook 或网络。")
                            else:
                                st.success("已保存到 SQLite 数据库。所有用户刷新页面后将看到最新数据。")
                            st.rerun()
                except Exception as e:
                    st.error(f"加载失败：{e}")
            else:
                st.warning("请填写有效目录路径")

        if not df.empty:
            parks = df["园区"].dropna().unique().tolist()
            parks = [p for p in parks if p and str(p).strip() and str(p) != "未知园区"]
            if parks:
                园区选择 = st.multiselect("筛选园区", options=parks, default=parks)
            else:
                园区选择 = []
        else:
            园区选择 = []

    if df.empty:
        st.warning("请先在侧边栏选择或上传数据源。")
        render_审核流程说明()
        return

    # 列名/列顺序规范化，再补齐关键列
    df = _canonicalize_df(df)
    df = _ensure_project_columns(df)

    if not df.empty and len(df) > 10:
        has_prof = "专业" in df.columns and df["专业"].astype(str).str.strip().str.len().gt(0).sum() > len(df) // 2
        has_name = "项目名称" in df.columns and df["项目名称"].astype(str).str.strip().str.len().gt(0).sum() > len(df) // 2
        if not has_prof or not has_name:
            st.warning("当前数据中「专业」「项目名称」等列多为空，可能是旧库列对齐问题。请用侧边栏「上传文件（覆盖数据库）」重新上传 **改良改造报表-V4.csv** 并保存，即可修复显示。")

    # 自动添加城市和区域列（用于地图与导出）
    df = _add_城市和区域列(df)

    st.subheader("项目录入 / 修改向导")
    st.caption("按步骤逐条填写项目数据，自动生成所属区域、城市与上传凭证。")
    if _get_feishu_webhook_url():
        st.info("💬 只要修改了数据并保存，飞书将自动收到消息推送。")
    _render_project_wizard(df)


if __name__ == "__main__":
    main()
