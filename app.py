# -*- coding: utf-8 -*-
"""
养老社区改良改造 - 项目新增/修改向导 + 飞书推送
从进度管理看板提取的独立功能模块。
"""
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import date, datetime
import io
import tempfile
import os
import sqlite3
import uuid
import time
from pathlib import Path as _Path

try:
    import tomllib  # py3.11+
except Exception:  # pragma: no cover
    tomllib = None

from data_loader import load_single_csv, load_from_directory, load_uploaded, TIMELINE_COLS
from location_config import 园区_TO_城市, 园区_TO_区域
from feishu_client import (
    get_feishu_webhook_url,
    push_to_feishu,
    build_feishu_payload_from_diff,
    row_to_dict,
    format_cell,
)
try:
    from feishu_doc_loader import load_from_feishu_doc, get_last_error as get_feishu_doc_last_error
    FEISHU_DOC_AVAILABLE = True
except Exception:
    def get_feishu_doc_last_error() -> str:
        return ""
    FEISHU_DOC_AVAILABLE = False
try:
    from feishu_oauth import build_authorize_url, exchange_code_for_user
    FEISHU_OAUTH_AVAILABLE = True
except Exception:
    FEISHU_OAUTH_AVAILABLE = False


# ---------- 团队共享数据：SQLite 存储 ----------
DB_PATH = os.getenv("APP203_DB_PATH", "app203_projects.db")
DEFAULT_DATA_DIR = str(Path(__file__).resolve().parent)
DEFAULT_ENCRYPTED_FILE = str(Path(__file__).resolve().parent / "改良改造报表-V4.csv.enc")
DEFAULT_SINGLE_FILE = str(Path(__file__).resolve().parent / "改良改造报表-V4.csv")
LEGACY_DB_ROWS_TO_REPLACE = {337}
DEFAULT_FEISHU_DOC_URL = os.getenv(
    "FEISHU_DEFAULT_DOC_URL",
    "https://tkhome.feishu.cn/wiki/DFIYwb1ELigVNgkdJQAcoPArnRg?sheet=0zsvcA",
)


def _get_db_connection():
    return sqlite3.connect(DB_PATH)


def _get_config_value(key: str, default: str = "") -> str:
    """读取配置：环境变量 > st.secrets > 本地 secrets.toml。"""
    v = os.getenv(key, "").strip()
    if v:
        return v
    try:
        if hasattr(st, "secrets") and st.secrets:
            sv = str(st.secrets.get(key, "")).strip()
            if sv:
                return sv
    except Exception:
        pass
    if tomllib is None:
        return default
    candidate_files = [
        _Path.home() / ".streamlit" / "secrets.toml",
        _Path(__file__).resolve().parent / ".streamlit" / "secrets.toml",
    ]
    for fp in candidate_files:
        if not fp.exists():
            continue
        try:
            data = tomllib.loads(fp.read_text(encoding="utf-8"))
            vv = str(data.get(key, "")).strip()
            if vv:
                return vv
        except Exception:
            continue
    return default


def _has_feishu_app_credentials() -> bool:
    """检查 FEISHU_APP_ID/FEISHU_APP_SECRET 是否已配置（环境变量或 secrets.toml）。"""
    return bool(_get_config_value("FEISHU_APP_ID") and _get_config_value("FEISHU_APP_SECRET"))


def _require_feishu_login() -> bool:
    """登录门禁：开启后仅飞书员工可访问。"""
    login_required = _get_config_value("FEISHU_LOGIN_REQUIRED", "")
    if login_required == "0":
        return True
    if login_required != "1":
        app_id = _get_config_value("FEISHU_APP_ID")
        secret = _get_config_value("FEISHU_APP_SECRET")
        redirect = _get_config_value("FEISHU_REDIRECT_URI")
        if not (app_id and secret and redirect):
            return True
    if not FEISHU_OAUTH_AVAILABLE:
        st.warning("飞书登录模块未就绪，请确认 feishu_oauth.py 存在。")
        return True

    app_id = _get_config_value("FEISHU_APP_ID")
    secret = _get_config_value("FEISHU_APP_SECRET")
    redirect = _get_config_value("FEISHU_REDIRECT_URI")
    if not app_id or not secret or not redirect:
        st.warning("请配置 FEISHU_APP_ID、FEISHU_APP_SECRET、FEISHU_REDIRECT_URI 以启用飞书登录。")
        return True

    # 兼容配置在 secrets.toml 的场景，供 feishu_oauth 模块读取
    os.environ.setdefault("FEISHU_APP_ID", app_id)
    os.environ.setdefault("FEISHU_APP_SECRET", secret)
    os.environ.setdefault("FEISHU_REDIRECT_URI", redirect)

    if st.session_state.get("feishu_user"):
        return True

    code = st.query_params.get("code")
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

    auth_url = build_authorize_url(redirect, state="project-wizard")
    st.markdown("### 请先登录")
    st.markdown("仅泰康飞书员工可访问此项目。")
    st.link_button("飞书登录", auth_url, type="primary")
    return False


def load_from_db() -> pd.DataFrame:
    """从 SQLite 加载团队共享数据表 projects。"""
    if not Path(DB_PATH).exists():
        return pd.DataFrame()
    try:
        with _get_db_connection() as conn:
            return pd.read_sql("SELECT * FROM projects", conn)
    except Exception:
        return pd.DataFrame()


def save_to_db(df: pd.DataFrame):
    """将 DataFrame 全量写入 SQLite（覆盖 projects 表）。"""
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
    ] + list(TIMELINE_COLS)
    out = df.copy()
    for col in needed:
        if col not in out.columns:
            out[col] = "" if col not in ["序号", "拟定金额"] else 0
    return out


def _strip_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """去掉列名为空字符串的列。"""
    keep_cols = [c for c in df.columns if str(c).strip() != ""]
    return df[keep_cols].copy()


def _canonicalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """加载后统一规范化。"""
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


DATE_FORMAT = "YYYY-MM-DD"
REQ_SUFFIX = " *"  # 必填标记放在标签后：园区 *
HELP_园区 = "请选择项目所在园区，必填"
HELP_项目分级 = "一级为最高优先级，必填"
HELP_项目分类 = "如品质提升、大修、安全等，必填"
HELP_所属业态 = "如独立、护理、其他，必填"
HELP_专业 = "如土建设施、供配电系统等，必填"
HELP_项目名称 = "请填写项目名称，必填"
HELP_预算金额 = "请填写预算金额，单位：万元，必填"
# 下拉选项（可扩展，来自数据 + 预设）
OPT_所属业态 = ["独立", "护理", "其他"]
OPT_项目分级 = ["一级（最高级）", "二级", "三级"]
OPT_项目分类 = ["品质提升", "大修", "安全", "运营需求", "节能改造", "智能化提升", "金额10万以上的常规维修", "金额10万以上的房态更新", "其他改造"]
OPT_拟定承建组织 = ["不动产项目部", "社区分包", "社区负责"]
OPT_总部重点关注 = ["是", "否", ""]
专业大类 = ["土建设施", "供配电系统", "暖通/供冷系统", "弱电系统", "供排水系统", "电梯系统", "其它系统", "消防系统", "安防系统"]
SENTINEL_DATE = date(2000, 1, 1)  # 表示未填写
DATE_RANGE_MIN = date(2020, 1, 1)
DATE_RANGE_MAX = date(2030, 12, 31)
DATE_DEFAULT = date(2025, 1, 1)  # 默认从 2025 年开始选择

def _get_dropdown_options(df: pd.DataFrame, col: str, extras: list = None) -> list:
    """从数据中提取唯一值 + 额外选项，用于下拉。"""
    opts = []
    if col in df.columns:
        opts = sorted(df[col].dropna().astype(str).unique().tolist())
    if extras:
        opts = sorted(set(opts) | set(extras))
    return [x for x in opts if x and str(x).strip() != "nan"]

def _date_to_str(d) -> str:
    """日期转 YYYY-MM-DD，SENTINEL_DATE 或 None 转为空。"""
    if d is None or (hasattr(d, "year") and d.year == 2000 and d.month == 1 and d.day == 1):
        return ""
    if isinstance(d, date):
        return d.strftime("%Y-%m-%d")
    return str(d) if d else ""

def _str_to_date(s):
    """字符串转 date，空或无效则返回 SENTINEL_DATE。仅支持 2020-2030 年。"""
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


def _get_default_node_date(existing_d: date = None) -> date:
    """节点日期默认值：修改时已有且在范围内用已有；否则默认 2025-01-01。"""
    if existing_d and existing_d != SENTINEL_DATE and DATE_RANGE_MIN <= existing_d <= DATE_RANGE_MAX:
        return existing_d
    return DATE_DEFAULT

def _normalize_date(s) -> str:
    """将日期字符串规范为 YYYY-MM-DD 格式，无效则返回空。"""
    if s is None or (isinstance(s, str) and not s.strip()):
        return ""
    s = str(s).strip()
    if not s:
        return ""
    try:
        dt = pd.to_datetime(s, errors="coerce", format="mixed")
        if pd.isna(dt):
            return s
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return s

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


def _safe_sheet_name(name: str, used: set[str]) -> str:
    """生成合法且唯一的 Excel sheet 名（<=31 字符）。"""
    raw = str(name or "").strip() or "未命名"
    for ch in ['\\', '/', '*', '?', ':', '[', ']']:
        raw = raw.replace(ch, "_")
    base = raw[:31] or "未命名"
    candidate = base
    idx = 2
    while candidate in used:
        suffix = f"_{idx}"
        candidate = (base[: 31 - len(suffix)] + suffix) if len(base) + len(suffix) > 31 else (base + suffix)
        idx += 1
    used.add(candidate)
    return candidate


def _build_multisheet_excel_bytes(df: pd.DataFrame) -> bytes:
    """导出为按园区分 sheet 的 Excel，同时保留全部项目总表。"""
    output = io.BytesIO()
    used_names: set[str] = set()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        all_sheet = _safe_sheet_name("全部项目", used_names)
        df.to_excel(writer, index=False, sheet_name=all_sheet)

        if "园区" in df.columns:
            parks = sorted([p for p in df["园区"].dropna().astype(str).unique().tolist() if p and p != "nan"])
            for park in parks:
                sub = df[df["园区"].astype(str) == park].copy()
                if sub.empty:
                    continue
                sheet = _safe_sheet_name(park, used_names)
                sub.to_excel(writer, index=False, sheet_name=sheet)
    output.seek(0)
    return output.getvalue()


def _render_project_wizard(df: pd.DataFrame):
    """项目新增 / 修改：平铺表单。"""
    df_all = _ensure_project_columns(df)

    mode = st.radio("操作类型", ["新增项目", "修改已有项目"], horizontal=True)

    if mode == "修改已有项目":
        st.markdown("### 步骤 1：下拉选择要修改的数据")
        candidates = df_all.reset_index(drop=True).copy()
        candidates["_rid"] = candidates.index.astype(int)
        if candidates.empty:
            st.info("暂无可修改项目，请先新增或导入数据。")
            return

        def _fmt_rid(rid: int) -> str:
            row = candidates[candidates["_rid"] == rid].iloc[0]
            seq = str(row.get("序号", "")).strip()
            park = str(row.get("园区", "")).strip()
            name = str(row.get("项目名称", "")).strip()
            return f"序号{seq} | {park or '未标注园区'} | {name or '未命名项目'}"

        chosen_rid = st.selectbox(
            "选择要修改的项目",
            options=candidates["_rid"].tolist(),
            format_func=_fmt_rid,
        )
        target_row = candidates[candidates["_rid"] == int(chosen_rid)].iloc[0]

        st.markdown("---")
        st.markdown(f"### 步骤 2：编辑项目（序号 {int(target_row['序号'])}）")

        st.markdown("**项目节点**（先选择节点，再选择日期，支持 2020-2030 年）")
        选择节点 = st.selectbox("选择要更新的项目节点", options=["（不更新节点）"] + list(TIMELINE_COLS), key="edit_node_select")
        edit_selected_date = None
        if 选择节点 != "（不更新节点）":
            raw_val = target_row.get(选择节点, "")
            existing_d = _str_to_date(raw_val)
            default_d = _get_default_node_date(existing_d)
            edit_selected_date = st.date_input(
                f"「{选择节点}」日期",
                value=default_d,
                min_value=DATE_RANGE_MIN,
                max_value=DATE_RANGE_MAX,
                format="YYYY-MM-DD",
                key="edit_date_picker",
            )

        with st.form("edit_project_form"):
            st.caption("提示：保存后将自动推送到飞书。带 * 为必填项。")
            st.markdown("**基础信息**")
            c1, c2, c3 = st.columns(3)
            with c1:
                园区_options = sorted(set(df_all["园区"].dropna().astype(str).tolist()) | set(园区_TO_城市.keys()))
                园区_options = [x for x in 园区_options if x]
                园区默认 = str(target_row.get("园区", ""))
                园区 = st.selectbox("园区" + REQ_SUFFIX, options=[""] + 园区_options, index=(园区_options.index(园区默认) + 1) if 园区默认 in 园区_options else 0, help=HELP_园区)
            with c2:
                区域_opts = _get_dropdown_options(df_all, "所属区域", list(园区_TO_区域.values()))
                _v = str(target_row.get("所属区域", ""))
                所属区域 = st.selectbox("所属区域", options=[""] + 区域_opts, index=区域_opts.index(_v) + 1 if _v in 区域_opts and 区域_opts else 0)
                城市_opts = _get_dropdown_options(df_all, "城市", list(园区_TO_城市.values()))
                _cv = str(target_row.get("城市", ""))
                城市 = st.selectbox("所在城市", options=[""] + 城市_opts, index=城市_opts.index(_cv) + 1 if _cv in 城市_opts and 城市_opts else 0)
            with c3:
                业态_opts = _get_dropdown_options(df_all, "所属业态", OPT_所属业态)
                _ev = str(target_row.get("所属业态", ""))
                所属业态 = st.selectbox("所属业态" + REQ_SUFFIX, options=[""] + 业态_opts, index=业态_opts.index(_ev) + 1 if _ev in 业态_opts and 业态_opts else 0, help=HELP_所属业态)

            st.markdown("**项目属性**")
            c4, c5, c6 = st.columns(3)
            with c4:
                分级_opts = _get_dropdown_options(df_all, "项目分级", OPT_项目分级)
                _lv = str(target_row.get("项目分级", ""))
                项目分级 = st.selectbox("项目分级" + REQ_SUFFIX, options=[""] + 分级_opts, index=分级_opts.index(_lv) + 1 if _lv in 分级_opts and 分级_opts else 0, help=HELP_项目分级)
            with c5:
                分类_opts = _get_dropdown_options(df_all, "项目分类", OPT_项目分类)
                _cv = str(target_row.get("项目分类", ""))
                项目分类 = st.selectbox("项目分类" + REQ_SUFFIX, options=[""] + 分类_opts, index=分类_opts.index(_cv) + 1 if _cv in 分类_opts and 分类_opts else 0, help=HELP_项目分类)
            with c6:
                承建_opts = _get_dropdown_options(df_all, "拟定承建组织", OPT_拟定承建组织)
                _bv = str(target_row.get("拟定承建组织", ""))
                拟定承建组织 = st.selectbox("拟定承建组织", options=[""] + 承建_opts, index=承建_opts.index(_bv) + 1 if _bv in 承建_opts and 承建_opts else 0)

            c7, c8 = st.columns(2)
            with c7:
                总部_opts = [x for x in _get_dropdown_options(df_all, "总部重点关注项目", OPT_总部重点关注) if x]
                _zv = str(target_row.get("总部重点关注项目", ""))
                总部重点关注项目 = st.selectbox("总部重点关注项目", options=[""] + 总部_opts, index=总部_opts.index(_zv) + 1 if _zv in 总部_opts and 总部_opts else 0)
            with c8:
                拟定金额 = st.number_input("预算金额（万元）" + REQ_SUFFIX, min_value=0.0, value=float(target_row.get("拟定金额") or 0.0), step=1.0, help=HELP_预算金额)

            st.markdown("**专业与名称**")
            c9, c10 = st.columns(2)
            with c9:
                专业_opts = _get_dropdown_options(df_all, "专业", 专业大类)
                _pv = str(target_row.get("专业", ""))
                专业 = st.selectbox("专业" + REQ_SUFFIX, options=[""] + 专业_opts, index=专业_opts.index(_pv) + 1 if _pv in 专业_opts and 专业_opts else 0, help=HELP_专业)
            with c10:
                分包_opts = _get_dropdown_options(df_all, "专业分包")
                _sbv = str(target_row.get("专业分包", ""))
                专业分包 = st.selectbox("专业分包", options=[""] + 分包_opts, index=分包_opts.index(_sbv) + 1 if _sbv in 分包_opts and 分包_opts else 0)
            项目名称 = st.text_input("项目名称" + REQ_SUFFIX, value=str(target_row.get("项目名称", "")), help=HELP_项目名称)
            备注说明 = st.text_area("备注说明", value=str(target_row.get("备注说明", "")))

            col_save, col_del = st.columns(2)
            with col_save:
                submitted = st.form_submit_button("💾 保存修改")
            with col_del:
                delete_clicked = st.form_submit_button("🗑 删除该项目")

        seq_val = int(target_row["序号"])
        if delete_clicked:
            df_new = df_all[df_all["序号"].astype(int) != seq_val].copy()
            save_to_db(df_new)
            if get_feishu_webhook_url():
                diff = {"deleted": [row_to_dict(target_row)], "added": [], "modified": []}
                payload = build_feishu_payload_from_diff(diff, len(df_new), source="向导删除")
                push_to_feishu(payload=payload)
            st.success(f"已删除序号为 {seq_val} 的项目。")
            st.rerun()

        if submitted:
            required_edit = ["园区", "项目分级", "项目分类", "所属业态", "专业", "项目名称"]
            edit_vals = {"园区": 园区, "项目分级": 项目分级, "项目分类": 项目分类, "所属业态": 所属业态, "专业": 专业, "项目名称": 项目名称}
            missing_edit = [k for k in required_edit if not str(edit_vals.get(k, "")).strip()]
            if missing_edit:
                st.error(f"以下字段为必填：{', '.join(missing_edit)}")
            else:
                date_values = {}
                for col in TIMELINE_COLS:
                    if 选择节点 == col and edit_selected_date is not None:
                        date_values[col] = edit_selected_date
                    else:
                        raw_val = target_row.get(col, "")
                        date_values[col] = _str_to_date(raw_val)
                df_new = df_all.copy()
                mask = df_new["序号"].astype(int) == seq_val
                update_dict = {
                    "园区": 园区, "所属区域": 所属区域, "城市": 城市, "所属业态": 所属业态,
                    "项目分级": 项目分级, "项目分类": 项目分类, "拟定承建组织": 拟定承建组织,
                    "总部重点关注项目": 总部重点关注项目, "专业": 专业, "专业分包": 专业分包,
                    "项目名称": 项目名称, "备注说明": 备注说明, "拟定金额": 拟定金额,
                }
                for col, val in update_dict.items():
                    if col in df_new.columns:
                        df_new.loc[mask, col] = val
                for col, val in date_values.items():
                    if col not in df_new.columns:
                        df_new[col] = ""
                    df_new.loc[mask, col] = _date_to_str(val)
                save_to_db(df_new)
                if get_feishu_webhook_url():
                    modified_row = df_new.loc[mask].iloc[0]
                    changes = []
                    for col in target_row.index:
                        if col not in modified_row.index:
                            continue
                        ov = format_cell(target_row[col])
                        nv = format_cell(modified_row[col])
                        if ov != nv:
                            changes.append(f"{col}：{ov or '（空）'} → {nv or '（空）'}")
                    modified_details = [{"序号": seq_val, "变更项": changes}]
                    diff = {
                        "deleted": [], "added": [], "modified": [row_to_dict(modified_row)],
                        "modified_details": modified_details,
                    }
                    payload = build_feishu_payload_from_diff(diff, len(df_new), source="向导修改")
                    push_to_feishu(payload=payload)
                st.success("已保存修改。")
                st.rerun()
        return

    # ---------- 新增项目 ----------
    st.markdown("### 新增项目")
    df_all = _ensure_project_columns(df_all)
    next_seq = _get_next_序号(df_all)
    required_fields = ["园区", "所属业态", "项目分级", "项目分类", "专业", "项目名称"]

    st.markdown("**项目节点**（先选择节点，再选择日期，支持 2020-2030 年）")
    选择节点 = st.selectbox("选择要更新的项目节点", options=["（不更新节点）"] + list(TIMELINE_COLS), key="add_node_select")
    add_selected_date = None
    if 选择节点 != "（不更新节点）":
        default_d = _get_default_node_date(None)
        add_selected_date = st.date_input(
            f"「{选择节点}」日期",
            value=default_d,
            min_value=DATE_RANGE_MIN,
            max_value=DATE_RANGE_MAX,
            format="YYYY-MM-DD",
            key="add_date_picker",
        )

    with st.form("add_project_form"):
        st.caption(f"新项目序号将自动设置为：{next_seq}")
        st.caption("提示：保存后将自动推送到飞书。带 * 为必填项。")

        c1, c2, c3 = st.columns(3)
        with c1:
            parks = sorted(set(df_all["园区"].dropna().astype(str).tolist()) | set(园区_TO_城市.keys()))
            parks = [x for x in parks if x]
            园区 = st.selectbox("园区" + REQ_SUFFIX, options=[""] + parks, help=HELP_园区)
        with c2:
            区域_opts = _get_dropdown_options(df_all, "所属区域", list(园区_TO_区域.values()))
            所属区域 = st.selectbox("所属区域", options=[""] + 区域_opts)
        with c3:
            城市_opts = _get_dropdown_options(df_all, "城市", list(园区_TO_城市.values()))
            城市 = st.selectbox("所在城市", options=[""] + 城市_opts)

        c4, c5, c6 = st.columns(3)
        with c4:
            业态_opts = _get_dropdown_options(df_all, "所属业态", OPT_所属业态)
            所属业态 = st.selectbox("所属业态" + REQ_SUFFIX, options=[""] + 业态_opts, help=HELP_所属业态)
        with c5:
            分级_opts = _get_dropdown_options(df_all, "项目分级", OPT_项目分级)
            项目分级 = st.selectbox("项目分级" + REQ_SUFFIX, options=[""] + 分级_opts, help=HELP_项目分级)
        with c6:
            分类_opts = _get_dropdown_options(df_all, "项目分类", OPT_项目分类)
            项目分类 = st.selectbox("项目分类" + REQ_SUFFIX, options=[""] + 分类_opts, help=HELP_项目分类)

        c7, c8 = st.columns(2)
        with c7:
            承建_opts = _get_dropdown_options(df_all, "拟定承建组织", OPT_拟定承建组织)
            拟定承建组织 = st.selectbox("拟定承建组织", options=[""] + 承建_opts)
        with c8:
            总部_opts = _get_dropdown_options(df_all, "总部重点关注项目", OPT_总部重点关注)
            总部重点关注项目 = st.selectbox("总部重点关注项目", options=[""] + [x for x in 总部_opts if x])

        c9, c10 = st.columns(2)
        with c9:
            专业_opts = _get_dropdown_options(df_all, "专业", 专业大类)
            专业 = st.selectbox("专业" + REQ_SUFFIX, options=[""] + 专业_opts, help=HELP_专业)
        with c10:
            分包_opts = _get_dropdown_options(df_all, "专业分包")
            专业分包 = st.selectbox("专业分包", options=[""] + 分包_opts)

        项目名称 = st.text_input("项目名称" + REQ_SUFFIX, help=HELP_项目名称)
        备注说明 = st.text_area("备注说明")
        拟定金额 = st.number_input("预算金额（万元）" + REQ_SUFFIX, min_value=0.0, value=0.0, step=1.0, help=HELP_预算金额)

        submitted = st.form_submit_button("✅ 完成并写入数据库")

    if submitted:
        form_dict = {
            "序号": next_seq,
            "园区": 园区, "所属区域": 所属区域, "城市": 城市, "所属业态": 所属业态,
            "项目分级": 项目分级, "项目分类": 项目分类, "拟定承建组织": 拟定承建组织,
            "总部重点关注项目": 总部重点关注项目, "专业": 专业, "专业分包": 专业分包,
            "项目名称": 项目名称, "备注说明": 备注说明, "拟定金额": 拟定金额,
        }
        missing = [k for k in required_fields if not str(form_dict.get(k, "")).strip()]
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
            if 选择节点 == col and add_selected_date is not None:
                form_dict[col] = _date_to_str(add_selected_date)
            else:
                form_dict[col] = _date_to_str(SENTINEL_DATE)

        df_new_row = pd.DataFrame([form_dict])
        df_all2 = pd.concat([df_all, df_new_row], ignore_index=True)
        save_to_db(df_all2)
        if get_feishu_webhook_url():
            diff = {"deleted": [], "added": [row_to_dict(df_new_row.iloc[0])], "modified": []}
            payload = build_feishu_payload_from_diff(diff, len(df_all2), source="向导新增")
            push_to_feishu(payload=payload)
        st.success(f"已写入数据库。上传凭证：{token}")
        st.info("请截图或记录该凭证号，后续如需确认或审计可用于检索。")
        st.rerun()


def main():
    # 兼容入口：仓库默认用 app.py 启动。
    # 将入口统一到 app203.py，避免两套 UI 逻辑分叉维护。
    from app203 import main as _main_app203
    _main_app203()
    return

    if not _require_feishu_login():
        return

    st.title("养老社区改良改造 - 项目新增/修改向导")
    st.info("📤 支持项目录入、修改、删除，**保存时自动推送到飞书**")

    with st.sidebar:
        if st.session_state.get("feishu_user"):
            u = st.session_state["feishu_user"]
            name = u.get("name") or u.get("user_id") or u.get("open_id", "未知")
            st.caption(f"👤 {name}")
            if st.button("退出登录", key="logout"):
                del st.session_state["feishu_user"]
                st.rerun()
        st.header("数据源")
        source = st.radio(
            "数据来源",
            ["数据库（团队共享）", "飞书文档（默认）", "上传文件（覆盖数据库）", "目录下全部 CSV（覆盖数据库）"],
            index=0,
        )
        df_db = load_from_db()
        df = pd.DataFrame()

        if source == "数据库（团队共享）":
            default_csv = Path(DEFAULT_ENCRYPTED_FILE) if Path(DEFAULT_ENCRYPTED_FILE).exists() else Path(DEFAULT_SINGLE_FILE)
            if df_db.empty:
                if default_csv.exists():
                    try:
                        df = load_single_csv(str(default_csv))
                        if not df.empty:
                            save_to_db(df)
                            if get_feishu_webhook_url():
                                if push_to_feishu(f"【养老社区进度表】已用「{default_csv.name}」初始化，共 {len(df)} 条记录。"):
                                    st.success(f"已用「{default_csv.name}」初始化团队共享数据库，共 {len(df)} 条记录；已推送至飞书。")
                                else:
                                    st.success(f"已用「{default_csv.name}」初始化团队共享数据库，共 {len(df)} 条记录。")
                                    st.warning("飞书推送失败，请检查网络或配置。")
                            else:
                                st.success(f"已用「{default_csv.name}」初始化团队共享数据库，共 {len(df)} 条记录。")
                        else:
                            st.info("当前数据库中暂无数据，请通过下方“上传文件”或“目录下全部 CSV”导入一次。")
                    except Exception as e:
                        st.warning(f"无法从默认 CSV 加载：{e}。请通过下方“上传文件”导入。")
                else:
                    st.info("当前数据库中暂无数据，请通过下方“上传文件”或“目录下全部 CSV”导入一次。")
            else:
                # 兼容历史旧库：检测到旧版 337 条时，自动替换为新的默认加密数据
                if len(df_db) in LEGACY_DB_ROWS_TO_REPLACE and default_csv.exists():
                    try:
                        df_new = load_single_csv(str(default_csv))
                        if not df_new.empty:
                            save_to_db(df_new)
                            df_db = df_new
                    except Exception as e:
                        st.warning(f"检测到历史旧数据但自动替换失败：{e}")
                st.success(f"已从数据库加载，共 {len(df_db)} 条记录。")
                df = df_db

        elif source == "飞书文档（默认）":
            if not FEISHU_DOC_AVAILABLE:
                st.warning("未安装飞书文档加载模块，请确认 feishu_doc_loader.py 存在。")
            elif not _has_feishu_app_credentials():
                st.warning("请先配置 FEISHU_APP_ID 和 FEISHU_APP_SECRET（环境变量或 Streamlit Secrets）。")
            else:
                doc_url = st.text_input(
                    "飞书文档链接",
                    value=DEFAULT_FEISHU_DOC_URL,
                    placeholder="https://xxx.feishu.cn/wiki/xxxx?sheet=xxxx",
                    help="默认使用项目预置链接；支持 wiki 文档，自动识别多 sheet/多表并清洗。",
                )
                auto_sync = st.checkbox("自动拉取（推荐）", value=True, key="feishu_doc_auto_sync")
                if doc_url.strip():
                    now_ts = time.time()
                    cache_url = st.session_state.get("feishu_doc_cache_url", "")
                    cache_ts = float(st.session_state.get("feishu_doc_cache_ts", 0.0) or 0.0)
                    cache_df = st.session_state.get("df_from_feishu_doc")
                    cache_valid = (
                        isinstance(cache_df, pd.DataFrame)
                        and not cache_df.empty
                        and cache_url == doc_url.strip()
                        and (now_ts - cache_ts) <= 300
                    )

                    # 自动拉取：首次进入 / URL 变更 / 缓存超时（5分钟）时自动刷新
                    if auto_sync and not cache_valid:
                        with st.spinner("正在自动从飞书拉取并清洗..."):
                            try:
                                loaded = load_from_feishu_doc(doc_url.strip())
                            except Exception as e:
                                loaded = pd.DataFrame()
                                st.warning(f"自动拉取异常：{e}")
                        if loaded.empty:
                            reason = get_feishu_doc_last_error()
                            msg = "自动拉取失败：未获取到数据，请检查链接、应用权限与文档共享设置。"
                            st.warning(f"{msg}\n{reason}" if reason else msg)
                        else:
                            st.session_state["df_from_feishu_doc"] = loaded
                            st.session_state["feishu_doc_cache_url"] = doc_url.strip()
                            st.session_state["feishu_doc_cache_ts"] = now_ts
                            st.success(f"自动拉取成功，共 {len(loaded)} 条记录。")

                    if st.button("🔄 立即刷新", key="load_feishu_doc"):
                        with st.spinner("正在从飞书拉取并清洗..."):
                            try:
                                loaded = load_from_feishu_doc(doc_url.strip())
                            except Exception as e:
                                loaded = pd.DataFrame()
                                st.error(f"拉取异常：{e}")
                        if loaded.empty:
                            reason = get_feishu_doc_last_error()
                            msg = "未获取到数据，请检查链接、应用权限与文档共享设置。"
                            st.warning(f"{msg}\n{reason}" if reason else msg)
                        else:
                            st.session_state["df_from_feishu_doc"] = loaded
                            st.session_state["feishu_doc_cache_url"] = doc_url.strip()
                            st.session_state["feishu_doc_cache_ts"] = time.time()
                            st.success(f"已从飞书加载并清洗，共 {len(loaded)} 条记录。")
                            st.rerun()

                    if "df_from_feishu_doc" in st.session_state and isinstance(st.session_state["df_from_feishu_doc"], pd.DataFrame):
                        df = st.session_state["df_from_feishu_doc"]
                        st.caption(f"当前显示飞书数据，共 {len(df)} 条。确认无误后可覆盖写入数据库。")
                        if st.button("✅ 保存到数据库（覆盖原有数据）", type="primary", key="save_feishu_doc_to_db"):
                            save_to_db(df)
                            if get_feishu_webhook_url():
                                if push_to_feishu(f"【养老社区进度表】已从飞书文档导入并清洗，共 {len(df)} 条记录。"):
                                    st.success("已保存到 SQLite 并推送至飞书。")
                                else:
                                    st.success("已保存到 SQLite。")
                                    st.warning("飞书推送失败，请检查网络或配置。")
                            else:
                                st.success("已保存到 SQLite。")
                            st.rerun()
                else:
                    st.info("请填写飞书文档链接。")

        elif source == "上传文件（覆盖数据库）":
            uploaded = st.file_uploader("上传 CSV 或 Excel 文件", type=["csv", "xlsx", "xls"])
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
                        st.warning("文件已解析但未找到有效数据行。")
                    else:
                        st.success(f"已解析：{name}，共 {len(df)} 条记录。")
                        if st.button("✅ 保存到数据库（覆盖原有数据）", type="primary"):
                            save_to_db(df)
                            if get_feishu_webhook_url():
                                if push_to_feishu(f"【养老社区进度表】已更新，共 {len(df)} 条记录。（上传文件：{name}）"):
                                    st.success("已保存到 SQLite 并已推送至飞书。")
                                else:
                                    st.success("已保存到 SQLite。"); st.warning("飞书推送失败，请检查配置。")
                            else:
                                st.success("已保存到 SQLite。")
                            st.rerun()
                except Exception as e:
                    st.error(f"解析失败：{e}")
            if df.empty and df_db.empty:
                single_path = st.text_input("或填写本地文件路径", value=DEFAULT_SINGLE_FILE)
                if single_path and Path(single_path).exists():
                    try:
                        df = load_uploaded(single_path, filename=Path(single_path).name)
                        if st.button("保存到数据库", key="save_from_path"):
                            save_to_db(df)
                            if get_feishu_webhook_url():
                                push_to_feishu(f"【养老社区进度表】已更新，共 {len(df)} 条记录。")
                            st.rerun()
                    except Exception as e:
                        st.error(f"加载失败：{e}")
            if df.empty and not df_db.empty:
                df = df_db

        else:
            dir_path = st.text_input("数据目录路径", value=DEFAULT_DATA_DIR)
            pattern = st.text_input("文件名匹配", value="*养老*进度*.csv")
            if dir_path and Path(dir_path).is_dir():
                try:
                    df = load_from_directory(dir_path, pattern)
                    if not df.empty:
                        st.success(f"已从目录加载，共 {len(df)} 条记录。")
                        if st.button("✅ 保存到数据库（覆盖原有数据）", type="primary"):
                            save_to_db(df)
                            if get_feishu_webhook_url():
                                push_to_feishu(f"【养老社区进度表】已更新，共 {len(df)} 条记录。（目录导入）")
                            st.rerun()
                except Exception as e:
                    st.error(f"加载失败：{e}")

    if df.empty:
        st.warning("请先在侧边栏选择或上传数据源。")
        return

    df = _canonicalize_df(df)
    df = _ensure_project_columns(df)

    st.download_button(
        "📥 导出Excel（按园区分Sheet）",
        data=_build_multisheet_excel_bytes(df),
        file_name=f"改良改造项目_分Sheet_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        help="导出包含「全部项目」总表，并按园区自动拆分多个工作表。",
    )
    _render_project_wizard(df)


if __name__ == "__main__":
    main()
