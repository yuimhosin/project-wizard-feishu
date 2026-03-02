# -*- coding: utf-8 -*-
"""
养老社区改良改造 - 项目新增/修改向导 + 飞书推送
从进度管理看板提取的独立功能模块。
"""
import streamlit as st
import pandas as pd
from pathlib import Path
import tempfile
import os
import sqlite3
import uuid

from data_loader import load_single_csv, load_from_directory, load_uploaded, TIMELINE_COLS
from location_config import 园区_TO_城市, 园区_TO_区域
from feishu_client import (
    get_feishu_webhook_url,
    push_to_feishu,
    build_feishu_payload_from_diff,
    row_to_dict,
    format_cell,
)


# ---------- 团队共享数据：SQLite 存储 ----------
DB_PATH = os.getenv("APP203_DB_PATH", "app203_projects.db")
DEFAULT_DATA_DIR = str(Path(__file__).resolve().parent)
DEFAULT_SINGLE_FILE = str(Path(__file__).resolve().parent / "改良改造报表-V4.csv")


def _get_db_connection():
    return sqlite3.connect(DB_PATH)


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
    ]
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


def _render_project_wizard(df: pd.DataFrame):
    """项目新增 / 修改：平铺表单。"""
    df_all = _ensure_project_columns(df)

    mode = st.radio("操作类型", ["新增项目", "修改已有项目"], horizontal=True)

    if mode == "修改已有项目":
        st.markdown("### 步骤 1：查找要修改的项目")
        # 1. 按园区筛选（优先）
        园区列表 = sorted(df["园区"].dropna().astype(str).unique().tolist())
        园区列表 = [p for p in 园区列表 if p and str(p).strip() and str(p) != "nan"]
        园区选择 = st.multiselect(
            "先选择园区（选择后自动显示该园区下项目）",
            options=园区列表,
            default=[],
            key="wizard_search_园区",
        )
        candidates = df.copy()
        if 园区选择:
            candidates = candidates[candidates["园区"].astype(str).isin(园区选择)]
            st.caption(f"已筛选园区：{', '.join(园区选择)}，共 {len(candidates)} 条")

        # 2. 序号、项目名称进一步筛选
        col1, col2 = st.columns(2)
        with col1:
            seq_input = st.text_input("按序号查找（可选）", value="", placeholder="例如：12")
        with col2:
            name_kw = st.text_input("按项目名称关键词查找（可选）", value="", placeholder="例如：配电、外立面等")

        if seq_input.strip():
            try:
                seq_val = int(float(seq_input.strip()))
                candidates = candidates[pd.to_numeric(candidates["序号"], errors="coerce") == seq_val]
            except ValueError:
                candidates = candidates.iloc[0:0]
        if name_kw.strip():
            candidates = candidates[candidates["项目名称"].astype(str).str.contains(name_kw.strip(), na=False)]

        if not 园区选择 and not seq_input.strip() and not name_kw.strip():
            st.info("请至少选择园区、或输入序号、或输入项目名称关键词进行筛选。")
            return

        if candidates.empty:
            st.info("未找到匹配项目，可切换到「新增项目」，或调整园区/序号/名称筛选条件。")
            return

        st.caption(f"找到 {len(candidates)} 条记录，请选择一条进行修改：")
        display_cols = ["序号", "园区", "项目名称", "项目分级", "拟定金额"]
        display_cols = [c for c in display_cols if c in candidates.columns]
        st.dataframe(candidates[display_cols].head(50), use_container_width=True, hide_index=True)

        seq_choices = sorted(candidates["序号"].dropna().astype(int).unique().tolist())
        def _fmt(seq):
            row = candidates[candidates["序号"].astype(int) == seq]
            name = str(row["项目名称"].iloc[0])[:40] if len(row) and "项目名称" in row.columns else ""
            return f"{seq} - {name}" if name else str(seq)
        chosen_seq = st.selectbox("选择要修改的项目", options=seq_choices, format_func=_fmt)
        target_row = df_all[df_all["序号"].astype(int) == int(chosen_seq)].iloc[0]

        st.markdown("---")
        st.markdown(f"### 步骤 2：编辑项目（序号 {int(target_row['序号'])}）")

        with st.form("edit_project_form"):
            st.caption("提示：保存后将自动推送到飞书。")
            st.markdown("**基础信息**")
            c1, c2, c3 = st.columns(3)
            with c1:
                园区_options = sorted(set(df_all["园区"].dropna().astype(str).tolist()) | set(园区_TO_城市.keys()))
                园区默认 = str(target_row.get("园区", ""))
                园区 = st.selectbox(
                    "园区（选填）",
                    options=[""] + 园区_options,
                    index=(园区_options.index(园区默认) + 1) if 园区默认 in 园区_options else 0,
                )
            with c2:
                所属区域 = st.text_input("所属区域（选填）", value=str(target_row.get("所属区域", "")))
                城市 = st.text_input("所在城市（选填）", value=str(target_row.get("城市", "")))
            with c3:
                所属业态 = st.text_input("所属业态（选填）", value=str(target_row.get("所属业态", "")))

            st.markdown("**项目属性**")
            c4, c5, c6 = st.columns(3)
            with c4:
                项目分级 = st.text_input("项目分级（选填）", value=str(target_row.get("项目分级", "")))
            with c5:
                项目分类 = st.text_input("项目分类（选填）", value=str(target_row.get("项目分类", "")))
            with c6:
                拟定承建组织 = st.text_input("拟定承建组织（选填）", value=str(target_row.get("拟定承建组织", "")))

            c7, c8 = st.columns(2)
            with c7:
                总部重点关注项目 = st.text_input("总部重点关注项目（选填）", value=str(target_row.get("总部重点关注项目", "")))
            with c8:
                拟定金额 = st.number_input("拟定金额（万元，选填）", min_value=0.0, value=float(target_row.get("拟定金额") or 0.0), step=1.0)

            st.markdown("**专业与名称**")
            c9, c10 = st.columns(2)
            with c9:
                专业 = st.text_input("专业（选填）", value=str(target_row.get("专业", "")))
            with c10:
                专业分包 = st.text_input("专业分包（选填）", value=str(target_row.get("专业分包", "")))
            项目名称 = st.text_input("项目名称（选填）", value=str(target_row.get("项目名称", "")))
            备注说明 = st.text_area("备注说明（选填）", value=str(target_row.get("备注说明", "")))

            st.markdown("**项目节点日期（全部选填）**")
            date_values = {}
            for col in TIMELINE_COLS:
                if col not in df_all.columns:
                    continue
                raw_val = target_row.get(col, "")
                date_str = "" if pd.isna(raw_val) else str(raw_val)
                date_values[col] = st.text_input(f"{col}", value=date_str)

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
                if col in df_new.columns:
                    df_new.loc[mask, col] = val
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
    required_fields = ["园区", "所属业态", "项目分级", "项目分类", "拟定承建组织", "专业", "项目名称"]

    with st.form("add_project_form"):
        st.caption(f"新项目序号将自动设置为：{next_seq}")
        st.caption("提示：保存后将自动推送到飞书。")

        c1, c2, c3 = st.columns(3)
        with c1:
            parks = sorted(set(df_all["园区"].dropna().astype(str).tolist()) | set(园区_TO_城市.keys()))
            园区 = st.selectbox("园区（必填）", options=[""] + parks)
        with c2:
            所属区域 = st.text_input("所属区域（选填）")
        with c3:
            城市 = st.text_input("所在城市（选填）")

        c4, c5, c6 = st.columns(3)
        with c4:
            所属业态 = st.text_input("所属业态（必填，例如：独立 / 护理等）")
        with c5:
            项目分级 = st.text_input("项目分级（必填，例如：一级/二级/三级）")
        with c6:
            项目分类 = st.text_input("项目分类（必填，例如：品质提升 / 大修等）")

        c7, c8 = st.columns(2)
        with c7:
            拟定承建组织 = st.text_input("拟定承建组织（必填，例如：项目部 / 社区分包）")
        with c8:
            总部重点关注项目 = st.text_input("总部重点关注项目（选填）")

        c9, c10 = st.columns(2)
        with c9:
            专业 = st.text_input("专业（必填，例如：土建设施 / 供配电等）")
        with c10:
            专业分包 = st.text_input("专业分包（选填，例如：土建-结构）")

        项目名称 = st.text_input("项目名称（必填）")
        备注说明 = st.text_area("备注说明（选填）")
        拟定金额 = st.number_input("拟定金额（万元，选填）", min_value=0.0, value=0.0, step=1.0)

        st.markdown("**项目节点日期（全部选填）**")
        date_values = {}
        for col in TIMELINE_COLS:
            date_values[col] = st.text_input(f"{col}", value="", key=f"add_{col}")

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
        for col, val in date_values.items():
            form_dict[col] = val

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
    st.set_page_config(page_title="养老社区项目向导", page_icon="🏠", layout="wide")
    st.title("养老社区改良改造 - 项目新增/修改向导")
    st.info("📤 支持项目录入、修改、删除，**保存时自动推送到飞书**")

    with st.sidebar:
        st.header("数据源")
        source = st.radio(
            "数据来源",
            ["数据库（团队共享）", "上传文件（覆盖数据库）", "目录下全部 CSV（覆盖数据库）"],
            index=0,
        )
        df_db = load_from_db()
        df = pd.DataFrame()

        if source == "数据库（团队共享）":
            if df_db.empty:
                default_csv = Path(DEFAULT_SINGLE_FILE)
                if default_csv.exists():
                    try:
                        df = load_single_csv(str(default_csv))
                        if not df.empty:
                            save_to_db(df)
                            if get_feishu_webhook_url():
                                if push_to_feishu(f"【养老社区进度表】已用「改良改造报表-V4.csv」初始化，共 {len(df)} 条记录。"):
                                    st.success(f"已用「改良改造报表-V4.csv」初始化团队共享数据库，共 {len(df)} 条记录；已推送至飞书。")
                                else:
                                    st.success(f"已用「改良改造报表-V4.csv」初始化团队共享数据库，共 {len(df)} 条记录。")
                                    st.warning("飞书推送失败，请检查网络或配置。")
                            else:
                                st.success(f"已用「改良改造报表-V4.csv」初始化团队共享数据库，共 {len(df)} 条记录。")
                        else:
                            st.info("当前数据库中暂无数据，请通过下方“上传文件”或“目录下全部 CSV”导入一次。")
                    except Exception as e:
                        st.warning(f"无法从默认 CSV 加载：{e}。请通过下方“上传文件”导入。")
                else:
                    st.info("当前数据库中暂无数据，请通过下方“上传文件”或“目录下全部 CSV”导入一次。")
            else:
                st.success(f"已从数据库加载，共 {len(df_db)} 条记录。")
                df = df_db

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
    _render_project_wizard(df)


if __name__ == "__main__":
    main()
